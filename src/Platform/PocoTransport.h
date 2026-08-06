#pragma once

#include "ITransport.h"
#include "TaskRunner.h"

#include <amqpcpp.h>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <cstdint>

namespace Poco::Net {
    class StreamSocket;
    class DatagramSocket;
    class SocketAddress;
    class PollSet;
}

namespace BlackRabbitMQ {

// Windows транспорт: POCO + AMQP::Connection.
// Реализует AMQP::ConnectionHandler, управляет своим потоком и, как ITaskRunner,
// исполняет в нём все обращения к AMQP-CPP.
class PocoTransport : public ITransport, public AMQP::ConnectionHandler, public ITaskRunner {
public:
    static constexpr size_t BUFFER_SIZE = 8 * 1024 * 1024;
    // Потолок роста буферов. Разбор кадра требует, чтобы кадр целиком лежал
    // в буфере: если брокер согласовал большой frame max, 8 МБ может не хватить,
    // и буфер растёт до этого предела, а не теряет хвост молча.
    static constexpr size_t MAX_BUFFER_SIZE = 64 * 1024 * 1024;
    static constexpr size_t TEMP_BUFFER_SIZE = 1 * 1024 * 1024;
    // Цикл спит на poll и будится событием, поэтому таймаут длинный:
    // он нужен только как страховка и точка проверки флага остановки.
    static constexpr int POLL_TIMEOUT_MS = 50;

    PocoTransport();
    ~PocoTransport() override;

    // ITransport
    void setTlsOptions(const TlsOptions& options) override { m_tls = options; }
    void setHeartbeat(int seconds) override { m_desiredHeartbeat = seconds; }
    void connect(const AMQP::Address& address, int timeoutSec) override;
    void disconnect() override;
    std::unique_ptr<AMQP::Channel> createChannel() override;
    bool isConnected() const noexcept override { return !m_closed.load(std::memory_order_acquire); }
    std::string error() const override {
        std::lock_guard<std::mutex> lock(m_errorMutex);
        return m_error;
    }

    ITaskRunner& taskRunner() override { return *this; }

    // ITaskRunner
    // Определение в .cpp: принадлежность потоку определяется thread_local
    // меткой, которую ставит сам цикл. Сравнение с сохранённым thread::id
    // было гонкой — поле пишет поток цикла, а читает поток 1С.
    bool inLoopThread() const noexcept override;
    void post(std::function<void()> task) override {
        {
            std::lock_guard<std::mutex> lock(m_taskMutex);
            m_tasks.push_back(std::move(task));
        }
        wake(); // иначе задача пролежит до конца текущего poll
    }

    // AMQP::ConnectionHandler
    void onData(AMQP::Connection* connection, const char* data, size_t size) override;
    void onReady(AMQP::Connection* connection) override;
    void onError(AMQP::Connection* connection, const char* message) override;
    void onClosed(AMQP::Connection* connection) override;
    uint16_t onNegotiate(AMQP::Connection* connection, uint16_t interval) override;

private:
    void startLoop(const std::string& host, uint16_t port, bool ssl);
    void stopLoop();
    // Клиентский TLS-контекст по TlsOptions: политика проверки, доверенные
    // корни, набор шифров. Возвращает готовый к connect() сокет.
    Poco::Net::StreamSocket* createSecureSocket();
    // Сверяет сертификат брокера с именем хоста после рукопожатия.
    void verifyPeerCertificate();
    // Ждёт завершения AMQP-handshake (onReady) или бросает по таймауту:
    // connect() не имеет права возвращать управление на полуоткрытом соединении.
    void waitForReady(int timeoutSec);
    static void loopThread(PocoTransport* self);
    void loopIteration();
    void sendDataFromBuffer();
    // Отдаёт накопленное парсеру AMQP-CPP и сдвигает разобранное.
    void parseInBuffer();
    // Шлёт heartbeat, если пришёл срок. Вызывается из потока цикла.
    void sendHeartbeatIfDue();
    // Брокер закрыл сокет: пометить соединение мёртвым, чтобы isConnected()
    // сказал правду сразу, а не после таймаута следующей операции.
    void markClosedByPeer(const std::string& reason);
    void drainTasks();
    // Будит цикл датаграммой на loopback: аналог self-pipe из EventLoop.
    // Без него операция ждала бы истечения poll, а сам poll приходилось бы
    // держать коротким — то есть будить поток тысячу раз в секунду впустую.
    void wake();
    void setError(const std::string& message) {
        std::lock_guard<std::mutex> lock(m_errorMutex);
        m_error = message;
    }

    struct Buffer {
        std::vector<char> data;
        size_t used = 0;
        explicit Buffer(size_t sz) : data(sz, 0) {}
        size_t write(const char* src, size_t sz);
        void drain() { used = 0; }
        size_t available() const { return used; }
        size_t capacity() const { return data.size(); }
        const char* ptr() const { return data.data(); }
        void shift(size_t count);
        // Расширяет буфер так, чтобы влезло ещё need байт, но не выше limit.
        // false — потолок достигнут и место не освободилось.
        bool ensureSpace(size_t need, size_t limit);
    };

    std::unique_ptr<Poco::Net::StreamSocket> m_socket;
    std::unique_ptr<Poco::Net::DatagramSocket> m_wakeIn;   // читает поток цикла
    std::unique_ptr<Poco::Net::DatagramSocket> m_wakeOut;  // пишет post()
    std::unique_ptr<Poco::Net::SocketAddress> m_wakeAddr;
    std::unique_ptr<Poco::Net::PollSet> m_pollSet;
    std::mutex m_wakeMutex;
    std::unique_ptr<AMQP::Connection> m_amqpConn;
    std::unique_ptr<Buffer> m_inBuf;
    std::unique_ptr<Buffer> m_outBuf;
    std::vector<char> m_tmpBuf;
    std::atomic<bool> m_closed{true};
    std::atomic<bool> m_stop{false};
    mutable std::mutex m_errorMutex;
    std::string m_error;
    std::unique_ptr<std::thread> m_thread;
    std::mutex m_taskMutex;
    std::vector<std::function<void()>> m_tasks;
    std::string m_host;
    uint16_t m_port;
    bool m_ssl;
    TlsOptions m_tls;
    int m_desiredHeartbeat{-1};     // что просим: -1 как брокер, 0 выключить
    uint16_t m_heartbeatSec{0};     // что согласовали, пишет поток цикла
    std::chrono::steady_clock::time_point m_lastHeartbeatSent{};
};

} // namespace BlackRabbitMQ
