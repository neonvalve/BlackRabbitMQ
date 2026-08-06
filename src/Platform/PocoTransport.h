#pragma once

#include "ITransport.h"
#include "TaskRunner.h"

#include <amqpcpp.h>
#include <atomic>
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
    static constexpr size_t TEMP_BUFFER_SIZE = 1 * 1024 * 1024;
    // Цикл спит на poll и будится событием, поэтому таймаут длинный:
    // он нужен только как страховка и точка проверки флага остановки.
    static constexpr int POLL_TIMEOUT_MS = 50;

    PocoTransport();
    ~PocoTransport() override;

    // ITransport
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
    bool inLoopThread() const noexcept override {
        return m_thread && std::this_thread::get_id() == m_loopThreadId;
    }
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
    // Ждёт завершения AMQP-handshake (onReady) или бросает по таймауту:
    // connect() не имеет права возвращать управление на полуоткрытом соединении.
    void waitForReady(int timeoutSec);
    static void loopThread(PocoTransport* self);
    void loopIteration();
    void sendDataFromBuffer();
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
        const char* ptr() const { return data.data(); }
        void shift(size_t count);
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
    std::thread::id m_loopThreadId;
    std::mutex m_taskMutex;
    std::vector<std::function<void()>> m_tasks;
    std::string m_host;
    uint16_t m_port;
    bool m_ssl;
};

} // namespace BlackRabbitMQ
