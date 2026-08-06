#pragma once

#include "ITransport.h"
#include "EventLoop.h"
#include "Logger.h"

#include <amqpcpp.h>
#include <amqpcpp/libevent.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>

namespace BlackRabbitMQ {

// Внутренний обработчик TCP событий на libevent.
// Все методы вызываются из потока event loop — состояние под мьютексом,
// ожидающая сторона (поток 1С) просыпается по condition_variable.
// Опрашивать состояние AMQP::TcpConnection из чужого потока нельзя: это гонка.
struct LibeventHandler : AMQP::LibEventHandler {
    LibeventHandler(event_base* evbase, TlsOptions tlsOptions, std::string peerHost)
        : AMQP::LibEventHandler(evbase)
        , tls(std::move(tlsOptions))
        , host(std::move(peerHost)) {}

    // TLS: до рукопожатия задаём политику проверки и доверенные корни,
    // после — сверяем результат. Без этого AMQP-CPP шифрует трафик, но
    // сертификат брокера не проверяет вовсе.
    bool onSecuring(AMQP::TcpConnection* connection, SSL* ssl) override;
    bool onSecured(AMQP::TcpConnection* connection, const SSL* ssl) override;

    // Согласование интервала heartbeat. Кадры отправляет транспорт по тику
    // цикла: библиотека сама этого не делает, а брокер разрывает соединение,
    // от которого не было ни байта два интервала.
    uint16_t onNegotiate(AMQP::TcpConnection* connection, uint16_t interval) override;

    // Зафиксировать причину отказа и разбудить ожидающего: соединение,
    // отклонённое на рукопожатии, иначе выглядело бы как таймаут.
    void fail(const std::string& message) {
        std::lock_guard<std::mutex> lock(mutex);
        if (error.empty()) error = message;
        finished = true;
        cv.notify_all();
    }

    void onReady(AMQP::TcpConnection*) override {
        std::lock_guard<std::mutex> lock(mutex);
        ready = true;
        cv.notify_all();
    }

    void onLost(AMQP::TcpConnection*) override {
        BRMQ_LOG_WARN("Connection lost");
        std::lock_guard<std::mutex> lock(mutex);
        lost.store(true, std::memory_order_release);
        finished = true;
        if (error.empty()) error = "connection lost";
        cv.notify_all();
    }

    void onError(AMQP::TcpConnection*, const char* msg) override {
        BRMQ_LOG_ERROR(std::string("AMQP error: ") + (msg ? msg : "unknown"));
        std::lock_guard<std::mutex> lock(mutex);
        if (msg) error = msg;
        finished = true;
        cv.notify_all();
    }

    void onConnected(AMQP::TcpConnection*) override {
        lost.store(false, std::memory_order_release);
    }

    std::mutex mutex;
    std::condition_variable cv;
    bool ready = false;      // handshake завершён, соединением можно пользоваться
    bool finished = false;   // ошибка или потеря связи — ждать больше нечего
    std::string error;
    std::atomic<bool> lost{true};
    TlsOptions tls;
    std::string host;
    int desiredHeartbeat = -1;              // -1 как брокер, 0 выключить, >0 своё
    std::atomic<uint16_t> heartbeatSec{0};  // согласованное значение
};

// Linux/macOS транспорт: libevent + AMQP::TcpConnection.
// Владеет EventLoop (поток), LibeventHandler и TcpConnection.
class LibeventTransport : public ITransport {
public:
    LibeventTransport();
    ~LibeventTransport() override;

    // ITransport
    void setTlsOptions(const TlsOptions& options) override { m_tls = options; }
    void setHeartbeat(int seconds) override { m_heartbeat = seconds; }
    void connect(const AMQP::Address& address, int timeoutSec) override;
    void disconnect() override;
    std::unique_ptr<AMQP::Channel> createChannel() override;
    // Соединение считается живым, пока не пришёл onLost: без этой проверки
    // после обрыва 1С видела бы «подключено» и получала непонятные ошибки.
    bool isConnected() const noexcept override {
        return m_connected.load(std::memory_order_acquire)
            && m_handler && !m_handler->lost.load(std::memory_order_acquire);
    }
    std::string error() const override {
        std::lock_guard<std::mutex> lock(m_errorMutex);
        return m_error;
    }
    ITaskRunner& taskRunner() override { return *m_eventLoop; }

private:
    void waitForReady(int timeoutSec);
    // Вызывается тиком цикла: шлёт heartbeat, когда пришёл срок.
    void onLoopTick();
    void setError(const std::string& message) {
        std::lock_guard<std::mutex> lock(m_errorMutex);
        m_error = message;
    }

    std::unique_ptr<EventLoop> m_eventLoop;
    std::unique_ptr<LibeventHandler> m_handler;
    std::unique_ptr<AMQP::TcpConnection> m_amqpConn;
    std::atomic<bool> m_connected{false};
    mutable std::mutex m_errorMutex;
    std::string m_error;
    int m_timeoutSec{30};
    TlsOptions m_tls;
    int m_heartbeat{-1};
    std::chrono::steady_clock::time_point m_lastHeartbeatSent{};
};

} // namespace BlackRabbitMQ
