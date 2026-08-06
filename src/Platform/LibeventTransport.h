#pragma once

#include "ITransport.h"
#include "EventLoop.h"

#include <amqpcpp.h>
#include <amqpcpp/libevent.h>
#include <atomic>
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
    explicit LibeventHandler(event_base* evbase) : AMQP::LibEventHandler(evbase) {}

    void onReady(AMQP::TcpConnection*) override {
        std::lock_guard<std::mutex> lock(mutex);
        ready = true;
        cv.notify_all();
    }

    void onLost(AMQP::TcpConnection*) override {
        std::lock_guard<std::mutex> lock(mutex);
        lost.store(true, std::memory_order_release);
        finished = true;
        if (error.empty()) error = "connection lost";
        cv.notify_all();
    }

    void onError(AMQP::TcpConnection*, const char* msg) override {
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
};

// Linux/macOS транспорт: libevent + AMQP::TcpConnection.
// Владеет EventLoop (поток), LibeventHandler и TcpConnection.
class LibeventTransport : public ITransport {
public:
    LibeventTransport();
    ~LibeventTransport() override;

    // ITransport
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
};

} // namespace BlackRabbitMQ
