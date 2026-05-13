#pragma once

#include <amqpcpp.h>
#include <atomic>
#include <memory>
#include <string>
#include <chrono>

#if defined(__linux__) || defined(__APPLE__)
#include <amqpcpp/libevent.h>
#endif

namespace BlackRabbitMQ {

#include "TcpTransport.h"

class EventLoop;

// RAII-обёртка над AMQP-соединением с RabbitMQ.
// Владеет: TcpTransport, EventLoop (поток), AMQP::TcpConnection/Connection.
// Поддерживает heartbeat через onNegotiate.
class Connection {
public:
    explicit Connection(const AMQP::Address& address, int timeoutSec = 30);
    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    // Установить соединение. Бросает std::runtime_error при ошибке.
    void connect();

    // Разорвать соединение. Безопасен для повторного вызова.
    void disconnect();

    // Переподключиться с теми же параметрами.
    bool reconnect();

    // Состояние соединения.
    bool isConnected() const noexcept { return m_connected.load(std::memory_order_acquire); }

    // Создать новый канал. Владелец — вызывающий.
    // Linux: TcpChannel (наследник AMQP::Channel)
    // Windows: AMQP::Channel
    std::unique_ptr<AMQP::Channel> createChannel();

    // Доступ к event loop (для продвинутых сценариев).
    EventLoop* eventLoop() const noexcept { return m_eventLoop.get(); }

    // Последняя ошибка.
    const std::string& lastError() const noexcept { return m_error; }

private:
    void waitForReady();

    AMQP::Address m_address;
    int m_timeoutSec;

    std::unique_ptr<TcpTransport> m_transport;
    std::unique_ptr<EventLoop> m_eventLoop;

#if defined(__linux__) || defined(__APPLE__)
    std::unique_ptr<AMQP::TcpConnection> m_amqpConn;
#elif defined(_WIN32) || defined(_WIN64)
    std::unique_ptr<AMQP::Connection> m_amqpConn;
#endif

    std::atomic<bool> m_connected;
    std::string m_error;
};

} // namespace BlackRabbitMQ
