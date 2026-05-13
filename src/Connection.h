#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <chrono>

namespace AMQP {
    class Address;
    class TcpConnection;
    class TcpChannel;
}

namespace BlackRabbitMQ {

class EventLoop;
class TcpTransport;

// RAII-обёртка над AMQP-соединением с RabbitMQ.
// Владеет: TcpTransport, EventLoop (поток), AMQP::TcpConnection.
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
    std::unique_ptr<AMQP::TcpChannel> createChannel();

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
    std::unique_ptr<AMQP::TcpConnection> m_amqpConn;

    std::atomic<bool> m_connected;
    std::string m_error;
};

} // namespace BlackRabbitMQ
