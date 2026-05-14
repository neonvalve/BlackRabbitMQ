#pragma once

#include <amqpcpp.h>
#include <amqpcpp/libevent.h>
#include <atomic>
#include <string>

namespace BlackRabbitMQ {

// Linux-реализация TCP транспорта через libevent.
// Расширяет AMQP::LibEventHandler — готовую интеграцию AMQP-CPP + libevent.
// Исправлено против upstream: std::atomic вместо volatile bool.
class TcpTransportLinux : public AMQP::LibEventHandler {
public:
    explicit TcpTransportLinux(event_base* evbase);

    TcpTransportLinux(const TcpTransportLinux&) = delete;
    TcpTransportLinux& operator=(const TcpTransportLinux&) = delete;

    // Состояние соединения.
    bool isLost() const noexcept { return m_lost.load(std::memory_order_acquire); }

    // Последняя ошибка.
    const std::string& error() const noexcept { return m_error; }

    // AMQP::TcpHandler overrides
    void onConnected(AMQP::TcpConnection* connection) override;
    void onLost(AMQP::TcpConnection* connection) override;
    void onError(AMQP::TcpConnection* connection, const char* message) override;

private:
    std::atomic<bool> m_lost;
    std::string m_error;
};

} // namespace BlackRabbitMQ
