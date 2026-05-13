#pragma once

#include <amqpcpp.h>
#include <atomic>
#include <memory>
#include <string>
#include <cstdint>

namespace BlackRabbitMQ {

// Реализация TcpTransport для Windows через POCO.
// Имплементирует AMQP::ConnectionHandler для интеграции с AMQP-CPP.
class TcpTransportWindows : public AMQP::ConnectionHandler {
public:
    TcpTransportWindows(const std::string& host, uint16_t port, bool ssl);
    ~TcpTransportWindows() override;

    TcpTransportWindows(const TcpTransportWindows&) = delete;
    TcpTransportWindows& operator=(const TcpTransportWindows&) = delete;

    bool connect();
    void close();

    bool isReady() const noexcept { return m_ready.load(std::memory_order_acquire); }
    bool isClosed() const noexcept { return m_closed.load(std::memory_order_acquire); }
    const std::string& error() const noexcept { return m_error; }

    // AMQP::ConnectionHandler interface
    void onData(AMQP::Connection* connection, const char* data, size_t size) override;
    void onReady(AMQP::Connection* connection) override;
    void onError(AMQP::Connection* connection, const char* message) override;
    void onClosed(AMQP::Connection* connection) override;
    uint16_t onNegotiate(AMQP::Connection* connection, uint16_t interval) override;

private:
    struct Impl;
    std::unique_ptr<Impl> m_impl;

    std::atomic<bool> m_ready;
    std::atomic<bool> m_closed;
    std::string m_error;
};

} // namespace BlackRabbitMQ
