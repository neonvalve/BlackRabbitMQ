#pragma once

#include "ITransport.h"
#include "EventLoop.h"

#include <amqpcpp.h>
#include <amqpcpp/libevent.h>
#include <atomic>
#include <memory>
#include <string>

namespace BlackRabbitMQ {

// Внутренний обработчик TCP событий на libevent.
// Расширяет AMQP::LibEventHandler, хранит флаг соединения и ошибку.
struct LibeventHandler : AMQP::LibEventHandler {
    explicit LibeventHandler(event_base* evbase) : AMQP::LibEventHandler(evbase) {}
    void onConnected(AMQP::TcpConnection*) override { lost.store(false); }
    void onLost(AMQP::TcpConnection*) override      { lost.store(true); }
    void onError(AMQP::TcpConnection*, const char* msg) override { if (msg) error = msg; }
    std::atomic<bool> lost{true};
    std::string error;
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
    bool isConnected() const noexcept override { return m_connected.load(std::memory_order_acquire); }
    const std::string& error() const noexcept override { return m_error; }

private:
    void waitForReady(int timeoutSec);

    std::unique_ptr<EventLoop> m_eventLoop;
    std::unique_ptr<LibeventHandler> m_handler;
    std::unique_ptr<AMQP::TcpConnection> m_amqpConn;
    std::atomic<bool> m_connected{false};
    std::string m_error;
};

} // namespace BlackRabbitMQ
