#pragma once

#include <amqpcpp.h>
#include <atomic>
#include <memory>
#include <string>
#include <chrono>

#include "ITransport.h"

namespace BlackRabbitMQ {

// RAII-обёртка над AMQP-соединением с RabbitMQ.
// Делегирует платформенную работу ITransport (LibeventTransport / PocoTransport).
// Ни одного #ifdef — весь платформенный код за интерфейсом.
class Connection {
public:
    explicit Connection(const AMQP::Address& address, int timeoutSec = 30);
    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    void connect();
    void disconnect();
    bool reconnect();

    bool isConnected() const noexcept { return m_transport->isConnected(); }
    std::string lastError() const { return m_transport->error(); }

    // Создать новый канал. Владелец — вызывающий.
    std::unique_ptr<AMQP::Channel> createChannel();

    // Исполнитель задач в потоке транспорта: через него идут вызовы AMQP-CPP.
    ITaskRunner& taskRunner() { return m_transport->taskRunner(); }

private:
    AMQP::Address m_address;
    int m_timeoutSec;
    std::unique_ptr<ITransport> m_transport;
};

} // namespace BlackRabbitMQ
