#include "Connection.h"
#include "EventLoop.h"
#include "TcpTransport.h"
#include "Platform/TcpTransportLinux.h"

#include <amqpcpp.h>
#include <amqpcpp/libevent.h>

#include <chrono>
#include <thread>

namespace BlackRabbitMQ {

Connection::Connection(const AMQP::Address& address, int timeoutSec)
    : m_address(address)
    , m_timeoutSec(timeoutSec)
    , m_connected(false)
{
}

Connection::~Connection() {
    disconnect();
}

void Connection::connect() {
    if (m_connected.load(std::memory_order_acquire)) {
        return;
    }

    m_error.clear();

    // 1. Создать EventLoop — владеет event_base
    m_eventLoop.reset(new EventLoop());

    // 2. Создать транспорт (AMQP::LibEventHandler) на event_base EventLoop'а
    m_transport.reset(new TcpTransport(m_eventLoop->base()));

    // 3. Создать AMQP соединение поверх транспорта
    m_amqpConn.reset(new AMQP::TcpConnection(m_transport.get(), m_address));

    // 4. Запустить event loop в отдельном потоке
    m_eventLoop->run();

    // 5. Ждать готовности соединения
    waitForReady();
}

void Connection::disconnect() {
    // Порядок важен: сначала соединение, потом транспорт, потом event loop.
    m_amqpConn.reset(nullptr);

    if (m_transport) {
        m_transport.reset(nullptr);
    }

    if (m_eventLoop) {
        m_eventLoop->stop();
        m_eventLoop.reset(nullptr);
    }

    m_connected.store(false, std::memory_order_release);
}

bool Connection::reconnect() {
    disconnect();
    try {
        connect();
        return true;
    } catch (const std::exception& e) {
        m_error = e.what();
        return false;
    }
}

void Connection::waitForReady() {
    auto end = std::chrono::system_clock::now() + std::chrono::seconds(m_timeoutSec);

    while (!m_amqpConn->ready() && !m_amqpConn->closed()) {
        if (std::chrono::system_clock::now() > end) {
            std::string err = m_transport->error();
            disconnect();
            if (!err.empty()) {
                throw std::runtime_error(err);
            }
            throw std::runtime_error("Connection timeout");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    if (!m_amqpConn->ready()) {
        std::string err = m_transport->error();
        disconnect();
        throw std::runtime_error(err.empty() ? "Connection failed" : err);
    }

    m_connected.store(true, std::memory_order_release);
}

std::unique_ptr<AMQP::TcpChannel> Connection::createChannel() {
    if (!m_connected.load(std::memory_order_acquire) || !m_amqpConn) {
        throw std::runtime_error("Connection not established");
    }
    if (!m_amqpConn->usable()) {
        throw std::runtime_error("Connection is not usable");
    }

    auto channel = std::make_unique<AMQP::TcpChannel>(m_amqpConn.get());

    // Синхронное ожидание готовности канала (как в upstream openChannel)
    std::mutex m;
    std::condition_variable cv;
    std::atomic<bool> ready{false};

    channel->onReady([&]() {
        ready.store(true, std::memory_order_release);
        cv.notify_all();
    });

    channel->onError([&](const char* message) {
        m_error = message ? message : "Channel error";
        ready.store(true, std::memory_order_release);
        cv.notify_all();
    });

    std::unique_lock<std::mutex> lock(m);
    cv.wait(lock, [&]() { return ready.load(std::memory_order_acquire); });

    if (!channel || !channel->usable()) {
        throw std::runtime_error(m_error.empty() ? "Channel not opened" : m_error);
    }

    // Установить постоянный обработчик ошибок
    channel->onError([this](const char* message) {
        if (message) m_error = message;
    });

    return channel;
}

} // namespace BlackRabbitMQ
