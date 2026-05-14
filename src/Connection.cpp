#include "Connection.h"
#include "EventLoop.h"
#include "TcpTransport.h"

#include <amqpcpp/libevent.h>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <stdexcept>

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

    // 2. Создать транспорт
#if defined(__linux__) || defined(__APPLE__)
    m_transport.reset(new TcpTransport(m_eventLoop->base()));

    // 3. Создать AMQP соединение поверх транспорта
    m_amqpConn.reset(new AMQP::TcpConnection(m_transport.get(), m_address));

    // 4. Запустить event loop
    m_eventLoop->run();
#elif defined(_WIN32) || defined(_WIN64)
    m_transport.reset(new TcpTransport(
        m_address.hostname(),
        m_address.port(),
        m_address.secure()
    ));

    // 3. Создать AMQP соединение
    m_amqpConn.reset(new AMQP::Connection(
        m_transport.get(),
        m_address.login(),
        m_address.vhost()
    ));
    m_transport->setAmqpConnection(m_amqpConn.get());

    // 4. Запустить поток чтения
    m_transport->startLoop();
#else
#error "Unsupported platform"
#endif

    // 5. Ждать готовности
    waitForReady();
}

void Connection::disconnect() {
    m_amqpConn.reset(nullptr);

    if (m_transport) {
#if defined(_WIN32) || defined(_WIN64)
        m_transport->stopLoop();
#endif
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

#if defined(__linux__) || defined(__APPLE__)
    while (!m_amqpConn->ready() && !m_amqpConn->closed()) {
        if (std::chrono::system_clock::now() > end) {
            std::string err = m_transport->error();
            disconnect();
            throw std::runtime_error(err.empty() ? "Connection timeout" : err);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    if (!m_amqpConn->ready()) {
        std::string err = m_transport->error();
        disconnect();
        throw std::runtime_error(err.empty() ? "Connection failed" : err);
    }
#elif defined(_WIN32) || defined(_WIN64)
    // Windows: ждём флаг isClosed у транспорта
    while (m_transport->isClosed()) {
        if (std::chrono::system_clock::now() > end) {
            std::string err = m_transport->error();
            disconnect();
            throw std::runtime_error(err.empty() ? "Connection timeout" : err);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
#endif

    m_connected.store(true, std::memory_order_release);
}

std::unique_ptr<AMQP::Channel> Connection::createChannel() {
    if (!m_connected.load(std::memory_order_acquire) || !m_amqpConn) {
        throw std::runtime_error("Connection not established");
    }

#if defined(__linux__) || defined(__APPLE__)
    if (!m_amqpConn->usable()) {
        throw std::runtime_error("Connection is not usable");
    }

    auto channel = std::make_unique<AMQP::TcpChannel>(m_amqpConn.get());

    std::mutex m;
    std::condition_variable cv;
    std::atomic<bool> ready{false};

    channel->onReady([&]() {
        ready.store(true, std::memory_order_release);
        cv.notify_all();
    });
    channel->onError([&](const char* msg) {
        m_error = msg ? msg : "Channel error";
        ready.store(true, std::memory_order_release);
        cv.notify_all();
    });

    std::unique_lock<std::mutex> lock(m);
    cv.wait(lock, [&]() { return ready.load(std::memory_order_acquire); });

    if (!channel || !channel->usable()) {
        throw std::runtime_error(m_error.empty() ? "Channel not opened" : m_error);
    }

    channel->onError([this](const char* msg) {
        if (msg) m_error = msg;
    });

    return channel;
#elif defined(_WIN32) || defined(_WIN64)
    // Windows: AMQP::Channel создаётся из AMQP::Connection
    auto channel = std::make_unique<AMQP::Channel>(m_amqpConn.get());

    std::mutex m;
    std::condition_variable cv;
    std::atomic<bool> ready{false};

    channel->onReady([&]() {
        ready.store(true, std::memory_order_release);
        cv.notify_all();
    });
    channel->onError([&](const char* msg) {
        m_error = msg ? msg : "Channel error";
        ready.store(true, std::memory_order_release);
        cv.notify_all();
    });

    std::unique_lock<std::mutex> lock(m);
    cv.wait(lock, [&]() { return ready.load(std::memory_order_acquire); });

    if (!channel || !channel->usable()) {
        throw std::runtime_error(m_error.empty() ? "Channel not opened" : m_error);
    }

    channel->onError([this](const char* msg) {
        if (msg) m_error = msg;
    });

    return channel;
#endif
}

} // namespace BlackRabbitMQ
