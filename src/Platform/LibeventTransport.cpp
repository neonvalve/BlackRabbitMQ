#include "Platform/LibeventTransport.h"

#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <stdexcept>

namespace BlackRabbitMQ {

LibeventTransport::LibeventTransport() = default;

LibeventTransport::~LibeventTransport() {
    disconnect();
}

void LibeventTransport::connect(const AMQP::Address& address, int timeoutSec) {
    if (m_connected.load(std::memory_order_acquire)) return;

    m_error.clear();

    // 1. EventLoop — владеет event_base
    m_eventLoop.reset(new EventLoop());

    // 2. Handler на event_base из EventLoop
    m_handler.reset(new LibeventHandler(m_eventLoop->base()));

    // 3. AMQP соединение
    m_amqpConn.reset(new AMQP::TcpConnection(m_handler.get(), address));

    // 4. Запустить event loop
    m_eventLoop->run();

    // 5. Ждать готовности
    waitForReady(timeoutSec);
    m_connected.store(true, std::memory_order_release);
}

void LibeventTransport::disconnect() {
    m_amqpConn.reset(nullptr);
    m_handler.reset(nullptr);
    if (m_eventLoop) {
        m_eventLoop->stop();
        m_eventLoop.reset(nullptr);
    }
    m_connected.store(false, std::memory_order_release);
}

std::unique_ptr<AMQP::Channel> LibeventTransport::createChannel() {
    if (!m_amqpConn || !m_amqpConn->usable()) {
        throw std::runtime_error("Connection not usable");
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
}

void LibeventTransport::waitForReady(int timeoutSec) {
    auto end = std::chrono::system_clock::now() + std::chrono::seconds(timeoutSec);
    while (!m_amqpConn->ready() && !m_amqpConn->closed()) {
        if (std::chrono::system_clock::now() > end) {
            std::string err = m_error;
            if (err.empty() && m_handler) err = m_handler->error;
            disconnect();
            throw std::runtime_error(err.empty() ? "Connection timeout" : err);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (!m_amqpConn->ready()) {
        std::string err = m_error;
        if (err.empty() && m_handler) err = m_handler->error;
        disconnect();
        throw std::runtime_error(err.empty() ? "Connection failed" : err);
    }
}

} // namespace BlackRabbitMQ
