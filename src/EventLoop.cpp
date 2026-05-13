#include "EventLoop.h"

#include <thread>
#include <event2/event.h>

namespace BlackRabbitMQ {

EventLoop::EventLoop()
    : m_base(nullptr)
    , m_running(false)
{
    m_base = event_base_new();
    if (!m_base) {
        throw std::runtime_error("EventLoop: failed to create event_base");
    }
}

EventLoop::~EventLoop() {
    stop();
    if (m_base) {
        event_base_free(m_base);
        m_base = nullptr;
    }
}

void EventLoop::run() {
    if (m_running.load(std::memory_order_acquire)) {
        return;
    }
    m_running.store(true, std::memory_order_release);
    m_thread = std::make_unique<std::thread>(runLoop, this);
}

void EventLoop::stop() {
    if (!m_running.load(std::memory_order_acquire)) {
        return;
    }
    event_base_loopbreak(m_base);
    if (m_thread && m_thread->joinable()) {
        m_thread->join();
    }
    m_thread.reset(nullptr);
    m_running.store(false, std::memory_order_release);
}

void EventLoop::runLoop(EventLoop* self) {
    // Блокирующий режим: поток спит на poll(), 0% CPU в простое.
    // event_base_loopbreak() прерывает цикл из другого потока.
    event_base_loop(self->m_base, 0);
}

} // namespace BlackRabbitMQ
