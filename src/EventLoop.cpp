#include "EventLoop.h"

#if !defined(_WIN32) && !defined(_WIN64)
#include <thread>
#include <stdexcept>
#include <event2/event.h>
#endif

namespace BlackRabbitMQ {

#if !defined(_WIN32) && !defined(_WIN64)

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
    // Сначала флаг, потом loopbreak — чтобы поток вышел из while
    m_running.store(false, std::memory_order_release);
    event_base_loopbreak(m_base);
    if (m_thread && m_thread->joinable()) {
        m_thread->join();
    }
    m_thread.reset(nullptr);
}

void EventLoop::runLoop(EventLoop* self) {
    // Неблокирующий цикл (как в upstream PinkRabbitMQ).
    // EVLOOP_NONBLOCK обрабатывает события и возвращается.
    // Выход по m_running = false из EventLoop::stop().
    while (self->m_running.load(std::memory_order_acquire)) {
        event_base_loop(self->m_base, EVLOOP_NONBLOCK);
    }
}

#else
// Windows: EventLoop не используется (TcpTransportWindows управляет своим потоком)
EventLoop::EventLoop() {}
EventLoop::~EventLoop() {}
void EventLoop::run() {}
void EventLoop::stop() {}
#endif

} // namespace BlackRabbitMQ
