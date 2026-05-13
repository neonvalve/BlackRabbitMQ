#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

struct event_base;
struct event;

namespace BlackRabbitMQ {

// RAII обёртка над event loop (libevent).
// Блокирующий режим: event_base_loop(base, 0) — поток спит на poll(),
// никакого busy-wait.
class EventLoop {
public:
    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    // Запустить event loop в отдельном потоке.
    void run();

    // Остановить event loop и дождаться завершения потока.
    void stop();

    // Потокобезопасная проверка, запущен ли loop.
    bool isRunning() const noexcept { return m_running.load(std::memory_order_acquire); }

    // Сырой указатель на event_base для регистрации событий.
    event_base* base() const noexcept { return m_base; }

private:
    static void runLoop(EventLoop* self);

    event_base* m_base;
    std::unique_ptr<std::thread> m_thread;
    std::atomic<bool> m_running;
};

} // namespace BlackRabbitMQ
