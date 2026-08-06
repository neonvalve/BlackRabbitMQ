#pragma once

#include "TaskRunner.h"

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

struct event_base;
struct event;

namespace BlackRabbitMQ {

// RAII обёртка над event loop (libevent).
// Блокирующий режим: event_base_loop(base, 0) — поток спит на poll(),
// никакого busy-wait.
//
// В базе всегда зарегистрирован persistent-таймер (tick):
//   1. цикл не завершается, когда других событий ещё/уже нет
//      (иначе event_base_loop вернётся сразу и соединение не установится);
//   2. остановка происходит изнутри потока цикла — event_base_loopbreak
//      вызывается из callback'а таймера, а не из чужого потока;
//   3. регистрация событий, сделанная потоком 1С (AMQP-CPP пишет в базу
//      из вызывающего потока), подхватывается не позже одного тика.
// Реализует ITaskRunner: все обращения к AMQP-CPP исполняются здесь.
class EventLoop : public ITaskRunner {
public:
    // Период тика: компромисс между задержкой подхвата чужих регистраций
    // и «спит на poll» (20 пробуждений в секунду — 0% CPU).
    static constexpr int kTickMs = 50;

    EventLoop();
    ~EventLoop() override;

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    // Запустить event loop в отдельном потоке.
    void run();

    // Остановить event loop и дождаться завершения потока.
    void stop();

    // --- ITaskRunner ---
    // Определение в .cpp: принадлежность потоку определяется thread_local
    // меткой, которую ставит сам цикл. Сравнение с сохранённым thread::id
    // было гонкой — поле пишет поток цикла, а читает поток 1С.
    bool inLoopThread() const noexcept override;
    void post(std::function<void()> task) override;

    // Задача, исполняемая каждый тик в потоке цикла (heartbeat и подобное).
    // Ставится до run(): после запуска колбэк читает поток цикла.
    void setTickCallback(std::function<void()> callback) {
        m_onTick = std::move(callback);
    }

    // Потокобезопасная проверка, запущен ли loop.
    bool isRunning() const noexcept { return m_running.load(std::memory_order_acquire); }

    // Сырой указатель на event_base для регистрации событий.
    event_base* base() const noexcept { return m_base; }

private:
    static void runLoop(EventLoop* self);
    static void onTick(int fd, short what, void* arg);
    static void onWake(int fd, short what, void* arg);
    void drainTasks();

    event_base* m_base;
    event* m_tick;
    event* m_wake;          // чтение из self-pipe: пробуждение из чужого потока
    int m_wakeFd[2];        // [0] — чтение в цикле, [1] — запись из post()
    std::unique_ptr<std::thread> m_thread;
    std::atomic<bool> m_running;

    std::function<void()> m_onTick;

    std::mutex m_taskMutex;
    std::vector<std::function<void()>> m_tasks;
};

} // namespace BlackRabbitMQ
