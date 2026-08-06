#include "EventLoop.h"

#if !defined(_WIN32) && !defined(_WIN64)
#include <chrono>
#include <thread>
#include <stdexcept>
#include <sys/time.h>
#include <unistd.h>
#include <event2/event.h>
#include <event2/util.h>
#endif

namespace BlackRabbitMQ {

namespace {
// Метку ставит сам поток цикла: сравнивать сохранённый std::thread::id нельзя —
// поле пишет поток цикла, а читает поток 1С, и это гонка.
thread_local const EventLoop* t_loopOwner = nullptr;
} // namespace

bool EventLoop::inLoopThread() const noexcept {
    return t_loopOwner == this;
}

#if !defined(_WIN32) && !defined(_WIN64)

EventLoop::EventLoop()
    : m_base(nullptr)
    , m_tick(nullptr)
    , m_wake(nullptr)
    , m_wakeFd{-1, -1}
    , m_running(false)
{
    m_base = event_base_new();
    if (!m_base) {
        throw std::runtime_error("EventLoop: failed to create event_base");
    }

    // self-pipe: запись в него будит poll() из любого потока.
    // event_active из чужого потока небезопасен без evthread_use_pthreads,
    // а write(2) потокобезопасен по стандарту.
    if (pipe(m_wakeFd) != 0) {
        event_base_free(m_base);
        m_base = nullptr;
        throw std::runtime_error("EventLoop: failed to create wakeup pipe");
    }
    evutil_make_socket_nonblocking(m_wakeFd[0]);
    evutil_make_socket_nonblocking(m_wakeFd[1]);
}

EventLoop::~EventLoop() {
    stop();
    if (m_wake) {
        event_free(m_wake);
        m_wake = nullptr;
    }
    if (m_tick) {
        event_free(m_tick);
        m_tick = nullptr;
    }
    if (m_base) {
        event_base_free(m_base);
        m_base = nullptr;
    }
    for (int& fd : m_wakeFd) {
        if (fd >= 0) { close(fd); fd = -1; }
    }
}

void EventLoop::post(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> lock(m_taskMutex);
        m_tasks.push_back(std::move(task));
    }
    // Один байт достаточно: обработчик разбирает всю очередь целиком.
    const char byte = 'x';
    ssize_t written = ::write(m_wakeFd[1], &byte, 1);
    (void)written; // переполнение трубы означает, что цикл и так проснётся
}

void EventLoop::drainTasks() {
    std::vector<std::function<void()>> tasks;
    {
        std::lock_guard<std::mutex> lock(m_taskMutex);
        tasks.swap(m_tasks);
    }
    // Задачи исполняем вне мьютекса: они могут ставить новые задачи.
    for (auto& task : tasks) {
        task();
    }
}

void EventLoop::onWake(int fd, short /*what*/, void* arg) {
    char buffer[256];
    while (::read(fd, buffer, sizeof(buffer)) > 0) {
        // вычерпываем трубу, чтобы событие не срабатывало вхолостую
    }
    static_cast<EventLoop*>(arg)->drainTasks();
}

void EventLoop::run() {
    if (m_running.load(std::memory_order_acquire)) {
        return;
    }

    // Событие пробуждения — тоже до старта потока.
    if (!m_wake) {
        m_wake = event_new(m_base, m_wakeFd[0], EV_READ | EV_PERSIST, &EventLoop::onWake, this);
        if (!m_wake || event_add(m_wake, nullptr) != 0) {
            throw std::runtime_error("EventLoop: failed to register wakeup event");
        }
    }

    // Таймер регистрируем ДО старта потока: база не должна быть пустой
    // ни в один момент, иначе event_base_loop вернётся немедленно.
    if (!m_tick) {
        m_tick = event_new(m_base, -1, EV_PERSIST, &EventLoop::onTick, this);
        if (!m_tick) {
            throw std::runtime_error("EventLoop: failed to create tick event");
        }
        timeval tv{};
        tv.tv_sec = kTickMs / 1000;
        tv.tv_usec = (kTickMs % 1000) * 1000;
        if (event_add(m_tick, &tv) != 0) {
            event_free(m_tick);
            m_tick = nullptr;
            throw std::runtime_error("EventLoop: failed to arm tick event");
        }
    }

    m_running.store(true, std::memory_order_release);
    m_thread = std::make_unique<std::thread>(runLoop, this);
}

void EventLoop::stop() {
    if (!m_running.load(std::memory_order_acquire)) {
        return;
    }
    // Флаг читает onTick в потоке цикла и вызывает loopbreak оттуда же:
    // event_base_loopbreak из чужого потока небезопасен без evthread_use_pthreads.
    m_running.store(false, std::memory_order_release);
    if (m_thread && m_thread->joinable()) {
        m_thread->join();
    }
    m_thread.reset(nullptr);
}

void EventLoop::onTick(int /*fd*/, short /*what*/, void* arg) {
    auto* self = static_cast<EventLoop*>(arg);
    if (!self->m_running.load(std::memory_order_acquire)) {
        // Дочерпываем очередь: иначе ожидающий runInLoop() не дождётся никогда.
        self->drainTasks();
        event_base_loopbreak(self->m_base);
        return;
    }
    // Периодическая работа транспорта — heartbeat. Исключение отсюда утащило бы
    // за собой поток цикла, поэтому гасим его здесь.
    if (self->m_onTick) {
        try { self->m_onTick(); } catch (...) {}
    }
}

void EventLoop::runLoop(EventLoop* self) {
    t_loopOwner = self;
    // Блокирующий цикл: поток спит на poll(), 0% CPU в простое.
    // Выход — loopbreak из onTick (см. stop()).
    int flags = 0;
#ifdef EVLOOP_NO_EXIT_ON_EMPTY
    flags |= EVLOOP_NO_EXIT_ON_EMPTY;
#endif
    while (self->m_running.load(std::memory_order_acquire)) {
        if (event_base_loop(self->m_base, flags) != 0) {
            // База опустела (сборка libevent без NO_EXIT_ON_EMPTY) — не крутим
            // цикл вхолостую, ждём тик и пробуем снова.
            std::this_thread::sleep_for(std::chrono::milliseconds(kTickMs));
        }
    }
    // Поток завершается: метку снимаем, иначе новый EventLoop по тому же
    // адресу принял бы чужой поток за свой.
    t_loopOwner = nullptr;
}

#else
// Windows: EventLoop не используется — PocoTransport ведёт свой поток
// и сам реализует ITaskRunner.
EventLoop::EventLoop()
    : m_base(nullptr), m_tick(nullptr), m_wake(nullptr), m_wakeFd{-1, -1}, m_running(false) {}
EventLoop::~EventLoop() {}
void EventLoop::run() {}
void EventLoop::stop() {}
void EventLoop::post(std::function<void()> task) { if (task) task(); }
void EventLoop::drainTasks() {}
#endif

} // namespace BlackRabbitMQ
