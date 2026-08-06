#pragma once

#include <condition_variable>
#include <exception>
#include <functional>
#include <mutex>

namespace BlackRabbitMQ {

// Исполнитель задач в потоке event loop.
//
// AMQP-CPP не потокобезопасен, и дело не только в гонках данных: отправка
// кадра и привязка callback'а — два отдельных шага, между которыми поток цикла
// успевает разобрать ответ брокера. `Deferred::reportSuccess` вызывает
// callback, только если он уже привязан (deferred.h:85), поэтому результат
// молча теряется, а вызывающая сторона ждёт до таймаута. Под нагрузкой это
// воспроизводится: публикация 10 000 сообщений падала с
// «publish: timeout 10000 ms waiting for broker».
//
// Поэтому любое обращение к AMQP-CPP исполняется в потоке цикла целиком.
class ITaskRunner {
public:
    virtual ~ITaskRunner() = default;

    // Вызывающий код уже находится в потоке цикла?
    virtual bool inLoopThread() const noexcept = 0;

    // Поставить задачу в очередь потока цикла. Не ждёт исполнения.
    virtual void post(std::function<void()> task) = 0;

    // Исполнить в потоке цикла и дождаться. Из самого потока цикла выполняется
    // на месте — иначе ожидание собственной задачи было бы самоблокировкой
    // (так вызывается ack/reject из onMessage).
    void runInLoop(const std::function<void()>& task) {
        if (inLoopThread()) {
            task();
            return;
        }

        std::mutex mutex;
        std::condition_variable cv;
        bool done = false;
        std::exception_ptr failure;

        post([&]() {
            try {
                task();
            } catch (...) {
                failure = std::current_exception();
            }
            std::lock_guard<std::mutex> lock(mutex);
            done = true;
            cv.notify_all();
        });

        {
            std::unique_lock<std::mutex> lock(mutex);
            cv.wait(lock, [&]() { return done; });
        }

        if (failure) std::rethrow_exception(failure);
    }
};

} // namespace BlackRabbitMQ
