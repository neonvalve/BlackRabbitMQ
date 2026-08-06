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

    setError("");
    m_timeoutSec = timeoutSec; // тем же лимитом ограничено открытие каналов

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
    // Соединение уничтожается в потоке цикла — это последнее обращение
    // к AMQP-CPP, и оно не должно пересечься с разбором входящих кадров.
    if (m_amqpConn && m_eventLoop) {
        m_eventLoop->runInLoop([this]() { m_amqpConn.reset(nullptr); });
    }
    m_amqpConn.reset(nullptr);

    if (m_eventLoop) {
        m_eventLoop->stop();
        m_eventLoop.reset(nullptr);
    }
    m_handler.reset(nullptr);
    m_connected.store(false, std::memory_order_release);
}

std::unique_ptr<AMQP::Channel> LibeventTransport::createChannel() {
    if (!m_eventLoop) {
        throw std::runtime_error("Connection not usable");
    }

    // Состояние ожидания живёт в shared_ptr: при таймауте мы выходим отсюда,
    // а callback'и AMQP-CPP могут сработать позже — ссылаться на локальные
    // переменные этой функции им уже нельзя.
    struct OpenState {
        std::mutex mutex;
        std::condition_variable cv;
        bool done = false;
        std::string error;
    };
    auto state = std::make_shared<OpenState>();

    // Создание канала и привязка callback'ов — в потоке цикла и неделимо:
    // между ними брокер успевает прислать channel.open-ok, и без callback'а
    // готовность канала потерялась бы.
    std::unique_ptr<AMQP::TcpChannel> channel;
    m_eventLoop->runInLoop([&]() {
        if (!m_amqpConn || !m_amqpConn->usable()) {
            throw std::runtime_error("Connection not usable");
        }
        channel = std::make_unique<AMQP::TcpChannel>(m_amqpConn.get());

        // notify под мьютексом — иначе возможен потерянный сигнал.
        channel->onReady([state]() {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->done = true;
            state->cv.notify_all();
        });
        channel->onError([state](const char* msg) {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->error = msg ? msg : "channel error";
            state->done = true;
            state->cv.notify_all();
        });
    });

    const int timeoutMs = (m_timeoutSec > 0 ? m_timeoutSec : 30) * 1000;
    std::string failure;
    {
        std::unique_lock<std::mutex> lock(state->mutex);
        if (!state->cv.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                                [&state]() { return state->done; })) {
            failure = "Channel open: timeout " + std::to_string(timeoutMs)
                + " ms waiting for broker";
        } else if (!state->error.empty()) {
            failure = state->error;
        }
    }

    // Проверка состояния, перевешивание onError и — при неудаче — уничтожение
    // канала тоже идут в потоке цикла: канал принадлежит ему.
    bool usable = false;
    m_eventLoop->runInLoop([&]() {
        usable = failure.empty() && channel && channel->usable();
        if (!usable) {
            channel.reset();
            return;
        }
        // Дальше ошибки канала только фиксируем: синхронно их уже никто не ждёт.
        channel->onError([this](const char* msg) { if (msg) setError(msg); });
    });

    if (!failure.empty()) {
        setError(failure);
        throw std::runtime_error(failure);
    }
    if (!usable) {
        throw std::runtime_error(error().empty() ? "Channel not opened" : error());
    }

    return channel;
}

void LibeventTransport::waitForReady(int timeoutSec) {
    // Событийное ожидание вместо опроса ready()/closed() из потока 1С:
    // состояние AMQP::TcpConnection принадлежит потоку event loop, читать его
    // снаружи — гонка (подтверждено ThreadSanitizer). Здесь ждём callback.
    std::string failure;
    {
        std::unique_lock<std::mutex> lock(m_handler->mutex);
        const bool signalled = m_handler->cv.wait_for(
            lock, std::chrono::seconds(timeoutSec > 0 ? timeoutSec : 30),
            [this]() { return m_handler->ready || m_handler->finished; });

        if (!signalled) {
            failure = m_handler->error.empty() ? "Connection timeout" : m_handler->error;
        } else if (!m_handler->ready) {
            failure = m_handler->error.empty() ? "Connection failed" : m_handler->error;
        }
    }

    if (!failure.empty()) {
        // disconnect() гасит поток цикла — вызывать под мьютексом обработчика нельзя.
        setError(failure);
        disconnect();
        throw std::runtime_error(failure);
    }
}

} // namespace BlackRabbitMQ
