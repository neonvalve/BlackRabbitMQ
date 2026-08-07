#pragma once

#include "TaskRunner.h"

#include <amqpcpp.h>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

namespace BlackRabbitMQ {

struct Message;

// Обёртка над AMQP::Channel с синхронным API.
// Каждая операция (declare, bind, publish) блокируется до ответа брокера
// и либо завершается успешно, либо бросает std::runtime_error — в том числе
// по таймауту: ожидание всегда ограничено, зависнуть в 1С нельзя.
// EventLoop должен быть запущен до вызова методов Channel.
// Работает на Linux (Channel) и Windows (Channel) через базовый тип.
class Channel {
public:
    // Ограничение на ответ брокера. Молчание дольше — ошибка, а не зависание.
    static constexpr int kDefaultTimeoutMs = 30000;

    // Принимает Channel (Linux) или Channel (Windows) — оба наследуют AMQP::Channel.
    // runner исполняет обращения к AMQP-CPP в потоке event loop: отправка кадра
    // и привязка callback'а должны быть неделимы относительно разбора ответов.
    Channel(std::unique_ptr<AMQP::Channel> ch, ITaskRunner& runner);
    ~Channel();

    // Таймаут одной операции, мс. 0 — вернуть значение по умолчанию.
    void setTimeout(int timeoutMs) {
        m_timeoutMs = timeoutMs > 0 ? timeoutMs : kDefaultTimeoutMs;
    }

    // Чем подтверждается публикация.
    // Confirms — расширение RabbitMQ (confirm.select): быстрее, гарантия та же.
    // Transactions — класс tx из спецификации AMQP 0.9.1: медленнее вдвое,
    // но работает на брокерах, которые confirms не поддерживают.
    enum class PublishMode { Confirms, Transactions };

    void setPublishMode(PublishMode mode) noexcept { m_publishMode = mode; }
    PublishMode publishMode() const noexcept { return m_publishMode; }

    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    // --- Exchange ---
    void declareExchange(
        const std::string& name,
        AMQP::ExchangeType type,
        int flags = 0,
        const AMQP::Table& args = {}
    );
    void removeExchange(const std::string& name, int flags = 0);

    // --- Queue ---
    // Брокер отвечает на declare числом сообщений и потребителей. Раньше эти
    // значения отбрасывались, хотя именно их спрашивают в эксплуатации:
    // «сколько накопилось» и «есть ли кто-то на очереди».
    struct QueueStats {
        uint32_t messageCount = 0;
        uint32_t consumerCount = 0;
    };

    QueueStats declareQueue(
        const std::string& name,
        int flags = 0,
        const AMQP::Table& args = {}
    );
    void removeQueue(const std::string& name, int flags = 0);
    // Очищает очередь, не трогая её саму и привязки. Возвращает,
    // сколько сообщений удалено.
    uint32_t purgeQueue(const std::string& name);

    // --- Binding ---
    void bindQueue(
        const std::string& exchange,
        const std::string& queue,
        const std::string& routingKey,
        const AMQP::Table& args = {}
    );
    void unbindQueue(
        const std::string& exchange,
        const std::string& queue,
        const std::string& routingKey
    );

    // --- Publish ---
    // Ждёт подтверждения брокера (publisher confirms): управление вернётся
    // только когда сообщение принято и записано в очередь, иначе — исключение.
    // mandatory: сообщение, которое не попало ни в одну очередь (нет привязки
    // или опечатка в ключе), возвращается брокером и превращается в ошибку —
    // без этого флага такая публикация «успешна», а сообщение исчезает.
    void publish(
        const std::string& exchange,
        const std::string& routingKey,
        const AMQP::Envelope& envelope,
        bool mandatory = true
    );

    // --- QoS ---
    void setQos(uint16_t prefetchCount);

    // --- Consume ---
    // Запускает потребителя. onMessage вызывается из потока EventLoop.
    // Возвращает consumer tag, присвоенный брокером (его ждёт 1С от BasicConsume).
    std::string consume(
        const std::string& queue,
        const std::string& consumerTag,
        int flags,
        const AMQP::Table& args,
        std::function<void(const Message&, uint64_t, bool)> onMessage,
        std::function<void(const std::string&)> onCancelled = nullptr
    );

    // --- Ack / Reject ---
    void ack(uint64_t deliveryTag);
    void reject(uint64_t deliveryTag, bool requeue);

    // Прямой доступ к AMQP-каналу для продвинутых сценариев.
    // Обращаться можно только из потока event loop.
    AMQP::Channel* raw() const noexcept { return m_channel.get(); }

    // Проверка, жив ли канал. Состояние читается в потоке цикла.
    bool usable() const;

    // Исполнитель потока цикла: владельцу канала он нужен, чтобы освобождать
    // канал там же, где исполняются callback'и.
    ITaskRunner& runner() const noexcept { return m_runner; }

private:
    // Каждой операции выдаётся номер: ответ, пришедший после таймаута,
    // не должен быть принят за результат следующей операции.
    uint64_t beginOp();
    void wait(uint64_t seq, const char* operation);
    void signalSuccess(uint64_t seq);
    void signalError(uint64_t seq, const char* message);

    // Переводит канал в режим подтверждений и подписывается на возвраты.
    // Вызывается лениво, в потоке цикла, один раз на канал.
    void ensureConfirms();
    void ensureReturnHandler();

    std::mutex m_mutex;
    std::condition_variable m_cv;
    bool m_ready{false};
    std::string m_error;
    std::string m_consumerTag;
    // Результаты последней операции с очередью: заполняются в потоке цикла
    // до signalSuccess, читаются вызывающим после wait — под тем же мьютексом.
    QueueStats m_queueStats;
    uint32_t m_purgedCount{0};
    uint64_t m_seq{0};
    int m_timeoutMs{kDefaultTimeoutMs};
    ITaskRunner& m_runner;

    // Подтверждения публикации. Публикация синхронная — в полёте всегда
    // не больше одной, поэтому достаточно помнить номер ожидающей операции.
    bool m_confirmsEnabled{false};
    bool m_returnHandlerSet{false};
    uint64_t m_pendingPublishOp{0};
    std::string m_returnedReason;
    PublishMode m_publishMode{PublishMode::Confirms};

    // Объявлен последним: уничтожается первым, до мьютекса и cv, — callback'и
    // AMQP-CPP, срабатывающие при закрытии канала, обращаются к ним.
    std::unique_ptr<AMQP::Channel> m_channel;
};

} // namespace BlackRabbitMQ
