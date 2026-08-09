#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <amqpcpp/table.h>

namespace BlackRabbitMQ {

class Channel;
struct Message;

// RAII-потребитель очереди RabbitMQ.
// Владеет выделенным каналом. При разрушении отменяет потребителя.
// onMessage и onCancelled вызываются из потока EventLoop.
class Consumer {
public:
    // Типы callback'ов.
    // onMessage: сообщение из очереди
    // onCancelled: потребитель отменён (брокером или локально)
    using MessageCallback = std::function<void(const Message&)>;
    using CancelledCallback = std::function<void(const std::string& consumerTag)>;

    Consumer();
    ~Consumer();

    Consumer(const Consumer&) = delete;
    Consumer& operator=(const Consumer&) = delete;

    // Запустить потребителя на переданном канале.
    // Канал переходит во владение Consumer.
    // noAck — режим авто-подтверждения (BasicConsume(..., НеПодтверждать)):
    // брокер не ждёт ack, сообщение считается доставленным сразу.
    void start(
        std::unique_ptr<Channel> channel,
        const std::string& queue,
        const std::string& consumerId = "",
        bool noAck = false,
        bool exclusive = false,
        uint16_t prefetchCount = 0,
        const AMQP::Table& args = {},
        MessageCallback onMessage = nullptr,
        CancelledCallback onCancelled = nullptr
    );

    // Отменить потребителя и освободить канал.
    void cancel();

    bool isActive() const noexcept { return m_active.load(std::memory_order_acquire); }

    // Потребитель жив И его канал ещё пригоден: только тогда ack/reject дойдут
    // до брокера. Канал мог умереть асинхронно (ошибка на нём закрывает канал).
    bool canAck() const noexcept;
    const std::string& tag() const noexcept { return m_tag; }
    const std::string& queueName() const noexcept { return m_queueName; }

    // Ack/Reject через канал потребителя (обязательно тот же канал!)
    // multiple: подтвердить всё до deliveryTag включительно.
    void ack(uint64_t deliveryTag, bool multiple = false);
    void reject(uint64_t deliveryTag, bool requeue = false);

private:
    std::string m_queueName;
    std::string m_tag;
    std::atomic<bool> m_active;

    // Объявлен последним: уничтожается первым — callback'и, которые AMQP-CPP
    // может вызвать при закрытии канала, обращаются к остальным полям.
    std::unique_ptr<Channel> m_channel;
};

} // namespace BlackRabbitMQ
