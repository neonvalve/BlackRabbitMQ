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
    void start(
        std::unique_ptr<Channel> channel,
        const std::string& queue,
        const std::string& consumerId = "",
        bool exclusive = false,
        uint16_t prefetchCount = 0,
        const AMQP::Table& args = {},
        MessageCallback onMessage = nullptr,
        CancelledCallback onCancelled = nullptr
    );

    // Отменить потребителя и освободить канал.
    void cancel();

    bool isActive() const noexcept { return m_active.load(std::memory_order_acquire); }
    const std::string& tag() const noexcept { return m_tag; }
    const std::string& queueName() const noexcept { return m_queueName; }

private:
    std::unique_ptr<Channel> m_channel;
    std::string m_queueName;
    std::string m_tag;
    std::atomic<bool> m_active;
};

} // namespace BlackRabbitMQ
