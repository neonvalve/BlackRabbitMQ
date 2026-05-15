#include "Consumer.h"
#include "Channel.h"
#include "Message.h"

#include <amqpcpp.h>

namespace BlackRabbitMQ {

Consumer::Consumer()
    : m_active(false)
{
}

Consumer::~Consumer() {
    cancel();
}

void Consumer::start(
    std::unique_ptr<Channel> channel,
    const std::string& queue,
    const std::string& consumerId,
    bool exclusive,
    uint16_t prefetchCount,
    const AMQP::Table& args,
    MessageCallback onMessage,
    CancelledCallback onCancelled)
{
    if (m_active.load(std::memory_order_acquire)) {
        cancel();
    }

    m_channel = std::move(channel);
    m_queueName = queue;
    m_tag.clear();

    if (prefetchCount > 0) {
        m_channel->setQos(prefetchCount);
    }

    int flags = exclusive ? AMQP::exclusive : 0;

    // Сохранить tag, полученный в onSuccess
    std::string capturedTag;

    m_channel->consume(
        queue,
        consumerId,
        flags,
        args,
        // onMessage
        [onMessage = std::move(onMessage)](const Message& msg, uint64_t, bool) {
            if (onMessage) {
                onMessage(msg);
            }
        },
        // onCancelled
        [this, onCancelled = std::move(onCancelled)](const std::string& consumerTag) {
            m_active.store(false, std::memory_order_release);
            if (onCancelled) {
                onCancelled(consumerTag);
            }
        }
    );

    // consume() в Channel блокируется до ответа брокера (onSuccess/onError).
    // В onSuccess AMQP-CPP передаёт tag, но мы не можем его перехватить
    // до того, как Channel::consume() вернётся.
    // Ставим флаг активности — потребитель зарегистрирован.
    m_active.store(true, std::memory_order_release);
}

void Consumer::ack(uint64_t deliveryTag) {
    if (m_channel && m_channel->usable()) {
        m_channel->ack(deliveryTag);
    }
}

void Consumer::reject(uint64_t deliveryTag, bool requeue) {
    if (m_channel && m_channel->usable()) {
        m_channel->reject(deliveryTag, requeue);
    }
}

void Consumer::cancel() {
    if (!m_active.load(std::memory_order_acquire)) {
        return;
    }
    // Закрытие канала автоматически отменяет потребителя.
    // onCancelled callback вызовется из EventLoop при обработке закрытия.
    m_channel.reset(nullptr);
    m_active.store(false, std::memory_order_release);
}

} // namespace BlackRabbitMQ
