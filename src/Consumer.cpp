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
    bool noAck,
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

    // basic.qos отправляем всегда, в том числе с нулём: ноль по AMQP означает
    // «без ограничения», а вот его отсутствие брокер трактует по-своему —
    // замерено на RabbitMQ 4.3.4: без qos потребитель получал ровно одно
    // сообщение из очереди и дальше поток вставал.
    m_channel->setQos(prefetchCount);

    int flags = exclusive ? AMQP::exclusive : 0;
    if (noAck) flags |= AMQP::noack;

    // Флаг ставим до consume: сообщения (и отмена от брокера) могут прийти
    // сразу после consume-ok, ещё до возврата из вызова.
    m_active.store(true, std::memory_order_release);
    try {
        // consume() блокируется до ответа брокера и возвращает присвоенный тег.
        m_tag = m_channel->consume(
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
    } catch (...) {
        m_active.store(false, std::memory_order_release);
        throw;
    }
}

bool Consumer::canAck() const noexcept {
    return m_active.load(std::memory_order_acquire) && m_channel && m_channel->usable();
}

void Consumer::ack(uint64_t deliveryTag, bool multiple) {
    if (m_channel && m_channel->usable()) {
        m_channel->ack(deliveryTag, multiple);
    }
}

void Consumer::reject(uint64_t deliveryTag, bool requeue) {
    if (m_channel && m_channel->usable()) {
        m_channel->reject(deliveryTag, requeue);
    }
}

void Consumer::cancel() {
    // Закрытие канала автоматически отменяет потребителя.
    // onCancelled callback вызовется из EventLoop при обработке закрытия.
    // Канал освобождаем всегда — даже если start() не дошёл до конца
    // и m_active остался false, иначе канал утечёт до смерти соединения.
    m_active.store(false, std::memory_order_release);
    if (m_channel) {
        // Освобождаем в потоке цикла: там же исполняется onMessage, откуда
        // прикладной код зовёт ack/reject по этому каналу. Иначе уничтожение
        // пересекается с работающим callback'ом (гонка, видна в TSan).
        m_channel->runner().runInLoop([this]() { m_channel.reset(nullptr); });
    }
    m_tag.clear();
}

} // namespace BlackRabbitMQ
