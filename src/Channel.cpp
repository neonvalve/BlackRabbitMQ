#include "Channel.h"
#include "Message.h"

#include <stdexcept>

namespace BlackRabbitMQ {

Channel::Channel(std::unique_ptr<AMQP::Channel> ch)
    : m_channel(std::move(ch))
{
    if (!m_channel) {
        throw std::runtime_error("Channel: null Channel");
    }
}

Channel::~Channel() = default;

// --- Private: синхронизация ---

void Channel::wait() {
    std::unique_lock<std::mutex> lock(m_mutex);
    m_cv.wait(lock, [this]() { return m_ready.load(std::memory_order_acquire); });
    m_ready.store(false, std::memory_order_release);
    if (!m_error.empty()) {
        std::string err = std::move(m_error);
        m_error.clear();
        throw std::runtime_error(err);
    }
}

void Channel::signalSuccess() {
    m_ready.store(true, std::memory_order_release);
    m_cv.notify_all();
}

void Channel::signalError(const char* message) {
    if (message) m_error = message;
    signalSuccess();
}

// --- Exchange ---

void Channel::declareExchange(
    const std::string& name,
    AMQP::ExchangeType type,
    int flags,
    const AMQP::Table& args)
{
    m_channel->declareExchange(name, type, flags, args)
        .onSuccess([this]() { signalSuccess(); })
        .onError([this](const char* msg) { signalError(msg); });
    wait();
}

void Channel::removeExchange(const std::string& name, int flags) {
    m_channel->removeExchange(name, flags)
        .onSuccess([this]() { signalSuccess(); })
        .onError([this](const char* msg) { signalError(msg); });
    wait();
}

// --- Queue ---

void Channel::declareQueue(
    const std::string& name,
    int flags,
    const AMQP::Table& args)
{
    m_channel->declareQueue(name, flags, args)
        .onSuccess([this]() { signalSuccess(); })
        .onError([this](const char* msg) { signalError(msg); });
    wait();
}

void Channel::removeQueue(const std::string& name, int flags) {
    m_channel->removeQueue(name, flags)
        .onSuccess([this]() { signalSuccess(); })
        .onError([this](const char* msg) { signalError(msg); });
    wait();
}

// --- Binding ---

void Channel::bindQueue(
    const std::string& exchange,
    const std::string& queue,
    const std::string& routingKey,
    const AMQP::Table& args)
{
    m_channel->bindQueue(exchange, queue, routingKey, args)
        .onSuccess([this]() { signalSuccess(); })
        .onError([this](const char* msg) { signalError(msg); });
    wait();
}

void Channel::unbindQueue(
    const std::string& exchange,
    const std::string& queue,
    const std::string& routingKey)
{
    m_channel->unbindQueue(exchange, queue, routingKey)
        .onSuccess([this]() { signalSuccess(); })
        .onError([this](const char* msg) { signalError(msg); });
    wait();
}

// --- Publish ---

void Channel::publish(
    const std::string& exchange,
    const std::string& routingKey,
    const AMQP::Envelope& envelope)
{
    m_channel->startTransaction();
    m_channel->publish(exchange, routingKey, envelope);
    m_channel->commitTransaction()
        .onSuccess([this]() { signalSuccess(); })
        .onError([this](const char* msg) { signalError(msg); });
    wait();
}

// --- QoS ---

void Channel::setQos(uint16_t prefetchCount) {
    m_channel->setQos(prefetchCount)
        .onSuccess([this]() { signalSuccess(); })
        .onError([this](const char* msg) { signalError(msg); });
    wait();
}

// --- Consume ---

void Channel::consume(
    const std::string& queue,
    const std::string& consumerTag,
    int flags,
    const AMQP::Table& args,
    std::function<void(const Message&, uint64_t, bool)> onMessage,
    std::function<void(const std::string&)> /*onCancelled*/)
{
    // NOTE: onCancelled не поддерживается в AMQP-CPP v4.1.4.
    // Отмена потребителя определяется через channel->usable().
    m_channel->consume(queue, consumerTag, flags, args)
        .onSuccess([this]() { signalSuccess(); })
        .onMessage([onMessage = std::move(onMessage)](
            const AMQP::Message& msg, uint64_t deliveryTag, bool redelivered) {
            onMessage(Message::from(msg, deliveryTag, redelivered), deliveryTag, redelivered);
        })
        .onError([this](const char* msg) { signalError(msg); });
    wait();
}

// --- Ack / Reject ---

void Channel::ack(uint64_t deliveryTag) {
    m_channel->ack(deliveryTag);
}

void Channel::reject(uint64_t deliveryTag, bool requeue) {
    int flags = requeue ? AMQP::requeue : 0;
    m_channel->reject(deliveryTag, flags);
}

} // namespace BlackRabbitMQ
