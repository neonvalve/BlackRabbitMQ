#include "Client.h"
#include "Connection.h"
#include "Channel.h"
#include "Message.h"

#include <amqpcpp.h>
#include <stdexcept>

namespace BlackRabbitMQ {

Client::~Client() {
    disconnect();
}

// --- Подключение ---

void Client::connect(
    const std::string& host,
    uint16_t port,
    const std::string& user,
    const std::string& password,
    const std::string& vhost,
    bool ssl,
    int timeoutSec)
{
    m_error.clear();

    AMQP::Address address(host, port, AMQP::Login(user, password), vhost, ssl);

    m_connection.reset(new Connection(address, timeoutSec));

    try {
        m_connection->connect();
        m_connected.store(true, std::memory_order_release);
    } catch (const std::exception& e) {
        m_error = e.what();
        m_connection.reset(nullptr);
        m_connected.store(false, std::memory_order_release);
        throw;
    }
}

void Client::disconnect() {
    m_consumeChannel.reset(nullptr);
    m_channel.reset(nullptr);

    if (m_connection) {
        m_connection->disconnect();
        m_connection.reset(nullptr);
    }

    m_connected.store(false, std::memory_order_release);
}

bool Client::reconnect() {
    if (!m_connection) return false;
    try {
        m_channel.reset(nullptr);
        m_consumeChannel.reset(nullptr);
        m_connection->reconnect();
        m_connected.store(true, std::memory_order_release);
        return true;
    } catch (const std::exception& e) {
        m_error = e.what();
        m_connected.store(false, std::memory_order_release);
        return false;
    }
}

// --- Каналы ---

std::unique_ptr<Channel> Client::createChannel() {
    if (!m_connection || !m_connection->isConnected()) {
        throw std::runtime_error("Not connected");
    }
    return std::make_unique<Channel>(m_connection->createChannel());
}

// --- Вспомогательные методы ---

Channel& Client::getOrCreateChannel() {
    if (!m_connection || !m_connection->isConnected()) {
        throw std::runtime_error("Not connected");
    }
    if (!m_channel || !m_channel->usable()) {
        m_channel.reset(new Channel(m_connection->createChannel()));
    }
    return *m_channel;
}

int Client::makeFlags(bool passive, bool durable, bool exclusive, bool autoDelete) const {
    int flags = 0;
    if (passive) flags |= AMQP::passive;
    if (durable) flags |= AMQP::durable;
    if (exclusive) flags |= AMQP::exclusive;
    if (autoDelete) flags |= AMQP::autodelete;
    return flags;
}

// --- Exchange ---

void Client::declareExchange(
    const std::string& name,
    AMQP::ExchangeType type,
    bool passive,
    bool durable,
    bool autoDelete,
    const AMQP::Table& args)
{
    try {
        getOrCreateChannel().declareExchange(name, type, makeFlags(passive, durable, false, autoDelete), args);
    } catch (const std::exception& e) {
        m_error = e.what();
        throw;
    }
}

void Client::deleteExchange(const std::string& name, bool ifUnused) {
    try {
        getOrCreateChannel().removeExchange(name, ifUnused ? AMQP::ifunused : 0);
    } catch (const std::exception& e) {
        m_error = e.what();
        throw;
    }
}

// --- Queue ---

void Client::declareQueue(
    const std::string& name,
    bool passive,
    bool durable,
    bool exclusive,
    bool autoDelete,
    const AMQP::Table& args)
{
    try {
        getOrCreateChannel().declareQueue(name, makeFlags(passive, durable, exclusive, autoDelete), args);
    } catch (const std::exception& e) {
        m_error = e.what();
        throw;
    }
}

void Client::deleteQueue(const std::string& name, bool ifUnused, bool ifEmpty) {
    try {
        getOrCreateChannel().removeQueue(name, (ifUnused ? AMQP::ifunused : 0) | (ifEmpty ? AMQP::ifempty : 0));
    } catch (const std::exception& e) {
        m_error = e.what();
        throw;
    }
}

void Client::bindQueue(
    const std::string& exchange,
    const std::string& queue,
    const std::string& routingKey,
    const AMQP::Table& args)
{
    try {
        getOrCreateChannel().bindQueue(exchange, queue, routingKey, args);
    } catch (const std::exception& e) {
        m_error = e.what();
        throw;
    }
}

void Client::unbindQueue(
    const std::string& exchange,
    const std::string& queue,
    const std::string& routingKey)
{
    try {
        getOrCreateChannel().unbindQueue(exchange, queue, routingKey);
    } catch (const std::exception& e) {
        m_error = e.what();
        throw;
    }
}

// --- Publish ---

void Client::publish(
    const std::string& exchange,
    const std::string& routingKey,
    const std::string& body)
{
    try {
        // Бинарно-безопасно: .data() + .size(), не strlen
        AMQP::Envelope env(body.data(), body.size());
        getOrCreateChannel().publish(exchange, routingKey, env);
    } catch (const std::exception& e) {
        m_error = e.what();
        throw;
    }
}

// --- Consume ---

std::string Client::startConsumer(
    const std::string& queue,
    const std::string& consumerId,
    bool exclusive,
    uint16_t prefetchCount,
    const AMQP::Table& args,
    std::function<void(const Message&)> onMessage)
{
    if (!m_connection || !m_connection->isConnected()) {
        throw std::runtime_error("Not connected");
    }

    // Создать отдельный канал для consume
    m_consumeChannel.reset(new Channel(m_connection->createChannel()));

    if (prefetchCount > 0) {
        m_consumeChannel->setQos(prefetchCount);
    }

    std::string tag;
    bool gotTag = false;

    int flags = (exclusive ? AMQP::exclusive : 0);

    m_consumeChannel->consume(
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
        [&tag](const std::string& consumerTag) {
            tag = consumerTag;
        }
    );

    return tag;
}

void Client::stopConsumer() {
    m_consumeChannel.reset(nullptr);
}

// --- Ack / Reject ---
// Ack и Reject должны выполняться на канале consume
// (том же, через который получено сообщение).
// Если consume не активен, используем publish-канал.

void Client::ack(uint64_t deliveryTag) {
    Channel* ch = m_consumeChannel.get();
    if (!ch || !ch->usable()) ch = m_channel.get();
    if (!ch || !ch->usable()) throw std::runtime_error("Channel not available for ack");
    ch->ack(deliveryTag);
}

void Client::reject(uint64_t deliveryTag, bool requeue) {
    Channel* ch = m_consumeChannel.get();
    if (!ch || !ch->usable()) ch = m_channel.get();
    if (!ch || !ch->usable()) throw std::runtime_error("Channel not available for reject");
    ch->reject(deliveryTag, requeue);
}

} // namespace BlackRabbitMQ
