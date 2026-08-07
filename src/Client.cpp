#include "Client.h"
#include "Connection.h"
#include "Channel.h"
#include "Message.h"
#include "Logger.h"

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
    int timeoutSec,
    const TlsOptions& tls,
    int heartbeatSec)
{
    m_error.clear();
    m_timeoutSec = timeoutSec > 0 ? timeoutSec : 30;
    // Политику TLS дальше хранит транспорт — при Reconnect она сохраняется.

    AMQP::Address address(host, port, AMQP::Login(user, password), vhost, ssl);

    m_connection.reset(new Connection(address, timeoutSec, tls, heartbeatSec));

    BRMQ_LOG_INFO("Connect: " + std::string(ssl ? "amqps://" : "amqp://") + user + "@"
                  + host + ":" + std::to_string(port) + vhost
                  + ", timeout " + std::to_string(m_timeoutSec) + " s");

    try {
        m_connection->connect();
        m_connected.store(true, std::memory_order_release);
        BRMQ_LOG_INFO("Connected to " + host + ":" + std::to_string(port));
    } catch (const std::exception& e) {
        m_error = e.what();
        m_connection.reset(nullptr);
        m_connected.store(false, std::memory_order_release);
        BRMQ_LOG_ERROR("Connect failed: " + m_error);
        throw;
    }
}

bool Client::isConnected() const {
    return m_connected.load(std::memory_order_acquire)
        && m_connection && m_connection->isConnected();
}

void Client::disconnect() {
    m_channel.reset(nullptr);

    if (m_connection) {
        m_connection->disconnect();
        m_connection.reset(nullptr);
    }

    m_connected.store(false, std::memory_order_release);
}

bool Client::reconnect() {
    if (!m_connection) {
        m_error = "Not connected: use Connect() first";
        return false;
    }
    try {
        // Старый канал публикации мёртв вместе с соединением;
        // новый создастся лениво при первой операции.
        m_channel.reset(nullptr);
        if (!m_connection->reconnect()) {
            m_error = m_connection->lastError();
            if (m_error.empty()) m_error = "Reconnect failed";
            m_connected.store(false, std::memory_order_release);
            BRMQ_LOG_ERROR("Reconnect failed: " + m_error);
            return false;
        }
        m_connected.store(true, std::memory_order_release);
        BRMQ_LOG_INFO("Reconnected");
        return true;
    } catch (const std::exception& e) {
        m_error = e.what();
        m_connected.store(false, std::memory_order_release);
        BRMQ_LOG_ERROR("Reconnect failed: " + m_error);
        return false;
    }
}

// --- Каналы ---

std::unique_ptr<Channel> Client::createChannel() {
    if (!m_connection || !m_connection->isConnected()) {
        throw std::runtime_error("Not connected");
    }
    auto channel = std::make_unique<Channel>(m_connection->createChannel(),
                                            m_connection->taskRunner());
    channel->setTimeout(m_timeoutSec * 1000);
    return channel;
}

// --- Вспомогательные методы ---

void Client::setPublishMode(Channel::PublishMode mode) {
    m_publishMode = mode;
    if (m_channel) m_channel->setPublishMode(mode);
}

void Client::setPublishBatchSize(size_t size) {
    m_publishBatchSize = size;
    if (m_channel) m_channel->setPublishBatchSize(size);
}

void Client::flushPublish() {
    // Канала может не быть вовсе: тогда и подтверждать нечего.
    if (!m_channel) return;
    try {
        m_channel->flushPublish();
    } catch (const std::exception& e) {
        m_error = e.what();
        throw;
    }
}

Channel& Client::getOrCreateChannel() {
    if (!m_connection || !m_connection->isConnected()) {
        throw std::runtime_error("Not connected");
    }
    if (!m_channel || !m_channel->usable()) {
        m_channel = createChannel();
        m_channel->setPublishMode(m_publishMode);
        m_channel->setPublishBatchSize(m_publishBatchSize);
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

Channel::QueueStats Client::declareQueue(
    const std::string& name,
    bool passive,
    bool durable,
    bool exclusive,
    bool autoDelete,
    const AMQP::Table& args)
{
    try {
        return getOrCreateChannel().declareQueue(
            name, makeFlags(passive, durable, exclusive, autoDelete), args);
    } catch (const std::exception& e) {
        m_error = e.what();
        throw;
    }
}

uint32_t Client::purgeQueue(const std::string& name) {
    try {
        const uint32_t purged = getOrCreateChannel().purgeQueue(name);
        BRMQ_LOG_INFO("PurgeQueue: '" + name + "', removed " + std::to_string(purged));
        return purged;
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
    // Бинарно-безопасно: .data() + .size(), не strlen
    AMQP::Envelope env(body.data(), body.size());
    publish(exchange, routingKey, env);
}

void Client::publish(
    const std::string& exchange,
    const std::string& routingKey,
    const AMQP::Envelope& envelope)
{
    try {
        getOrCreateChannel().publish(exchange, routingKey, envelope);
    } catch (const std::exception& e) {
        m_error = e.what();
        throw;
    }
}

} // namespace BlackRabbitMQ
