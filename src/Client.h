#pragma once

#include <amqpcpp.h>
#include <atomic>
#include <functional>
#include <memory>
#include <string>

namespace BlackRabbitMQ {

class Connection;
class Channel;
struct Message;

// Главный фасад для работы с RabbitMQ.
// Владеет Connection и каналами. Потокобезопасен в пределах
// одного потока 1С (как и upstream).
class Client {
public:
    Client() = default;
    ~Client();

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    // --- Подключение ---

    void connect(
        const std::string& host,
        uint16_t port,
        const std::string& user,
        const std::string& password,
        const std::string& vhost = "/",
        bool ssl = false,
        int timeoutSec = 30
    );

    void disconnect();
    bool reconnect();
    bool isConnected() const noexcept { return m_connected.load(std::memory_order_acquire); }
    const std::string& lastError() const noexcept { return m_error; }

    // --- Exchange ---

    void declareExchange(
        const std::string& name,
        AMQP::ExchangeType type,
        bool passive = false,
        bool durable = false,
        bool autoDelete = false,
        const AMQP::Table& args = {}
    );

    void deleteExchange(
        const std::string& name,
        bool ifUnused = false
    );

    // --- Queue ---

    void declareQueue(
        const std::string& name,
        bool passive = false,
        bool durable = false,
        bool exclusive = false,
        bool autoDelete = false,
        const AMQP::Table& args = {}
    );

    void deleteQueue(
        const std::string& name,
        bool ifUnused = false,
        bool ifEmpty = false
    );

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

    void publish(
        const std::string& exchange,
        const std::string& routingKey,
        const std::string& body
    );

    // --- Каналы ---

    // Создать новый канал. Вызывающий владеет каналом.
    std::unique_ptr<Channel> createChannel();

    // --- Consume (событийная модель) ---

    // Запустить потребителя. onMessage вызывается из потока EventLoop.
    // Возвращает consumer tag.
    std::string startConsumer(
        const std::string& queue,
        const std::string& consumerId = "",
        bool exclusive = false,
        uint16_t prefetchCount = 0,
        const AMQP::Table& args = {},
        std::function<void(const Message&)> onMessage = nullptr
    );

    // Отменить потребителя.
    void stopConsumer();

    // --- Ack / Reject ---

    void ack(uint64_t deliveryTag);
    void reject(uint64_t deliveryTag, bool requeue = false);

private:
    Channel& getOrCreateChannel();
    int makeFlags(bool passive, bool durable, bool exclusive, bool autoDelete) const;

    std::unique_ptr<Connection> m_connection;
    std::unique_ptr<Channel> m_channel;         // для declare/bind/publish
    std::unique_ptr<Channel> m_consumeChannel;  // для consume
    std::atomic<bool> m_connected{false};
    std::string m_error;
};

} // namespace BlackRabbitMQ
