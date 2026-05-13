#pragma once

#include <amqpcpp.h>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

namespace BlackRabbitMQ {

struct Message;

// Обёртка над AMQP::TcpChannel с синхронным API.
// Каждая операция (declare, bind, publish) блокируется до ответа брокера
// и либо завершается успешно, либо бросает std::runtime_error.
// EventLoop должен быть запущен до вызова методов Channel.
class Channel {
public:
    explicit Channel(std::unique_ptr<AMQP::TcpChannel> ch);
    ~Channel();

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
    void declareQueue(
        const std::string& name,
        int flags = 0,
        const AMQP::Table& args = {}
    );
    void removeQueue(const std::string& name, int flags = 0);

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
    void publish(
        const std::string& exchange,
        const std::string& routingKey,
        const AMQP::Envelope& envelope
    );

    // --- QoS ---
    void setQos(uint16_t prefetchCount);

    // --- Consume ---
    // Запускает потребителя. onMessage вызывается из потока EventLoop.
    void consume(
        const std::string& queue,
        const std::string& consumerTag,
        int flags,
        const AMQP::Table& args,
        std::function<void(const Message&, uint64_t, bool)> onMessage,
        std::function<void(const std::string&)> onCancelled
    );

    // --- Ack / Reject ---
    void ack(uint64_t deliveryTag);
    void reject(uint64_t deliveryTag, bool requeue);

    // Прямой доступ к AMQP-каналу для продвинутых сценариев.
    AMQP::TcpChannel* raw() const noexcept { return m_channel.get(); }

    // Проверка, жив ли канал.
    bool usable() const noexcept {
        return m_channel && m_channel->usable();
    }

private:
    void wait();
    void signalSuccess();
    void signalError(const char* message);

    std::unique_ptr<AMQP::TcpChannel> m_channel;

    std::mutex m_mutex;
    std::condition_variable m_cv;
    std::atomic<bool> m_ready{false};
    std::string m_error;
};

} // namespace BlackRabbitMQ
