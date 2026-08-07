#pragma once

#include <amqpcpp.h>
#include <atomic>
#include <functional>
#include <memory>
#include <string>

#include "Connection.h"
#include "Channel.h"
#include "TlsOptions.h"

namespace BlackRabbitMQ {

struct Message;

// Главный фасад для работы с RabbitMQ.
// Владеет Connection и каналами. Потокобезопасен в пределах
// одного потока 1С (как и типовых решениях).
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
        int timeoutSec = 30,
        const TlsOptions& tls = {},
        // Интервал heartbeat: -1 — как предложит брокер, 0 — выключить.
        int heartbeatSec = -1
    );

    void disconnect();
    bool reconnect();
    // Учитывает реальное состояние транспорта: соединение могло оборваться
    // без участия 1С (перезапуск брокера, сеть).
    bool isConnected() const;
    // Возвращает копией: при обрыве причину знает транспорт, а не фасад —
    // без этого сторож переподключения писал в журнал пустую причину.
    std::string lastError() const {
        if (!m_error.empty() || !m_connection) return m_error;
        return m_connection->lastError();
    }

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

    // Возвращает состояние очереди со слов брокера: сколько в ней сообщений
    // и сколько потребителей. С passive = true это способ просто спросить,
    // не создавая и не меняя очередь.
    Channel::QueueStats declareQueue(
        const std::string& name,
        bool passive = false,
        bool durable = false,
        bool exclusive = false,
        bool autoDelete = false,
        const AMQP::Table& args = {}
    );

    // Очищает очередь, не удаляя её и привязки. Возвращает число удалённых.
    uint32_t purgeQueue(const std::string& name);

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

    // Чем подтверждается публикация: confirms (быстрее) или транзакции
    // (стандарт AMQP 0.9.1 — для брокеров, где confirms не поддержаны).
    void setPublishMode(Channel::PublishMode mode);

    // Размер окна пакетной публикации и ожидание подтверждений по всему
    // отправленному. В пакетном режиме без flushPublish() считать сообщения
    // доставленными нельзя.
    void setPublishBatchSize(size_t size);
    void flushPublish();

    // Тело без свойств.
    void publish(
        const std::string& exchange,
        const std::string& routingKey,
        const std::string& body
    );

    // Готовый конверт: свойства сообщения (correlationId, contentType,
    // deliveryMode, headers...) задаёт вызывающий.
    void publish(
        const std::string& exchange,
        const std::string& routingKey,
        const AMQP::Envelope& envelope
    );

    // --- Каналы ---

    // Создать новый канал. Вызывающий владеет каналом.
    // Потребление идёт через Consumer на собственном канале: ack/reject
    // обязаны выполняться на том же канале, что доставил сообщение.
    std::unique_ptr<Channel> createChannel();

private:
    Channel& getOrCreateChannel();
    int makeFlags(bool passive, bool durable, bool exclusive, bool autoDelete) const;

    std::unique_ptr<Connection> m_connection;
    std::unique_ptr<Channel> m_channel;         // для declare/bind/publish
    // Чем подтверждается публикация: confirms (быстрее, расширение RabbitMQ)
    // или транзакции (стандарт AMQP 0.9.1, для брокеров без confirms).
    Channel::PublishMode m_publishMode{Channel::PublishMode::Confirms};
    size_t m_publishBatchSize{100};
    std::atomic<bool> m_connected{false};
    std::string m_error;
    int m_timeoutSec{30};                       // лимит ожидания ответа брокера
};

} // namespace BlackRabbitMQ
