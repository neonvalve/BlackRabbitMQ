#pragma once

#include "Component.h"
#include "CallContext.h"
#include "Message.h"
#include "TlsOptions.h"

#include <amqpcpp/table.h>

#include <memory>
#include <queue>
#include <mutex>
#include <condition_variable>

namespace BlackRabbitMQ {

class Client;
class Consumer;

namespace AddIn1S {

// Совместимый с 1С API над Client и Consumer.
// Исключения не прокидываются в 1С — перехватываются в wrapCall.
//
// Ключевые решения:
//   - BasicReject(tag, requeue) — добавлен параметр requeue
//   - Publish: бинарно-безопасное тело (.data()+.size(), не strlen)
//   - Внутри: std::unique_ptr, без ручного new/delete
class RabbitApi1S : public Component {
public:
    // Потолок polling-очереди: дальше сообщения не копятся, а BasicConsumeMessage
    // сообщает об отставании. Без потолка очередь растёт до OOM процесса 1С
    // (особенно с noConfirm, где prefetch брокером не ограничен).
    static constexpr size_t kMaxQueuedMessages = 10000;

    // Глубина буфера внешних событий платформы. Значение по умолчанию рассчитано
    // на редкие уведомления: на потоке сообщений платформа начинает отказывать
    // в приёме, и события пропадают (проверено — из 50 доходило одно).
    static constexpr long kEventBufferDepth = 10000;

    RabbitApi1S();
    ~RabbitApi1S() override;

    // --- 1C Methods (имена сохранены ради совместимости с существующим кодом 1С) ---

    // Connect(host, port, user, pwd, vhost, skip, ssl, timeout)
    void connectImpl(CallContext& ctx);

    // DeclareExchange(name, type, onlyCheckIfExists, durable, autodelete, propsJson)
    void declareExchangeImpl(CallContext& ctx);

    // DeleteExchange(name, ifUnused)
    void deleteExchangeImpl(CallContext& ctx);

    // DeclareQueue(name, onlyCheckIfExists, durable, exclusive, autodelete,
    //              maxPriority, propsJson)
    void declareQueueImpl(CallContext& ctx);

    // DeleteQueue(name, ifUnused, ifEmpty)
    void deleteQueueImpl(CallContext& ctx);

    // BindQueue(queue, exchange, routingKey, propsJson)
    void bindQueueImpl(CallContext& ctx);

    // UnbindQueue(queue, exchange, routingKey)
    void unbindQueueImpl(CallContext& ctx);

    // BasicPublish(exchange, routingKey, message, skip, persistent, propsJson)
    // Тело сообщения передаётся бинарно-безопасно (по размеру, не strlen)
    void basicPublishImpl(CallContext& ctx);

    // BasicConsume(queue, consumerId, noConfirm, exclusive, selectSize, propsJson)
    // Возвращает consumerTag.
    // Поддерживает два режима:
    //   - Legacy polling: сообщения в m_messageQueue, читать через BasicConsumeMessage
    //   - Event-driven: сообщения через ExternalEvent (если включено)
    void basicConsumeImpl(CallContext& ctx);

    // BasicConsumeMessage(skip, &outData, &outMessageTag, timeout)
    // Возвращает тело сообщения и deliveryTag.
    // Для обратной совместимости — polling режим.
    void basicConsumeMessageImpl(CallContext& ctx);

    // Включить событийную модель: сообщения доставляются через ExternalEvent.
    // После вызова BasicConsume сообщения приходят в ОбработкаВнешнегоСобытия.
    void enableExternalEvent(bool enable) { m_useExternalEvent = enable; }
    bool isExternalEventEnabled() const { return m_useExternalEvent; }

    // BasicCancel()
    void basicCancelImpl(CallContext& ctx);

    // Reconnect()
    // Поднимает соединение после обрыва и восстанавливает потребителя
    // с прежними параметрами. Тег потребителя брокер присвоит новый.
    void reconnectImpl(CallContext& ctx);

    // BasicAck(deliveryTag)
    void basicAckImpl(CallContext& ctx);

    // BasicReject(deliveryTag, requeue)
    // Параметр requeue: в типовых решениях его обычно нет.
    void basicRejectImpl(CallContext& ctx);

    // SetPublishMode(режим): "confirms" (по умолчанию) или "transactions".
    // Confirms — расширение RabbitMQ: быстрее вдвое. Transactions — класс tx
    // из спецификации AMQP 0.9.1, нужен на брокерах без поддержки confirms.
    void setPublishModeImpl(CallContext& ctx);

    // SleepNative(milliseconds)
    void sleepNativeImpl(CallContext& ctx);

    // EnableExternalEvent(enable) — включить/выключить событийную модель
    void enableExternalEventImpl(CallContext& ctx);

    // --- Properties ---
    void getRoutingKeyImpl(CallContext& ctx);
    void getHeadersImpl(CallContext& ctx);
    void setPriorityImpl(CallContext& ctx);
    void getPriorityImpl(CallContext& ctx);
    void setMsgPropImpl(long propNum, CallContext& ctx);
    void getMsgPropImpl(long propNum, CallContext& ctx);
    // SslCaFile / SslVerifyPeer / SslVerifyHostname — политика проверки
    // сертификата брокера. Читаются в момент Connect.
    void setTlsPropImpl(long propNum, CallContext& ctx);
    void getTlsPropImpl(long propNum, CallContext& ctx);

private:
    // Параметры активного потребителя: нужны, чтобы поднять его после обрыва.
    struct ConsumeParams {
        std::string queue;
        std::string consumerId;
        bool noConfirm = false;
        bool exclusive = false;
        uint16_t prefetch = 0;
        AMQP::Table args;
        bool valid = false;
    };

    // Запустить потребителя по сохранённым параметрам (BasicConsume и Reconnect).
    void startConsumer(const ConsumeParams& params);

    void checkConnection();
    // Ack/Reject возможны только пока жив потребитель: подтверждать нужно
    // на канале, доставившем сообщение.
    void checkConsumer(const char* method);
    AMQP::Table headersFromJson(const std::string& json, bool forConsume = false);
    std::string lastMessageHeaders();
    void clear();

    std::unique_ptr<Client> m_client;
    std::unique_ptr<Consumer> m_consumer;
    ConsumeParams m_consumeParams;
    TlsOptions m_tls;

    // Очередь сообщений для polling-совместимости (BasicConsumeMessage)
    std::queue<Message> m_messageQueue;
    std::mutex m_queueMutex;
    std::condition_variable m_cvDataArrived;
    std::string m_consumerError;
    bool m_useExternalEvent = false;
    // Сколько событий платформа отказалась принять (переполнение её буфера).
    size_t m_eventsDropped = 0;

    // Свойства для следующей публикации (устанавливаются через setMsgProp)
    MessageProperties m_outgoingProps;
    int m_outgoingPriority = 0;
    // Последнее полученное сообщение
    Message m_lastMessage;
};

} // namespace AddIn1S
} // namespace BlackRabbitMQ
