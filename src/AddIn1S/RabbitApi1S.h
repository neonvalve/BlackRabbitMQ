#pragma once

#include "Component.h"
#include "CallContext.h"

#include <memory>
#include <queue>
#include <mutex>
#include <condition_variable>

namespace BlackRabbitMQ {

class Client;
class Consumer;
struct Message;

namespace AddIn1S {

// Совместимый с 1С API над Client и Consumer.
// Исключения не прокидываются в 1С — перехватываются в wrapCall.
//
// Отличия от upstream:
//   - BasicReject(tag, requeue) — добавлен параметр requeue
//   - Publish: бинарно-безопасное тело (.data()+.size(), не strlen)
//   - Внутри: std::unique_ptr, без ручного new/delete
class RabbitApi1S : public Component {
public:
    RabbitApi1S();
    ~RabbitApi1S() override;

    // --- 1C Methods (сигнатуры совместимы с upstream) ---

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

    // BasicAck(deliveryTag)
    void basicAckImpl(CallContext& ctx);

    // BasicReject(deliveryTag, requeue)
    // Параметр requeue — НОВЫЙ (отсутствовал в upstream).
    void basicRejectImpl(CallContext& ctx);

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

private:
    void checkConnection();
    AMQP::Table headersFromJson(const std::string& json, bool forConsume = false);
    std::string lastMessageHeaders();
    void clear();

    std::unique_ptr<Client> m_client;
    std::unique_ptr<Consumer> m_consumer;

    // Очередь сообщений для polling-совместимости (BasicConsumeMessage)
    std::queue<Message> m_messageQueue;
    std::mutex m_queueMutex;
    std::condition_variable m_cvDataArrived;
    std::string m_consumerError;
    bool m_useExternalEvent = false;

    // Последнее полученное сообщение
    Message m_lastMessage;
};

} // namespace AddIn1S
} // namespace BlackRabbitMQ
