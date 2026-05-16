#include "RabbitApi1S.h"
#include "Client.h"
#include "Consumer.h"
#include "Message.h"
#include "Channel.h"

#include <nlohmann/json.hpp>

#include <chrono>
#include <thread>
#include <stdexcept>

namespace BlackRabbitMQ {
namespace AddIn1S {

using json = nlohmann::json;

RabbitApi1S::RabbitApi1S()
    : Component("RabbitMQClient")
{
}

RabbitApi1S::~RabbitApi1S() = default;

// --- Проверка соединения ---

void RabbitApi1S::checkConnection() {
    if (!m_client || !m_client->isConnected()) {
        throw std::runtime_error("Connection is not established! Use Connect() first");
    }
}

// --- Connect ---

void RabbitApi1S::connectImpl(CallContext& ctx) {
    std::string host = ctx.stringParamUtf8();
    uint16_t port = static_cast<uint16_t>(ctx.intParam());
    std::string user = ctx.stringParamUtf8();
    std::string pwd = ctx.stringParamUtf8();
    std::string vhost = ctx.stringParamUtf8();
    ctx.skipParam(); // skip
    bool ssl = ctx.boolParam();
    int timeout = ctx.intParam();

    if (host.empty()) {
        throw std::runtime_error("Empty hostname not allowed");
    }

    // Разорвать предыдущее соединение
    clear();

    // Создать новое
    m_client.reset(new Client());
    m_client->connect(host, port, user, pwd, vhost, ssl, timeout);
}

// --- Exchange ---

void RabbitApi1S::declareExchangeImpl(CallContext& ctx) {
    checkConnection();

    std::string name = ctx.stringParamUtf8();
    std::string type = ctx.stringParamUtf8();
    bool passive = ctx.boolParam();
    bool durable = ctx.boolParam();
    bool autoDelete = ctx.boolParam();
    std::string propsJson = ctx.stringParamUtf8();

    AMQP::ExchangeType exType = AMQP::ExchangeType::topic;
    if (type == "topic")       exType = AMQP::ExchangeType::topic;
    else if (type == "fanout")  exType = AMQP::ExchangeType::fanout;
    else if (type == "direct")  exType = AMQP::ExchangeType::direct;
    else throw std::runtime_error("Exchange type not supported: " + type);

    m_client->declareExchange(name, exType, passive, durable, autoDelete, headersFromJson(propsJson));
}

void RabbitApi1S::deleteExchangeImpl(CallContext& ctx) {
    checkConnection();
    std::string name = ctx.stringParamUtf8();
    bool ifUnused = ctx.boolParam();
    m_client->deleteExchange(name, ifUnused);
}

// --- Queue ---

void RabbitApi1S::declareQueueImpl(CallContext& ctx) {
    checkConnection();

    std::string name = ctx.stringParamUtf8();
    bool passive = ctx.boolParam();
    bool durable = ctx.boolParam();
    bool exclusive = ctx.boolParam();
    bool autoDelete = ctx.boolParam();
    auto maxPriority = static_cast<uint16_t>(ctx.intParam());
    std::string propsJson = ctx.stringParamUtf8();

    AMQP::Table args = headersFromJson(propsJson);
    if (maxPriority != 0) {
        args.set("x-max-priority", maxPriority);
    }

    m_client->declareQueue(name, passive, durable, exclusive, autoDelete, args);
    ctx.setStringOrEmptyResult(m_converter.from_bytes(name));
}

void RabbitApi1S::deleteQueueImpl(CallContext& ctx) {
    checkConnection();
    std::string name = ctx.stringParamUtf8();
    bool ifUnused = ctx.boolParam();
    bool ifEmpty = ctx.boolParam();
    m_client->deleteQueue(name, ifUnused, ifEmpty);
}

// --- Binding ---

void RabbitApi1S::bindQueueImpl(CallContext& ctx) {
    checkConnection();
    std::string queue = ctx.stringParamUtf8();
    std::string exchange = ctx.stringParamUtf8();
    std::string routingKey = ctx.stringParamUtf8();
    std::string propsJson = ctx.stringParamUtf8();

    m_client->bindQueue(exchange, queue, routingKey, headersFromJson(propsJson));
}

void RabbitApi1S::unbindQueueImpl(CallContext& ctx) {
    checkConnection();
    std::string queue = ctx.stringParamUtf8();
    std::string exchange = ctx.stringParamUtf8();
    std::string routingKey = ctx.stringParamUtf8();

    m_client->unbindQueue(exchange, queue, routingKey);
}

// --- Publish ---

void RabbitApi1S::basicPublishImpl(CallContext& ctx) {
    checkConnection();

    std::string exchange = ctx.stringParamUtf8();
    std::string routingKey = ctx.stringParamUtf8();
    std::string body = ctx.stringParamUtf8();
    ctx.skipParam();
    bool persistent = ctx.boolParam();
    std::string propsJson = ctx.stringParamUtf8();

    // Бинарно-безопасный Envelope: .data() + .size(), не strlen
    AMQP::Envelope envelope(body.data(), body.size());

    if (persistent) {
        envelope.setDeliveryMode(2);
    }

    // Применить свойства, установленные через setMsgProp
    if (!m_outgoingProps.correlationId.empty())   envelope.setCorrelationID(m_outgoingProps.correlationId);
    if (!m_outgoingProps.messageId.empty())       envelope.setMessageID(m_outgoingProps.messageId);
    if (!m_outgoingProps.typeName.empty())        envelope.setTypeName(m_outgoingProps.typeName);
    if (!m_outgoingProps.appId.empty())           envelope.setAppID(m_outgoingProps.appId);
    if (!m_outgoingProps.contentEncoding.empty()) envelope.setContentEncoding(m_outgoingProps.contentEncoding);
    if (!m_outgoingProps.contentType.empty())     envelope.setContentType(m_outgoingProps.contentType);
    if (!m_outgoingProps.userId.empty())          envelope.setUserID(m_outgoingProps.userId);
    if (!m_outgoingProps.clusterId.empty())       envelope.setClusterID(m_outgoingProps.clusterId);
    if (!m_outgoingProps.expiration.empty())      envelope.setExpiration(m_outgoingProps.expiration);
    if (!m_outgoingProps.replyTo.empty())         envelope.setReplyTo(m_outgoingProps.replyTo);
    if (m_lastMessage.priority != 0)              envelope.setPriority(m_lastMessage.priority);

    AMQP::Table headers = headersFromJson(propsJson);
    if (headers.keys().size() > 0) {
        envelope.setHeaders(headers);
    }

    m_client->publish(exchange, routingKey, body);
    m_outgoingProps = MessageProperties{};
}

// --- Consume (legacy polling) ---

void RabbitApi1S::basicConsumeImpl(CallContext& ctx) {
    checkConnection();

    std::string queue = ctx.stringParamUtf8();
    std::string consumerId = ctx.stringParamUtf8(true);
    bool noConfirm = ctx.boolParam();
    bool exclusive = ctx.boolParam();
    int selectSize = ctx.intParam();
    std::string propsJson = ctx.stringParamUtf8();

    // Остановить предыдущий consumer
    clear();

    AMQP::Table args = headersFromJson(propsJson, true);

    m_consumer.reset(new Consumer());

    // Захватываем consumerTag из callback'а
    std::string capturedTag;

    // Создать канал для consumer
    auto channel = m_client->createChannel();

    m_consumer->start(
        std::move(channel),
        queue,
        consumerId,
        exclusive,
        static_cast<uint16_t>(selectSize),
        args,
        // onMessage
        [this](const Message& msg) {
            if (m_useExternalEvent && m_addin) {
                // Событийная модель: отправить ExternalEvent в 1С
                json eventData;
                eventData["body"] = msg.body;
                eventData["deliveryTag"] = msg.deliveryTag;
                eventData["routingKey"] = msg.routingKey;
                eventData["priority"] = msg.priority;
                eventData["redelivered"] = msg.redelivered;
                eventData["correlationId"] = msg.props.correlationId;
                eventData["messageId"] = msg.props.messageId;
                eventData["contentType"] = msg.props.contentType;

                std::u16string u16data = m_converter.from_bytes(eventData.dump());
                m_addin->ExternalEvent(
                    const_cast<WCHAR_T*>(reinterpret_cast<const WCHAR_T*>(u"BlackRabbitMQ")),
                    const_cast<WCHAR_T*>(reinterpret_cast<const WCHAR_T*>(u"MessageReceived")),
                    const_cast<WCHAR_T*>(reinterpret_cast<const WCHAR_T*>(u16data.c_str()))
                );
            }
            // Всегда сохраняем в очередь для обратной совместимости
            {
                std::lock_guard<std::mutex> lock(m_queueMutex);
                m_messageQueue.push(msg);
                m_cvDataArrived.notify_all();
            }
        },
        // onCancelled
        [this](const std::string& consumerTag) {
            std::lock_guard<std::mutex> lock(m_queueMutex);
            m_consumerError = "Consumer cancelled: " + consumerTag;
            if (m_useExternalEvent && m_addin) {
                // Уведомить 1С об отмене потребителя
                std::u16string u16tag = m_converter.from_bytes(consumerTag);
                m_addin->ExternalEvent(
                    const_cast<WCHAR_T*>(reinterpret_cast<const WCHAR_T*>(u"BlackRabbitMQ")),
                    const_cast<WCHAR_T*>(reinterpret_cast<const WCHAR_T*>(u"ConsumerCancelled")),
                    const_cast<WCHAR_T*>(reinterpret_cast<const WCHAR_T*>(u16tag.c_str()))
                );
            }
        }
    );

    ctx.setStringOrEmptyResult(m_converter.from_bytes(m_consumer->tag()));
}

void RabbitApi1S::basicConsumeMessageImpl(CallContext& ctx) {
    checkConnection();

    if (!m_consumer || !m_consumer->isActive()) {
        throw std::runtime_error("No active consumers. Use BasicConsume() first");
    }

    ctx.skipParam();
    tVariant* outData = ctx.skipParam();
    tVariant* outMessageTag = ctx.skipParam();
    int timeout = ctx.intParam();

    ctx.setEmptyResult(outData);
    ctx.setIntResult(0, outMessageTag);

    {
        std::unique_lock<std::mutex> lock(m_queueMutex);

        if (m_messageQueue.empty()) {
            if (!m_consumerError.empty()) {
                std::string err = std::move(m_consumerError);
                throw std::runtime_error(err);
            }
            if (!m_cvDataArrived.wait_for(lock, std::chrono::milliseconds(timeout),
                                          [this] { return !m_messageQueue.empty(); })) {
                ctx.setBoolResult(false);
                return;
            }
        }

        if (m_messageQueue.empty()) {
            throw std::runtime_error("Empty consume message");
        }

        m_lastMessage = m_messageQueue.front();
        m_messageQueue.pop();
    }

    ctx.setStringResult(m_converter.from_bytes(m_lastMessage.body), outData);
    ctx.setLongResult(static_cast<int64_t>(m_lastMessage.deliveryTag), outMessageTag);
    ctx.setBoolResult(true);
}

// --- Ack / Reject ---

void RabbitApi1S::basicAckImpl(CallContext& ctx) {
    checkConnection();
    uint64_t tag = static_cast<uint64_t>(ctx.longParam());
    if (tag == 0) {
        throw std::runtime_error("Message tag cannot be empty!");
    }
    m_client->ack(tag);
}

void RabbitApi1S::basicRejectImpl(CallContext& ctx) {
    checkConnection();
    uint64_t tag = static_cast<uint64_t>(ctx.longParam());
    if (tag == 0) {
        throw std::runtime_error("Message tag cannot be empty!");
    }
    // Второй параметр — requeue (НОВЫЙ, отсутствовал в upstream)
    bool requeue = ctx.boolParam();
    m_client->reject(tag, requeue);
}

// --- Cancel ---

void RabbitApi1S::basicCancelImpl(CallContext& /*ctx*/) {
    checkConnection();
    clear();
}

// --- ExternalEvent ---

void RabbitApi1S::enableExternalEventImpl(CallContext& ctx) {
    bool enable = ctx.boolParam();
    enableExternalEvent(enable);
}

// --- Sleep ---

void RabbitApi1S::sleepNativeImpl(CallContext& ctx) {
    uint64_t amount = static_cast<uint64_t>(ctx.longParam());
    std::this_thread::sleep_for(std::chrono::milliseconds(amount));
}

// --- Properties ---

void RabbitApi1S::getRoutingKeyImpl(CallContext& ctx) {
    ctx.setStringResult(m_converter.from_bytes(m_lastMessage.routingKey));
}

void RabbitApi1S::getHeadersImpl(CallContext& ctx) {
    ctx.setStringResult(m_converter.from_bytes(lastMessageHeaders()));
}

void RabbitApi1S::setPriorityImpl(CallContext& ctx) {
    // priority сохраняется в m_lastMessage для использования в publish
    m_lastMessage.priority = ctx.intParam();
}

void RabbitApi1S::getPriorityImpl(CallContext& ctx) {
    ctx.setIntResult(m_lastMessage.priority);
}

void RabbitApi1S::setMsgPropImpl(long propNum, CallContext& ctx) {
    std::string value = ctx.stringParamUtf8();
    switch (propNum) {
        case 1:  m_outgoingProps.correlationId = value;    break;
        case 2:  m_outgoingProps.typeName = value;         break;
        case 3:  m_outgoingProps.messageId = value;        break;
        case 4:  m_outgoingProps.appId = value;            break;
        case 5:  m_outgoingProps.contentEncoding = value;  break;
        case 6:  m_outgoingProps.contentType = value;      break;
        case 7:  m_outgoingProps.userId = value;           break;
        case 8:  m_outgoingProps.clusterId = value;        break;
        case 9:  m_outgoingProps.expiration = value;       break;
        case 10: m_outgoingProps.replyTo = value;          break;
    }
}

void RabbitApi1S::getMsgPropImpl(long propNum, CallContext& ctx) {
    const auto& props = m_lastMessage.props;
    std::string value;
    switch (propNum) {
        case 1:  value = props.correlationId;    break;
        case 2:  value = props.typeName;         break;
        case 3:  value = props.messageId;        break;
        case 4:  value = props.appId;            break;
        case 5:  value = props.contentEncoding;  break;
        case 6:  value = props.contentType;      break;
        case 7:  value = props.userId;           break;
        case 8:  value = props.clusterId;        break;
        case 9:  value = props.expiration;       break;
        case 10: value = props.replyTo;          break;
    }
    ctx.setStringOrEmptyResult(m_converter.from_bytes(value));
}

// --- Helpers ---

void RabbitApi1S::clear() {
    m_consumer.reset(nullptr);
    std::lock_guard<std::mutex> lock(m_queueMutex);
    m_consumerError.clear();
    std::queue<Message> empty;
    m_messageQueue.swap(empty);
    m_cvDataArrived.notify_all();
}

AMQP::Table RabbitApi1S::headersFromJson(const std::string& propsJson, bool /*forConsume*/) {
    AMQP::Table headers;
    if (propsJson.empty()) return headers;

    auto object = json::parse(propsJson);
    for (auto& it : object.items()) {
        auto& value = it.value();
        std::string name = it.key();
        if (value.is_boolean())
            headers.set(name, value.get<bool>());
        else if (value.is_number())
            headers.set(name, value.get<int64_t>());
        else if (value.is_string())
            headers.set(name, value.get<std::string>());
        else
            throw std::runtime_error("Unsupported json type for property " + name);
    }
    return headers;
}

std::string RabbitApi1S::lastMessageHeaders() {
    json hdr = json::object();
    AMQP::Table& tbl = m_lastMessage.headers;
    for (const std::string& key : tbl.keys()) {
        const AMQP::Field& field = tbl.get(key);
        if (field.isInteger())      hdr[key] = static_cast<int64_t>(field);
        else if (field.isDecimal()) hdr[key] = static_cast<double>(field);
        else if (field.isString())  hdr[key] = static_cast<const std::string&>(field);
        else if (field.isBoolean()) hdr[key] = static_cast<bool>(static_cast<int64_t>(field));
    }
    return hdr.dump();
}

} // namespace AddIn1S
} // namespace BlackRabbitMQ
