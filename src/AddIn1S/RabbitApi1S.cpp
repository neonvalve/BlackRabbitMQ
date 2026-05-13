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

RabbitApi1S::~RabbitApi1S() {
    clear();
}

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

    // Установить свойства сообщения (из setMsgProp)
    // Они хранятся отдельно, применяются здесь.

    AMQP::Table headers = headersFromJson(propsJson);
    if (headers.keys().size() > 0) {
        envelope.setHeaders(headers);
    }

    m_client->publish(exchange, routingKey, body);
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
            std::lock_guard<std::mutex> lock(m_queueMutex);
            m_messageQueue.push(msg);
            m_cvDataArrived.notify_all();
        },
        // onCancelled
        [this](const std::string& consumerTag) {
            std::lock_guard<std::mutex> lock(m_queueMutex);
            m_consumerError = "Consumer cancelled: " + consumerTag;
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
    // TODO: хранить свойства для следующего publish
    // propNum: 1=CorrelationId, 2=Type, 3=MessageId, ...
}

void RabbitApi1S::getMsgPropImpl(long propNum, CallContext& ctx) {
    // TODO: вернуть свойство последнего сообщения по номеру
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
    // Конвертировать AMQP::Table в JSON
    AMQP::Table& tbl = m_lastMessage.headers;
    json hdr = json::object();
    for (const std::string& key : tbl.keys()) {
        const AMQP::Field& field = tbl.get(key);
        if (field.isBoolean()) hdr[key] = static_cast<bool>(field.get(0));
        else if (field.isInteger()) hdr[key] = static_cast<int64_t>(field);
        else if (field.isDecimal()) hdr[key] = static_cast<double>(field);
        else if (field.isString()) hdr[key] = static_cast<const std::string&>(field);
    }
    return hdr.dump();
}

} // namespace AddIn1S
} // namespace BlackRabbitMQ
