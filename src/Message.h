#pragma once

#include <amqpcpp.h>
#include <string>
#include <cstdint>

namespace BlackRabbitMQ {

// Типобезопасные свойства сообщения.
// Замена map<int,string> из upstream (где ключи — магические числа 1-10).
struct MessageProperties {
    std::string correlationId;
    std::string typeName;
    std::string messageId;
    std::string appId;
    std::string contentEncoding;
    std::string contentType;
    std::string userId;
    std::string clusterId;
    std::string expiration;
    std::string replyTo;
};

// Value-объект сообщения. Бинарно-безопасный: body хранится как std::string
// с явным размером, без опоры на strlen.
struct Message {
    std::string body;
    uint64_t deliveryTag = 0;
    int priority = 0;
    std::string routingKey;
    MessageProperties props;
    AMQP::Table headers;
    bool redelivered = false;

    // Построить из AMQP::Message (полученного в onMessage callback).
    static Message from(const AMQP::Message& msg, uint64_t deliveryTag, bool redelivered) {
        Message m;
        m.body.assign(msg.body(), msg.bodySize());
        m.deliveryTag = deliveryTag;
        m.priority = msg.priority();
        m.routingKey = msg.routingkey();
        m.props.correlationId = msg.correlationID();
        m.props.typeName = msg.typeName();
        m.props.messageId = msg.messageID();
        m.props.appId = msg.appID();
        m.props.contentEncoding = msg.contentEncoding();
        m.props.contentType = msg.contentType();
        m.props.userId = msg.userID();
        m.props.clusterId = msg.clusterID();
        m.props.expiration = msg.expiration();
        m.props.replyTo = msg.replyTo();
        m.headers = msg.headers();
        m.redelivered = redelivered;
        return m;
    }
};

} // namespace BlackRabbitMQ
