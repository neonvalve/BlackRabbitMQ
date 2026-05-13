#include <gtest/gtest.h>
#include "Message.h"

using namespace BlackRabbitMQ;

// Message — value-объект, бинарно-безопасный.
// Тесты не требуют AMQP-CPP или RabbitMQ.

TEST(Message, DefaultConstructedIsEmpty) {
    Message msg;
    EXPECT_TRUE(msg.body.empty());
    EXPECT_EQ(msg.deliveryTag, 0);
    EXPECT_EQ(msg.priority, 0);
    EXPECT_TRUE(msg.routingKey.empty());
    EXPECT_FALSE(msg.redelivered);
}

TEST(Message, BodyStoresBinaryData) {
    Message msg;
    // Бинарные данные с null-байтами
    std::string binary("\x00\x01\x02\xFF\xFE", 5);
    msg.body = binary;

    EXPECT_EQ(msg.body.size(), 5);
    EXPECT_EQ(static_cast<unsigned char>(msg.body[0]), 0x00);
    EXPECT_EQ(static_cast<unsigned char>(msg.body[3]), 0xFF);
}

TEST(Message, BodyIsNotTruncatedByStrlen) {
    // В upstream была проблема: strlen обрезал тело на первом \0
    Message msg;
    msg.body = std::string("hello\0world", 11);

    EXPECT_EQ(msg.body.size(), 11);
    EXPECT_EQ(msg.body[5], '\0');
    EXPECT_EQ(msg.body[6], 'w');
}

TEST(Message, DeliveryTagStoredCorrectly) {
    Message msg;
    msg.deliveryTag = 12345678901234ULL;
    EXPECT_EQ(msg.deliveryTag, 12345678901234ULL);
}

TEST(Message, PropertiesAreTypedStruct) {
    Message msg;
    msg.props.correlationId = "corr-1";
    msg.props.messageId = "msg-1";
    msg.props.contentType = "application/json";

    EXPECT_EQ(msg.props.correlationId, "corr-1");
    EXPECT_EQ(msg.props.messageId, "msg-1");
    EXPECT_EQ(msg.props.contentType, "application/json");
    EXPECT_TRUE(msg.props.appId.empty()); // не установлено
}

TEST(Message, MessagePropertiesIsNotMap) {
    // Проверяем, что MessageProperties — это типобезопасная структура,
    // а не map<int,string> с магическими числами.
    MessageProperties props;
    props.correlationId = "test";
    props.typeName = "type";
    props.messageId = "id";
    props.appId = "app";
    props.contentEncoding = "utf-8";
    props.contentType = "text/plain";
    props.userId = "user";
    props.clusterId = "cluster";
    props.expiration = "10000";
    props.replyTo = "reply-queue";

    // Все поля доступны по имени, а не по индексу.
    EXPECT_EQ(props.correlationId, "test");
    EXPECT_EQ(props.typeName, "type");
    EXPECT_EQ(props.messageId, "id");
    EXPECT_EQ(props.appId, "app");
    EXPECT_EQ(props.contentEncoding, "utf-8");
    EXPECT_EQ(props.contentType, "text/plain");
    EXPECT_EQ(props.userId, "user");
    EXPECT_EQ(props.clusterId, "cluster");
    EXPECT_EQ(props.expiration, "10000");
    EXPECT_EQ(props.replyTo, "reply-queue");
}

TEST(Message, RedeliveredFlagDefaultFalse) {
    Message msg;
    EXPECT_FALSE(msg.redelivered);

    msg.redelivered = true;
    EXPECT_TRUE(msg.redelivered);
}

TEST(Message, RoutingKey) {
    Message msg;
    msg.routingKey = "orders.created";
    EXPECT_EQ(msg.routingKey, "orders.created");
}
