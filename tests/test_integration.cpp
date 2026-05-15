#include <gtest/gtest.h>
#include "Client.h"
#include "Consumer.h"
#include "Message.h"
#include "Channel.h"

#include <cstdlib>
#include <string>
#include <thread>
#include <chrono>

using namespace BlackRabbitMQ;

// Интеграционные тесты с реальным RabbitMQ.
// Пропускаются, если нет RABBITMQ_HOST (по умолчанию localhost:5672).
// Запуск:
//   RABBITMQ_HOST=localhost cmake --build build && ctest

namespace {

std::string rabbitHost() {
    const char* env = std::getenv("RABBITMQ_HOST");
    return env ? env : "localhost";
}

int rabbitPort() {
    const char* env = std::getenv("RABBITMQ_PORT");
    return env ? std::atoi(env) : 5672;
}

bool rabbitAvailable() {
    try {
        Client client;
        client.connect(rabbitHost(), rabbitPort(), "guest", "guest", "/", false, 2);
        client.disconnect();
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace

class IntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!rabbitAvailable()) {
            GTEST_SKIP() << "RabbitMQ not available at "
                         << rabbitHost() << ":" << rabbitPort();
        }
    }

    Client client;
};

TEST_F(IntegrationTest, ConnectAndDisconnect) {
    client.connect(rabbitHost(), rabbitPort(), "guest", "guest", "/", false, 5);
    EXPECT_TRUE(client.isConnected());
    client.disconnect();
    EXPECT_FALSE(client.isConnected());
}

TEST_F(IntegrationTest, DeclareExchangeAndQueue) {
    client.connect(rabbitHost(), rabbitPort(), "guest", "guest", "/", false, 5);

    client.declareExchange("test_exchange", AMQP::ExchangeType::topic, false, false, true);

    client.declareQueue("test_queue", false, false, true, true);

    client.bindQueue("test_exchange", "test_queue", "test.key");

    // Очистка
    client.deleteQueue("test_queue");
    client.deleteExchange("test_exchange");
    client.disconnect();
}

TEST_F(IntegrationTest, PublishAndConsume) {
    client.connect(rabbitHost(), rabbitPort(), "guest", "guest", "/", false, 5);

    client.declareExchange("test_pub_ex", AMQP::ExchangeType::direct, false, false, true);
    client.declareQueue("test_pub_q", false, false, true, true);
    client.bindQueue("test_pub_ex", "test_pub_q", "hello");

    // Публикация — бинарно-безопасное тело
    std::string body = "Hello \x00 Rabbit \x00 World";
    AMQP::Envelope env(body.data(), body.size());
    env.setDeliveryMode(1);
    client.publish("test_pub_ex", "hello", body);

    // Consume через Consumer
    bool received = false;
    std::string receivedBody;

    auto channel = client.createChannel();
    Consumer consumer;
    consumer.start(
        std::move(channel),
        "test_pub_q",
        "test-consumer",
        true,
        1,
        {},
        [&](const Message& msg) {
            received = true;
            receivedBody = msg.body;
        },
        nullptr
    );

    // Ждать сообщение (макс 3 сек)
    for (int i = 0; i < 30 && !received; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_TRUE(received);
    EXPECT_EQ(receivedBody, body);

    // Очистка
    consumer.cancel();
    client.deleteQueue("test_pub_q");
    client.deleteExchange("test_pub_ex");
    client.disconnect();
}

TEST_F(IntegrationTest, RejectWithRequeue) {
    client.connect(rabbitHost(), rabbitPort(), "guest", "guest", "/", false, 5);

    client.declareExchange("test_reject_ex", AMQP::ExchangeType::direct, false, false, true);
    client.declareQueue("test_reject_q", false, false, true, true);
    client.bindQueue("test_reject_ex", "test_reject_q", "reject");

    // Публикуем сообщение
    client.publish("test_reject_ex", "reject", "reject-me");

    // Читаем и reject с requeue = true
    int receivedCount = 0;
    auto channel = client.createChannel();
    Consumer consumer;
    consumer.start(
        std::move(channel),
        "test_reject_q",
        "test-reject-consumer",
        false,
        1,
        {},
        [&consumer, &receivedCount](const Message& msg) {
            receivedCount++;
            if (receivedCount == 1) {
                consumer.reject(msg.deliveryTag, true);
            } else {
                consumer.ack(msg.deliveryTag);
            }
        },
        nullptr
    );

    // Ждать оба получения (макс 5 сек)
    for (int i = 0; i < 50 && receivedCount < 2; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // Сообщение должно прийти дважды: оригинал + после requeue
    EXPECT_GE(receivedCount, 2) << "Message should be redelivered after reject(requeue=true)";

    consumer.cancel();
    client.deleteQueue("test_reject_q");
    client.deleteExchange("test_reject_ex");
    client.disconnect();
}

TEST_F(IntegrationTest, BinaryMessageRoundTrip) {
    client.connect(rabbitHost(), rabbitPort(), "guest", "guest", "/", false, 5);

    client.declareExchange("test_bin_ex", AMQP::ExchangeType::direct, false, false, true);
    client.declareQueue("test_bin_q", false, false, true, true);
    client.bindQueue("test_bin_ex", "test_bin_q", "bin");

    // Бинарные данные с null-байтами
    std::string binaryBody("\x00\x01\x02\xFF\xFE\x00\x00\xAB", 8);

    client.publish("test_bin_ex", "bin", binaryBody);

    bool received = false;
    std::string receivedBody;

    auto channel = client.createChannel();
    Consumer consumer;
    consumer.start(
        std::move(channel),
        "test_bin_q",
        "test-bin-consumer",
        true,
        1,
        {},
        [&](const Message& msg) {
            received = true;
            receivedBody = msg.body;
        },
        nullptr
    );

    for (int i = 0; i < 30 && !received; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_TRUE(received);
    EXPECT_EQ(receivedBody.size(), 8);
    EXPECT_EQ(static_cast<unsigned char>(receivedBody[0]), 0x00);
    EXPECT_EQ(static_cast<unsigned char>(receivedBody[3]), 0xFF);
    EXPECT_EQ(static_cast<unsigned char>(receivedBody[7]), 0xAB);

    consumer.cancel();
    client.deleteQueue("test_bin_q");
    client.deleteExchange("test_bin_ex");
    client.disconnect();
}

TEST_F(IntegrationTest, Reconnect) {
    client.connect(rabbitHost(), rabbitPort(), "guest", "guest", "/", false, 5);
    EXPECT_TRUE(client.isConnected());

    // Переподключиться
    bool ok = client.reconnect();
    EXPECT_TRUE(ok);
    EXPECT_TRUE(client.isConnected());

    client.disconnect();
    EXPECT_FALSE(client.isConnected());
}

// Полный пользовательский сценарий: Connect → Queue → Publish → Consume → Ack
TEST_F(IntegrationTest, FullEndToEndDemo) {
    client.connect(rabbitHost(), rabbitPort(), "guest", "guest", "/", false, 5);

    client.declareExchange("demo_ex", AMQP::ExchangeType::direct, false, false, true);
    client.declareQueue("demo_queue", false, false, true, true);
    client.bindQueue("demo_ex", "demo_queue", "demo.key");

    // Публикация с UTF-8 текстом
    std::string body = u8"Привет из BlackRabbitMQ! Сообщение №1";
    client.publish("demo_ex", "demo.key", body);

    // Чтение
    bool received = false;
    std::string receivedBody;
    uint64_t receivedTag = 0;

    auto channel = client.createChannel();
    Consumer consumer;
    consumer.start(
        std::move(channel), "demo_queue", "demo-consumer",
        false, 1, {},
        [&](const Message& msg) {
            received = true;
            receivedBody = msg.body;
            receivedTag = msg.deliveryTag;
        },
        nullptr);

    for (int i = 0; i < 30 && !received; i++)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    EXPECT_TRUE(received);
    EXPECT_EQ(receivedBody, body);
    EXPECT_GT(receivedTag, 0ULL);

    consumer.ack(receivedTag);
    consumer.cancel();
    client.deleteQueue("demo_queue");
    client.deleteExchange("demo_ex");
    client.disconnect();
}
