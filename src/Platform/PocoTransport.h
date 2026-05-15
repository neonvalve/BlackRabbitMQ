#pragma once

#include "ITransport.h"

#include <amqpcpp.h>
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <cstdint>

namespace Poco::Net {
    class StreamSocket;
}

namespace BlackRabbitMQ {

// Windows транспорт: POCO + AMQP::Connection.
// Реализует AMQP::ConnectionHandler, управляет своим потоком.
class PocoTransport : public ITransport, public AMQP::ConnectionHandler {
public:
    static constexpr size_t BUFFER_SIZE = 8 * 1024 * 1024;
    static constexpr size_t TEMP_BUFFER_SIZE = 1 * 1024 * 1024;

    PocoTransport();
    ~PocoTransport() override;

    // ITransport
    void connect(const AMQP::Address& address, int timeoutSec) override;
    void disconnect() override;
    std::unique_ptr<AMQP::Channel> createChannel() override;
    bool isConnected() const noexcept override { return !m_closed.load(std::memory_order_acquire); }
    const std::string& error() const noexcept override { return m_error; }

    // AMQP::ConnectionHandler
    void onData(AMQP::Connection* connection, const char* data, size_t size) override;
    void onReady(AMQP::Connection* connection) override;
    void onError(AMQP::Connection* connection, const char* message) override;
    void onClosed(AMQP::Connection* connection) override;
    uint16_t onNegotiate(AMQP::Connection* connection, uint16_t interval) override;

private:
    void startLoop(const std::string& host, uint16_t port, bool ssl);
    void stopLoop();
    static void loopThread(PocoTransport* self);
    void loopIteration();
    void sendDataFromBuffer();

    struct Buffer {
        std::vector<char> data;
        size_t used = 0;
        explicit Buffer(size_t sz) : data(sz, 0) {}
        size_t write(const char* src, size_t sz);
        void drain() { used = 0; }
        size_t available() const { return used; }
        const char* ptr() const { return data.data(); }
        void shift(size_t count);
    };

    std::unique_ptr<Poco::Net::StreamSocket> m_socket;
    std::unique_ptr<AMQP::Connection> m_amqpConn;
    std::unique_ptr<Buffer> m_inBuf;
    std::unique_ptr<Buffer> m_outBuf;
    std::vector<char> m_tmpBuf;
    std::atomic<bool> m_closed{true};
    std::atomic<bool> m_stop{false};
    std::string m_error;
    std::unique_ptr<std::thread> m_thread;
    std::string m_host;
    uint16_t m_port;
    bool m_ssl;
};

} // namespace BlackRabbitMQ
