#pragma once

#include <amqpcpp.h>
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <cstdint>
#include <vector>

namespace Poco::Net {
    class StreamSocket;
}

namespace BlackRabbitMQ {

// Реализация TCP транспорта для Windows через POCO.
// Имплементирует AMQP::ConnectionHandler.
// Исправлено против upstream: std::atomic вместо volatile bool.
class TcpTransportWindows : public AMQP::ConnectionHandler {
public:
    static constexpr size_t BUFFER_SIZE = 8 * 1024 * 1024;      // 8 MB
    static constexpr size_t TEMP_BUFFER_SIZE = 1 * 1024 * 1024; // 1 MB

    TcpTransportWindows(const std::string& host, uint16_t port, bool ssl);
    ~TcpTransportWindows() override;

    TcpTransportWindows(const TcpTransportWindows&) = delete;
    TcpTransportWindows& operator=(const TcpTransportWindows&) = delete;

    // Запустить поток чтения.
    void startLoop();

    // Остановить поток чтения.
    void stopLoop();

    // Состояние.
    bool isReady() const noexcept { return !m_closed.load(std::memory_order_acquire); }
    bool isClosed() const noexcept { return m_closed.load(std::memory_order_acquire); }
    const std::string& error() const noexcept { return m_error; }

    // AMQP::ConnectionHandler interface
    void onData(AMQP::Connection* connection, const char* data, size_t size) override;
    void onReady(AMQP::Connection* connection) override;
    void onError(AMQP::Connection* connection, const char* message) override;
    void onClosed(AMQP::Connection* connection) override;
    uint16_t onNegotiate(AMQP::Connection* connection, uint16_t interval) override;

    // Доступ к AMQP::Connection (нужен для channel).
    AMQP::Connection* amqpConnection() const noexcept { return m_amqpConn; }
    void setAmqpConnection(AMQP::Connection* conn) { m_amqpConn = conn; }

private:
    void loopIteration();
    void sendDataFromBuffer();
    void close();

    static void initializeSSL();
    static void uninitializeSSL();
    static void loopThread(TcpTransportWindows* self);

    // Buffer helper
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
    AMQP::Connection* m_amqpConn;

    Buffer m_inBuf;
    Buffer m_outBuf;
    std::vector<char> m_tmpBuf;

    std::atomic<bool> m_closed;
    std::atomic<bool> m_stop;
    std::string m_error;

    std::unique_ptr<std::thread> m_thread;

    static bool s_sslInited;
};

} // namespace BlackRabbitMQ
