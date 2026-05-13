#include "Platform/TcpTransportWindows.h"

#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/AcceptCertificateHandler.h>
#include <Poco/Net/NetException.h>
#include <Poco/Timespan.h>

#include <thread>
#include <cstring>
#include <stdexcept>

namespace BlackRabbitMQ {

bool TcpTransportWindows::s_sslInited = false;

// --- Buffer ---

size_t TcpTransportWindows::Buffer::write(const char* src, size_t sz) {
    if (used == data.size()) return 0;
    size_t writeSz = sz;
    if (used + sz > data.size()) writeSz = data.size() - used;
    std::memcpy(data.data() + used, src, writeSz);
    used += writeSz;
    return writeSz;
}

void TcpTransportWindows::Buffer::shift(size_t count) {
    if (count >= used) { used = 0; return; }
    size_t diff = used - count;
    std::memmove(data.data(), data.data() + count, diff);
    used = diff;
}

// --- Constructor / Destructor ---

TcpTransportWindows::TcpTransportWindows(const std::string& host, uint16_t port, bool ssl)
    : m_amqpConn(nullptr)
    , m_inBuf(BUFFER_SIZE)
    , m_outBuf(BUFFER_SIZE)
    , m_tmpBuf(TEMP_BUFFER_SIZE)
    , m_closed(true)
    , m_stop(false)
{
    initializeSSL();

    Poco::Net::SocketAddress address(host, port);

    if (ssl) {
        auto* sslSocket = new Poco::Net::SecureStreamSocket();
        sslSocket->setPeerHostName(host);
        sslSocket->setLazyHandshake(true);
        m_socket.reset(sslSocket);
    } else {
        m_socket.reset(new Poco::Net::StreamSocket());
    }

    m_socket->connect(address);
    m_socket->setBlocking(true);
    m_socket->setSendBufferSize(static_cast<int>(TEMP_BUFFER_SIZE));
    m_socket->setReceiveBufferSize(static_cast<int>(TEMP_BUFFER_SIZE));
    m_socket->setKeepAlive(true);
}

TcpTransportWindows::~TcpTransportWindows() {
    close();
    stopLoop();
    uninitializeSSL();
}

// --- Loop ---

void TcpTransportWindows::startLoop() {
    m_stop.store(false, std::memory_order_release);
    m_thread.reset(new std::thread(loopThread, this));
}

void TcpTransportWindows::stopLoop() {
    m_stop.store(true, std::memory_order_release);
    if (m_thread && m_thread->joinable()) {
        m_thread->join();
    }
    m_thread.reset(nullptr);
}

void TcpTransportWindows::loopThread(TcpTransportWindows* self) {
    while (!self->m_stop.load(std::memory_order_acquire)) {
        try {
            self->loopIteration();
        } catch (const Poco::Net::ConnectionResetException& e) {
            self->m_error = e.displayText();
            if (self->m_amqpConn) self->m_amqpConn->close();
        } catch (const Poco::Exception& e) {
            self->m_error = std::string(e.what()) + ": " + e.displayText();
        } catch (const std::exception& e) {
            self->m_error = e.what();
        }
    }
}

void TcpTransportWindows::loopIteration() {
    if (!m_socket) return;

    Poco::Timespan pollTimeout(0, 1000); // 1 ms

    if (m_socket->poll(pollTimeout, Poco::Net::Socket::SELECT_READ)) {
        int expected = m_amqpConn ? m_amqpConn->expected() : 4;
        if (expected <= 0) expected = 4;

        while (expected > 0) {
            if (m_tmpBuf.size() < static_cast<size_t>(expected)) {
                m_tmpBuf.resize(expected, 0);
            }
            int received = m_socket->receiveBytes(m_tmpBuf.data(), expected);
            if (received <= 0) break;
            m_inBuf.write(m_tmpBuf.data(), received);
            expected = m_socket->available();
        }
    }

    // Передать данные в AMQP-CPP
    if (m_amqpConn && m_inBuf.available() > 0) {
        size_t parsed = m_amqpConn->parse(m_inBuf.ptr(), m_inBuf.available());
        if (parsed == m_inBuf.available()) {
            m_inBuf.drain();
        } else if (parsed > 0) {
            m_inBuf.shift(parsed);
        }
    }

    sendDataFromBuffer();
}

void TcpTransportWindows::sendDataFromBuffer() {
    if (m_outBuf.available() > 0 && m_socket) {
        m_socket->sendBytes(m_outBuf.ptr(), static_cast<int>(m_outBuf.available()));
        m_outBuf.drain();
    }
}

// --- AMQP::ConnectionHandler ---

void TcpTransportWindows::onData(AMQP::Connection* connection, const char* data, size_t size) {
    m_amqpConn = connection;
    size_t written = m_outBuf.write(data, size);
    while (written < size) {
        sendDataFromBuffer();
        written += m_outBuf.write(data + written, size - written);
    }
}

void TcpTransportWindows::onReady(AMQP::Connection* /*connection*/) {
    m_closed.store(false, std::memory_order_release);
}

void TcpTransportWindows::onError(AMQP::Connection* /*connection*/, const char* message) {
    if (message) m_error = message;
}

void TcpTransportWindows::onClosed(AMQP::Connection* /*connection*/) {
    m_closed.store(true, std::memory_order_release);
}

uint16_t TcpTransportWindows::onNegotiate(AMQP::Connection* /*connection*/, uint16_t interval) {
    return interval; // heartbeat: принимаем предложенный интервал
}

// --- Close ---

void TcpTransportWindows::close() {
    if (m_socket) {
        try {
            m_socket->close();
        } catch (...) {}
    }
    m_closed.store(true, std::memory_order_release);
}

// --- SSL ---

void TcpTransportWindows::initializeSSL() {
    if (s_sslInited) return;
    Poco::Net::initializeSSL();
    s_sslInited = true;
}

void TcpTransportWindows::uninitializeSSL() {
    if (!s_sslInited) return;
    Poco::Net::uninitializeSSL();
    s_sslInited = false;
}

} // namespace BlackRabbitMQ
