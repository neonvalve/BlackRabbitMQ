#include "Platform/PocoTransport.h"

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

// --- Buffer ---

size_t PocoTransport::Buffer::write(const char* src, size_t sz) {
    if (used == data.size()) return 0;
    size_t writeSz = sz;
    if (used + sz > data.size()) writeSz = data.size() - used;
    std::memcpy(data.data() + used, src, writeSz);
    used += writeSz;
    return writeSz;
}

void PocoTransport::Buffer::shift(size_t count) {
    if (count >= used) { used = 0; return; }
    size_t diff = used - count;
    std::memmove(data.data(), data.data() + count, diff);
    used = diff;
}

// --- Constructor / Destructor ---

PocoTransport::PocoTransport()
    : m_port(0), m_ssl(false)
{
}

PocoTransport::~PocoTransport() {
    disconnect();
}

// --- ITransport ---

void PocoTransport::connect(const AMQP::Address& address, int /*timeoutSec*/) {
    m_host = address.hostname();
    m_port = address.port();
    m_ssl = address.secure();

    Poco::Net::initializeSSL();

    Poco::Net::SocketAddress sockAddr(m_host, m_port);

    if (m_ssl) {
        auto* sslSocket = new Poco::Net::SecureStreamSocket();
        sslSocket->setPeerHostName(m_host);
        sslSocket->setLazyHandshake(true);
        m_socket.reset(sslSocket);
    } else {
        m_socket.reset(new Poco::Net::StreamSocket());
    }

    m_socket->connect(sockAddr);
    m_socket->setBlocking(true);
    m_socket->setSendBufferSize(static_cast<int>(TEMP_BUFFER_SIZE));
    m_socket->setReceiveBufferSize(static_cast<int>(TEMP_BUFFER_SIZE));
    m_socket->setKeepAlive(true);

    m_inBuf.reset(new Buffer(BUFFER_SIZE));
    m_outBuf.reset(new Buffer(BUFFER_SIZE));
    m_tmpBuf.resize(TEMP_BUFFER_SIZE);

    m_amqpConn.reset(new AMQP::Connection(this, address.login(), address.vhost()));

    startLoop(m_host, m_port, m_ssl);
}

void PocoTransport::disconnect() {
    stopLoop();
    m_amqpConn.reset(nullptr);
    m_inBuf.reset(nullptr);
    m_outBuf.reset(nullptr);
    if (m_socket) {
        try { m_socket->close(); } catch (...) {}
        m_socket.reset(nullptr);
    }
    m_closed.store(true, std::memory_order_release);
}

std::unique_ptr<AMQP::Channel> PocoTransport::createChannel() {
    if (!m_amqpConn) throw std::runtime_error("Not connected");
    return std::make_unique<AMQP::Channel>(m_amqpConn.get());
}

// --- Loop ---

void PocoTransport::startLoop(const std::string& /*host*/, uint16_t /*port*/, bool /*ssl*/) {
    m_stop.store(false, std::memory_order_release);
    m_thread.reset(new std::thread(loopThread, this));
}

void PocoTransport::stopLoop() {
    m_stop.store(true, std::memory_order_release);
    if (m_thread && m_thread->joinable()) {
        m_thread->join();
    }
    m_thread.reset(nullptr);
}

void PocoTransport::loopThread(PocoTransport* self) {
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

void PocoTransport::loopIteration() {
    if (!m_socket) return;

    Poco::Timespan pollTimeout(0, 1000);
    if (m_socket->poll(pollTimeout, Poco::Net::Socket::SELECT_READ)) {
        int expected = m_amqpConn ? m_amqpConn->expected() : 4;
        if (expected <= 0) expected = 4;
        while (expected > 0) {
            if (m_tmpBuf.size() < static_cast<size_t>(expected))
                m_tmpBuf.resize(expected, 0);
            int received = m_socket->receiveBytes(m_tmpBuf.data(), expected);
            if (received <= 0) break;
            m_inBuf->write(m_tmpBuf.data(), received);
            expected = m_socket->available();
        }
    }

    if (m_amqpConn && m_inBuf->available() > 0) {
        size_t parsed = m_amqpConn->parse(m_inBuf->ptr(), m_inBuf->available());
        if (parsed == m_inBuf->available()) m_inBuf->drain();
        else if (parsed > 0) m_inBuf->shift(parsed);
    }
    sendDataFromBuffer();
}

void PocoTransport::sendDataFromBuffer() {
    if (m_outBuf->available() > 0 && m_socket) {
        m_socket->sendBytes(m_outBuf->ptr(), static_cast<int>(m_outBuf->available()));
        m_outBuf->drain();
    }
}

// --- AMQP::ConnectionHandler ---

void PocoTransport::onData(AMQP::Connection*, const char* data, size_t size) {
    size_t written = m_outBuf->write(data, size);
    while (written < size) {
        sendDataFromBuffer();
        written += m_outBuf->write(data + written, size - written);
    }
}
void PocoTransport::onReady(AMQP::Connection*) {
    m_closed.store(false, std::memory_order_release);
}
void PocoTransport::onError(AMQP::Connection*, const char* message) {
    if (message) m_error = message;
}
void PocoTransport::onClosed(AMQP::Connection*) {
    m_closed.store(true, std::memory_order_release);
}
uint16_t PocoTransport::onNegotiate(AMQP::Connection*, uint16_t interval) {
    return interval;
}

} // namespace BlackRabbitMQ
