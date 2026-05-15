#include "Connection.h"
#include "ITransport.h"

#if defined(__linux__) || defined(__APPLE__)
#include "Platform/LibeventTransport.h"
#elif defined(_WIN32) || defined(_WIN64)
#include "Platform/PocoTransport.h"
#else
#error "Unsupported platform"
#endif

#include <stdexcept>

namespace BlackRabbitMQ {

static std::unique_ptr<ITransport> createTransport() {
#if defined(__linux__) || defined(__APPLE__)
    return std::make_unique<LibeventTransport>();
#elif defined(_WIN32) || defined(_WIN64)
    return std::make_unique<PocoTransport>();
#endif
}

Connection::Connection(const AMQP::Address& address, int timeoutSec)
    : m_address(address)
    , m_timeoutSec(timeoutSec)
    , m_transport(createTransport())
{
}

Connection::~Connection() {
    disconnect();
}

void Connection::connect() {
    m_transport->connect(m_address, m_timeoutSec);
}

void Connection::disconnect() {
    m_transport->disconnect();
}

bool Connection::reconnect() {
    disconnect();
    try {
        connect();
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

std::unique_ptr<AMQP::Channel> Connection::createChannel() {
    return m_transport->createChannel();
}

} // namespace BlackRabbitMQ
