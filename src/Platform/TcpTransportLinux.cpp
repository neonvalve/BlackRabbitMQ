#include "Platform/TcpTransportLinux.h"

namespace BlackRabbitMQ {

TcpTransportLinux::TcpTransportLinux(event_base* evbase)
    : AMQP::LibEventHandler(evbase)
    , m_lost(true)
{
}

TcpTransportLinux::~TcpTransportLinux() = default;

void TcpTransportLinux::onConnected(AMQP::TcpConnection* /*connection*/) {
    m_lost.store(false, std::memory_order_release);
}

void TcpTransportLinux::onLost(AMQP::TcpConnection* /*connection*/) {
    m_lost.store(true, std::memory_order_release);
}

void TcpTransportLinux::onError(AMQP::TcpConnection* /*connection*/, const char* message) {
    if (message) {
        m_error = message;
    }
}

} // namespace BlackRabbitMQ
