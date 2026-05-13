#include "Platform/TcpTransportWindows.h"

namespace BlackRabbitMQ {

struct TcpTransportWindows::Impl {
    // TODO: POCO socket, SSL context, etc.
};

TcpTransportWindows::TcpTransportWindows(const std::string&, uint16_t, bool)
    : m_ready(false)
    , m_closed(false)
{
    // TODO: реализовать Windows-транспорт через POCO
}

TcpTransportWindows::~TcpTransportWindows() {
    close();
}

bool TcpTransportWindows::connect() {
    // TODO
    return false;
}

void TcpTransportWindows::close() {
    m_ready.store(false, std::memory_order_release);
    m_closed.store(true, std::memory_order_release);
}

void TcpTransportWindows::onData(AMQP::Connection*, const char*, size_t) {
    // TODO
}

void TcpTransportWindows::onReady(AMQP::Connection*) {
    m_ready.store(true, std::memory_order_release);
}

void TcpTransportWindows::onError(AMQP::Connection*, const char* message) {
    m_error = message ? message : "Unknown error";
    m_ready.store(false, std::memory_order_release);
}

void TcpTransportWindows::onClosed(AMQP::Connection*) {
    m_closed.store(true, std::memory_order_release);
}

uint16_t TcpTransportWindows::onNegotiate(AMQP::Connection*, uint16_t interval) {
    return interval; // Принимаем предложенный heartbeat
}

} // namespace BlackRabbitMQ
