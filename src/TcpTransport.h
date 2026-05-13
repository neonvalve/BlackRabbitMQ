#pragma once

// Платформенный селектор TCP транспорта.
// Linux:   AMQP::LibEventHandler (libevent + OpenSSL)
// Windows: AMQP::ConnectionHandler (POCO)

#if defined(__linux__) || defined(__APPLE__)
#include "Platform/TcpTransportLinux.h"
namespace BlackRabbitMQ { using TcpTransport = TcpTransportLinux; }
#elif defined(_WIN32) || defined(_WIN64)
#include "Platform/TcpTransportWindows.h"
namespace BlackRabbitMQ { using TcpTransport = TcpTransportWindows; }
#else
#error "Unsupported platform"
#endif
