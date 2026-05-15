#pragma once

// Фабрика транспорта. Весь платформенный #ifdef — только здесь и в Connection.cpp.
// Больше нигде в коде нет платформенных условий.

#include "ITransport.h"
#include <memory>

#if defined(__linux__) || defined(__APPLE__)
#include "Platform/LibeventTransport.h"
namespace BlackRabbitMQ { inline std::unique_ptr<ITransport> makeTransport() { return std::make_unique<LibeventTransport>(); } }
#elif defined(_WIN32) || defined(_WIN64)
#include "Platform/PocoTransport.h"
namespace BlackRabbitMQ { inline std::unique_ptr<ITransport> makeTransport() { return std::make_unique<PocoTransport>(); } }
#else
#error "Unsupported platform"
#endif
