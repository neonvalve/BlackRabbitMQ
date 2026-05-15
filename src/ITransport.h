#pragma once

#include <amqpcpp.h>
#include <memory>
#include <string>

namespace BlackRabbitMQ {

// Абстрактный транспорт для Connection.
// Скрывает платформенные различия:
//   - Linux/macOS: LibeventTransport (libevent + AMQP::TcpConnection)
//   - Windows:     PocoTransport (POCO + AMQP::Connection)
class ITransport {
public:
    virtual ~ITransport() = default;

    // Установить соединение с брокером.
    // Бросает std::runtime_error при ошибке.
    virtual void connect(const AMQP::Address& address, int timeoutSec) = 0;

    // Разорвать соединение.
    virtual void disconnect() = 0;

    // Создать новый AMQP канал.
    virtual std::unique_ptr<AMQP::Channel> createChannel() = 0;

    // Состояние соединения.
    virtual bool isConnected() const noexcept = 0;

    // Последняя ошибка.
    virtual const std::string& error() const noexcept = 0;
};

} // namespace BlackRabbitMQ
