#pragma once

#include "TaskRunner.h"
#include "TlsOptions.h"

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

    // Параметры TLS. Задаются до connect(): в момент рукопожатия менять
    // политику проверки уже поздно.
    virtual void setTlsOptions(const TlsOptions& options) = 0;

    // Установить соединение с брокером.
    // Бросает std::runtime_error при ошибке.
    virtual void connect(const AMQP::Address& address, int timeoutSec) = 0;

    // Разорвать соединение.
    virtual void disconnect() = 0;

    // Создать новый AMQP канал.
    virtual std::unique_ptr<AMQP::Channel> createChannel() = 0;

    // Состояние соединения.
    virtual bool isConnected() const noexcept = 0;

    // Последняя ошибка. Возвращается копией: пишет её поток event loop,
    // читает поток 1С — отдавать ссылку на живую строку нельзя.
    virtual std::string error() const = 0;

    // Исполнитель задач в потоке, который владеет объектами AMQP-CPP.
    // Через него идут все обращения к библиотеке (см. TaskRunner.h).
    virtual ITaskRunner& taskRunner() = 0;
};

} // namespace BlackRabbitMQ
