#pragma once

#include "Component.h"
#include "CallContext.h"
#include "Message.h"
#include "TlsOptions.h"
#include "Logger.h"

#include <amqpcpp/table.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

namespace BlackRabbitMQ {

class Client;
class Consumer;

namespace AddIn1S {

// Совместимый с 1С API над Client и Consumer.
// Исключения не прокидываются в 1С — перехватываются в wrapCall.
//
// Ключевые решения:
//   - BasicReject(tag, requeue) — добавлен параметр requeue
//   - Publish: бинарно-безопасное тело (.data()+.size(), не strlen)
//   - Внутри: std::unique_ptr, без ручного new/delete
class RabbitApi1S : public Component {
public:
    // Потолок внутренней очереди по умолчанию. Штатный регулятор потока —
    // prefetch: пока 1С не подтвердит, брокер больше не отдаст, и очередь
    // физически не превысит prefetch. Потолок нужен для noAck, где брокер
    // prefetch игнорирует по спецификации и сваливает очередь целиком:
    // без него память rphost растёт до OOM, а это уронит чужие сеансы.
    // Меняется свойством MaxQueuedMessages до BasicConsume.
    static constexpr size_t kDefaultMaxQueuedMessages = 10000;

    // Глубина буфера внешних событий платформы. Значение по умолчанию рассчитано
    // на редкие уведомления: на потоке сообщений платформа начинает отказывать
    // в приёме, и события пропадают (проверено — из 50 доходило одно).
    static constexpr long kEventBufferDepth = 10000;

    RabbitApi1S();
    ~RabbitApi1S() override;

    // --- 1C Methods (имена сохранены ради совместимости с существующим кодом 1С) ---

    // Connect(host, port, user, pwd, vhost, skip, ssl, timeout)
    void connectImpl(CallContext& ctx);

    // DeclareExchange(name, type, onlyCheckIfExists, durable, autodelete, propsJson)
    void declareExchangeImpl(CallContext& ctx);

    // DeleteExchange(name, ifUnused)
    void deleteExchangeImpl(CallContext& ctx);

    // DeclareQueue(name, onlyCheckIfExists, durable, exclusive, autodelete,
    //              maxPriority, propsJson)
    void declareQueueImpl(CallContext& ctx);

    // DeleteQueue(name, ifUnused, ifEmpty)
    void deleteQueueImpl(CallContext& ctx);

    // BindQueue(queue, exchange, routingKey, propsJson)
    void bindQueueImpl(CallContext& ctx);

    // UnbindQueue(queue, exchange, routingKey)
    void unbindQueueImpl(CallContext& ctx);

    // BasicPublish(exchange, routingKey, message, skip, persistent, propsJson)
    // Тело сообщения передаётся бинарно-безопасно (по размеру, не strlen)
    void basicPublishImpl(CallContext& ctx);

    // BasicConsume(queue, consumerId, noConfirm, exclusive, selectSize, propsJson)
    // Возвращает consumerTag.
    // Поддерживает два режима:
    //   - Legacy polling: сообщения в m_messageQueue, читать через BasicConsumeMessage
    //   - Event-driven: сообщения через ExternalEvent (если включено)
    void basicConsumeImpl(CallContext& ctx);

    // BasicConsumeMessage(skip, &outData, &outMessageTag, timeout)
    // Возвращает тело сообщения и deliveryTag.
    // Для обратной совместимости — polling режим.
    void basicConsumeMessageImpl(CallContext& ctx);

    // Включить событийную модель: сообщения доставляются через ExternalEvent.
    // После вызова BasicConsume сообщения приходят в ОбработкаВнешнегоСобытия.
    void enableExternalEvent(bool enable) { m_useExternalEvent = enable; }
    bool isExternalEventEnabled() const { return m_useExternalEvent; }

    // BasicCancel()
    void basicCancelImpl(CallContext& ctx);

    // Reconnect()
    // Поднимает соединение после обрыва и восстанавливает потребителя
    // с прежними параметрами. Тег потребителя брокер присвоит новый.
    void reconnectImpl(CallContext& ctx);

    // BasicAck(deliveryTag, multiple = Ложь)
    // multiple: подтвердить всё до deliveryTag включительно — один кадр
    // вместо N вызовов, ради этого и делалось пакетное получение.
    void basicAckImpl(CallContext& ctx);

    // BasicConsumeMessages(count, timeout) — массив сообщений одним вызовом.
    // Возвращает JSON: {"count":N,"lastTag":T,"messages":[...]}.
    void basicConsumeMessagesImpl(CallContext& ctx);

    // BasicReject(deliveryTag, requeue)
    // Параметр requeue: в типовых решениях его обычно нет.
    void basicRejectImpl(CallContext& ctx);

    // GetQueueInfo(queue) — состояние очереди со слов брокера, JSON:
    // {"queue":"...","messages":N,"consumers":M}. Очередь не создаётся
    // и не меняется: объявление идёт в passive-режиме.
    void getQueueInfoImpl(CallContext& ctx);

    // PurgeQueue(queue) — очистить очередь, вернуть число удалённых сообщений.
    // Сама очередь и привязки остаются на месте.
    void purgeQueueImpl(CallContext& ctx);

    // SetPublishMode(режим): "confirms" (по умолчанию) или "transactions".
    // Confirms — расширение RabbitMQ: быстрее вдвое. Transactions — класс tx
    // из спецификации AMQP 0.9.1, нужен на брокерах без поддержки confirms.
    void setPublishModeImpl(CallContext& ctx);

    // FlushPublish() — дождаться подтверждения всего, что отправлено пакетно.
    // В режиме "batch" публикация возвращает управление сразу, и до этого
    // вызова сообщения нельзя считать доставленными.
    void flushPublishImpl(CallContext& ctx);

    // SleepNative(milliseconds)
    void sleepNativeImpl(CallContext& ctx);

    // EnableExternalEvent(enable) — включить/выключить событийную модель
    void enableExternalEventImpl(CallContext& ctx);

    // --- Properties ---
    void getRoutingKeyImpl(CallContext& ctx);
    void getHeadersImpl(CallContext& ctx);
    void setPriorityImpl(CallContext& ctx);
    void getPriorityImpl(CallContext& ctx);
    void setMsgPropImpl(long propNum, CallContext& ctx);
    void getMsgPropImpl(long propNum, CallContext& ctx);
    // SslCaFile / SslVerifyPeer / SslVerifyHostname — политика проверки
    // сертификата брокера. Читаются в момент Connect.
    void setOptionPropImpl(long propNum, CallContext& ctx);
    void getOptionPropImpl(long propNum, CallContext& ctx);
    // Пересобрать журнал после смены LogFile или LogLevel.
    void applyLogSettings();

private:
    // Параметры активного потребителя: нужны, чтобы поднять его после обрыва.
    struct ConsumeParams {
        std::string queue;
        std::string consumerId;
        bool noConfirm = false;
        bool exclusive = false;
        uint16_t prefetch = 0;
        AMQP::Table args;
        bool valid = false;
    };

    // Запустить потребителя по сохранённым параметрам (BasicConsume и Reconnect).
    void startConsumer(const ConsumeParams& params);

    // Поднять соединение и потребителя. Общий путь для метода Reconnect
    // и фонового сторожа; вызывается с уже захваченным m_callMutex.
    void restoreConnection();

    // Фоновый сторож: замечает обрыв и переподключается сам, с нарастающей
    // паузой. Без него после ночного обрыва обмен стоит до прихода человека —
    // фоновому заданию некому сказать Reconnect.
    void watchdogLoop();
    void startWatchdog();
    void stopWatchdog();
    // Уведомить 1С о смене состояния связи (только в событийном режиме).
    void notifyConnectionEvent(const char16_t* event, const std::string& detail);

    void checkConnection();
    // Ack/Reject возможны только пока жив потребитель: подтверждать нужно
    // на канале, доставившем сообщение.
    void checkConsumer(const char* method);
    AMQP::Table headersFromJson(const std::string& json, bool forConsume = false);
    std::string lastMessageHeaders();
    void clear();

    std::unique_ptr<Client> m_client;
    std::unique_ptr<Consumer> m_consumer;
    ConsumeParams m_consumeParams;
    TlsOptions m_tls;
    size_t m_maxQueuedMessages{kDefaultMaxQueuedMessages};
    int m_heartbeat{-1};    // -1 — как предложит брокер, 0 — выключить

    // Автоматическое переподключение
    std::atomic<bool> m_autoReconnect{false};
    int m_reconnectDelayMs{1000};        // первая пауза
    int m_reconnectMaxDelayMs{60000};    // потолок нарастающей паузы
    std::atomic<int> m_reconnectCount{0};// сколько раз подняли связь
    std::thread m_watchdog;
    std::atomic<bool> m_watchdogStop{false};
    std::mutex m_watchdogMutex;
    std::condition_variable m_watchdogCv;

    // Журнал: путь и уровень приходят из 1С отдельными свойствами,
    // поэтому храним их и пересобираем настройку при изменении любого.
    std::string m_logFile;
    std::string m_logLevel;

    // Очередь сообщений для polling-совместимости (BasicConsumeMessage)
    std::queue<Message> m_messageQueue;
    std::mutex m_queueMutex;
    std::condition_variable m_cvDataArrived;
    std::string m_consumerError;
    bool m_useExternalEvent = false;
    // Сколько событий платформа отказалась принять (переполнение её буфера).
    size_t m_eventsDropped = 0;

    // Свойства для следующей публикации (устанавливаются через setMsgProp)
    MessageProperties m_outgoingProps;
    int m_outgoingPriority = 0;
    // Последнее полученное сообщение
    Message m_lastMessage;
};

} // namespace AddIn1S
} // namespace BlackRabbitMQ
