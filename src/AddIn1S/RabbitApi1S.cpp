#include "RabbitApi1S.h"
// Ради номеров свойств: TLS-настройки различаются по enum, а не по числу —
// у соседних методов номера захардкожены, и это ровно та ошибка, которую
// не хочется повторять.
#include "RabbitMQClientNative.h"
#include "Client.h"
#include "Consumer.h"
#include "Message.h"
#include "Channel.h"
#include "Logger.h"

#include <nlohmann/json.hpp>

#include <chrono>
#include <thread>
#include <stdexcept>

namespace BlackRabbitMQ {
namespace AddIn1S {

using json = nlohmann::json;

namespace {

// Конвертер для callback'ов потока event loop: Component::m_converter
// принадлежит потоку 1С, а wstring_convert хранит внутреннее состояние —
// делить один объект между потоками нельзя (ловится ThreadSanitizer).
std::u16string toU16(const std::string& utf8) {
    std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> converter;
    return converter.from_bytes(utf8);
}

} // namespace

RabbitApi1S::RabbitApi1S()
    : Component("RabbitMQClient")
{
}

RabbitApi1S::~RabbitApi1S() {
    // Сторож трогает m_client и m_consumer — гасим его первым, иначе он
    // переживёт поля, за которыми следит.
    stopWatchdog();
}

// --- Проверка соединения ---

void RabbitApi1S::checkConnection() {
    if (!m_client || !m_client->isConnected()) {
        throw std::runtime_error("Connection is not established! Use Connect() first");
    }
}

// --- Connect ---

void RabbitApi1S::connectImpl(CallContext& ctx) {
    std::string host = ctx.stringParamUtf8();
    uint16_t port = static_cast<uint16_t>(ctx.intParam());
    std::string user = ctx.stringParamUtf8();
    std::string pwd = ctx.stringParamUtf8();
    std::string vhost = ctx.stringParamUtf8();
    ctx.skipParam(); // skip
    bool ssl = ctx.boolParam();
    int timeout = ctx.intParam();

    if (host.empty()) {
        throw std::runtime_error("Empty hostname not allowed");
    }

    // Разорвать предыдущее соединение
    clear();

    // Создать новое
    m_client.reset(new Client());
    m_client->connect(host, port, user, pwd, vhost, ssl, timeout, m_tls, m_heartbeat);

    // Сторож живёт вместе с соединением и сам проверяет, включено ли
    // автопереподключение: свойство могут выставить и после Connect.
    startWatchdog();
}

// --- Журнал ---

void RabbitApi1S::applyLogSettings() {
    // Уровень по умолчанию — info: если путь задали, а уровень нет, писать
    // хочется хотя бы подключения и обрывы, ради которых журнал и включают.
    const LogLevel level = m_logLevel.empty() ? LogLevel::Info
                                              : Logger::levelFromString(m_logLevel);
    Logger::instance().configure(m_logFile, level);
    if (!m_logFile.empty() && level != LogLevel::Off) {
        BRMQ_LOG_INFO("BlackRabbitMQ " + m_version + ": log level "
                      + Logger::levelToString(level));
    }
}

// --- Настройки TLS ---

void RabbitApi1S::setTlsPropImpl(long propNum, CallContext& ctx) {
    switch (propNum) {
        case RabbitMQClientNative::ePropSslCaFile:
            m_tls.caFile = ctx.stringParamUtf8();
            break;
        case RabbitMQClientNative::ePropSslVerifyPeer:
            m_tls.verifyPeer = ctx.boolParam();
            break;
        case RabbitMQClientNative::ePropSslVerifyHostname:
            m_tls.verifyHostname = ctx.boolParam();
            break;
        case RabbitMQClientNative::ePropHeartbeat:
            m_heartbeat = ctx.intParam();
            break;
        case RabbitMQClientNative::ePropAutoReconnect:
            m_autoReconnect.store(ctx.boolParam(), std::memory_order_release);
            break;
        case RabbitMQClientNative::ePropReconnectDelayMs: {
            const int value = ctx.intParam();
            // Пауза меньше сотни миллисекунд — это долбёжка в брокер, который
            // и так не отвечает.
            m_reconnectDelayMs = value < 100 ? 100 : value;
            break;
        }
        case RabbitMQClientNative::ePropReconnectMaxDelayMs: {
            const int value = ctx.intParam();
            m_reconnectMaxDelayMs = value < m_reconnectDelayMs ? m_reconnectDelayMs : value;
            break;
        }
        case RabbitMQClientNative::ePropLogFile:
            m_logFile = ctx.stringParamUtf8();
            applyLogSettings();
            break;
        case RabbitMQClientNative::ePropLogLevel:
            m_logLevel = ctx.stringParamUtf8();
            applyLogSettings();
            break;
        default:
            break;
    }
}

void RabbitApi1S::getTlsPropImpl(long propNum, CallContext& ctx) {
    switch (propNum) {
        case RabbitMQClientNative::ePropSslCaFile:
            ctx.setStringOrEmptyResult(m_converter.from_bytes(m_tls.caFile));
            break;
        case RabbitMQClientNative::ePropSslVerifyPeer:
            ctx.setBoolResult(m_tls.verifyPeer);
            break;
        case RabbitMQClientNative::ePropSslVerifyHostname:
            ctx.setBoolResult(m_tls.verifyHostname);
            break;
        case RabbitMQClientNative::ePropHeartbeat:
            ctx.setIntResult(m_heartbeat);
            break;
        case RabbitMQClientNative::ePropAutoReconnect:
            ctx.setBoolResult(m_autoReconnect.load(std::memory_order_acquire));
            break;
        case RabbitMQClientNative::ePropReconnectDelayMs:
            ctx.setIntResult(m_reconnectDelayMs);
            break;
        case RabbitMQClientNative::ePropReconnectMaxDelayMs:
            ctx.setIntResult(m_reconnectMaxDelayMs);
            break;
        case RabbitMQClientNative::ePropReconnectCount:
            ctx.setIntResult(m_reconnectCount.load(std::memory_order_relaxed));
            break;
        case RabbitMQClientNative::ePropLogFile:
            ctx.setStringOrEmptyResult(m_converter.from_bytes(m_logFile));
            break;
        case RabbitMQClientNative::ePropLogLevel:
            ctx.setStringOrEmptyResult(m_converter.from_bytes(
                Logger::levelToString(Logger::instance().level())));
            break;
        default:
            ctx.setEmptyResult();
            break;
    }
}

// --- Exchange ---

void RabbitApi1S::declareExchangeImpl(CallContext& ctx) {
    checkConnection();

    std::string name = ctx.stringParamUtf8();
    std::string type = ctx.stringParamUtf8();
    bool passive = ctx.boolParam();
    bool durable = ctx.boolParam();
    bool autoDelete = ctx.boolParam();
    std::string propsJson = ctx.stringParamUtf8();

    AMQP::ExchangeType exType = AMQP::ExchangeType::topic;
    if (type == "topic")       exType = AMQP::ExchangeType::topic;
    else if (type == "fanout")  exType = AMQP::ExchangeType::fanout;
    else if (type == "direct")  exType = AMQP::ExchangeType::direct;
    else throw std::runtime_error("Exchange type not supported: " + type);

    m_client->declareExchange(name, exType, passive, durable, autoDelete, headersFromJson(propsJson));
}

void RabbitApi1S::deleteExchangeImpl(CallContext& ctx) {
    checkConnection();
    std::string name = ctx.stringParamUtf8();
    bool ifUnused = ctx.boolParam();
    m_client->deleteExchange(name, ifUnused);
}

// --- Queue ---

void RabbitApi1S::declareQueueImpl(CallContext& ctx) {
    checkConnection();

    std::string name = ctx.stringParamUtf8();
    bool passive = ctx.boolParam();
    bool durable = ctx.boolParam();
    bool exclusive = ctx.boolParam();
    bool autoDelete = ctx.boolParam();
    auto maxPriority = static_cast<uint16_t>(ctx.intParam());
    std::string propsJson = ctx.stringParamUtf8();

    AMQP::Table args = headersFromJson(propsJson);
    if (maxPriority != 0) {
        args.set("x-max-priority", maxPriority);
    }

    m_client->declareQueue(name, passive, durable, exclusive, autoDelete, args);
    ctx.setStringOrEmptyResult(m_converter.from_bytes(name));
}

void RabbitApi1S::deleteQueueImpl(CallContext& ctx) {
    checkConnection();
    std::string name = ctx.stringParamUtf8();
    bool ifUnused = ctx.boolParam();
    bool ifEmpty = ctx.boolParam();
    m_client->deleteQueue(name, ifUnused, ifEmpty);
}

// --- Binding ---

void RabbitApi1S::bindQueueImpl(CallContext& ctx) {
    checkConnection();
    std::string queue = ctx.stringParamUtf8();
    std::string exchange = ctx.stringParamUtf8();
    std::string routingKey = ctx.stringParamUtf8();
    std::string propsJson = ctx.stringParamUtf8();

    m_client->bindQueue(exchange, queue, routingKey, headersFromJson(propsJson));
}

void RabbitApi1S::unbindQueueImpl(CallContext& ctx) {
    checkConnection();
    std::string queue = ctx.stringParamUtf8();
    std::string exchange = ctx.stringParamUtf8();
    std::string routingKey = ctx.stringParamUtf8();

    m_client->unbindQueue(exchange, queue, routingKey);
}

// --- Publish ---

void RabbitApi1S::basicPublishImpl(CallContext& ctx) {
    checkConnection();

    std::string exchange = ctx.stringParamUtf8();
    std::string routingKey = ctx.stringParamUtf8();
    std::string body = ctx.stringParamUtf8();
    ctx.skipParam();
    bool persistent = ctx.boolParam();
    std::string propsJson = ctx.stringParamUtf8();

    // Бинарно-безопасный Envelope: .data() + .size(), не strlen
    AMQP::Envelope envelope(body.data(), body.size());

    if (persistent) {
        envelope.setDeliveryMode(2);
    }

    // Применить свойства, установленные через setMsgProp
    if (!m_outgoingProps.correlationId.empty())   envelope.setCorrelationID(m_outgoingProps.correlationId);
    if (!m_outgoingProps.messageId.empty())       envelope.setMessageID(m_outgoingProps.messageId);
    if (!m_outgoingProps.typeName.empty())        envelope.setTypeName(m_outgoingProps.typeName);
    if (!m_outgoingProps.appId.empty())           envelope.setAppID(m_outgoingProps.appId);
    if (!m_outgoingProps.contentEncoding.empty()) envelope.setContentEncoding(m_outgoingProps.contentEncoding);
    if (!m_outgoingProps.contentType.empty())     envelope.setContentType(m_outgoingProps.contentType);
    if (!m_outgoingProps.userId.empty())          envelope.setUserID(m_outgoingProps.userId);
    if (!m_outgoingProps.clusterId.empty())       envelope.setClusterID(m_outgoingProps.clusterId);
    if (!m_outgoingProps.expiration.empty())      envelope.setExpiration(m_outgoingProps.expiration);
    if (!m_outgoingProps.replyTo.empty())         envelope.setReplyTo(m_outgoingProps.replyTo);
    if (m_outgoingPriority != 0)                  envelope.setPriority(static_cast<uint8_t>(m_outgoingPriority));

    AMQP::Table headers = headersFromJson(propsJson);
    if (headers.keys().size() > 0) {
        envelope.setHeaders(headers);
    }

    // Публикуем именно собранный конверт: свойства и заголовки должны дойти
    // до брокера (раньше здесь терялось всё, кроме тела).
    m_client->publish(exchange, routingKey, envelope);

    // Свойства одноразовые: заданы перед публикацией, сброшены после.
    m_outgoingProps = MessageProperties{};
    m_outgoingPriority = 0;
}

// --- Состояние очереди ---

void RabbitApi1S::getQueueInfoImpl(CallContext& ctx) {
    checkConnection();

    const std::string queue = ctx.stringParamUtf8();
    if (queue.empty()) {
        throw std::runtime_error("GetQueueInfo: queue name is required");
    }

    // passive: брокер отвечает состоянием существующей очереди и не создаёт
    // новую. Если очереди нет — ошибка от брокера, а не тихий ноль.
    const auto stats = m_client->declareQueue(queue, true);

    json info;
    info["queue"] = queue;
    info["messages"] = stats.messageCount;
    info["consumers"] = stats.consumerCount;

    ctx.setStringResult(m_converter.from_bytes(info.dump()));
}

void RabbitApi1S::purgeQueueImpl(CallContext& ctx) {
    checkConnection();

    const std::string queue = ctx.stringParamUtf8();
    if (queue.empty()) {
        throw std::runtime_error("PurgeQueue: queue name is required");
    }

    ctx.setIntResult(static_cast<int>(m_client->purgeQueue(queue)));
}

// --- Consume (legacy polling) ---

void RabbitApi1S::basicConsumeImpl(CallContext& ctx) {
    checkConnection();

    ConsumeParams params;
    params.queue = ctx.stringParamUtf8();
    params.consumerId = ctx.stringParamUtf8(true);
    params.noConfirm = ctx.boolParam();
    params.exclusive = ctx.boolParam();
    params.prefetch = static_cast<uint16_t>(ctx.intParam());
    params.args = headersFromJson(ctx.stringParamUtf8(), true);
    params.valid = true;

    // Остановить предыдущий consumer
    clear();

    startConsumer(params);
    m_consumeParams = params; // пригодятся для восстановления после обрыва

    ctx.setStringOrEmptyResult(m_converter.from_bytes(m_consumer->tag()));
}

// Общий путь для BasicConsume и восстановления после Reconnect.
void RabbitApi1S::startConsumer(const ConsumeParams& params) {
    BRMQ_LOG_INFO("BasicConsume: queue '" + params.queue + "', prefetch "
                  + std::to_string(params.prefetch)
                  + (params.noConfirm ? ", noAck" : ", manual ack")
                  + (m_useExternalEvent ? ", external events" : ", polling"));
    m_consumer.reset(new Consumer());

    // Канал потребителя: ack/reject пойдут именно по нему.
    auto channel = m_client->createChannel();

    m_consumer->start(
        std::move(channel),
        params.queue,
        params.consumerId,
        params.noConfirm,
        params.exclusive,
        params.prefetch,
        params.args,
        // onMessage
        [this](const Message& msg) {
            if (m_useExternalEvent && m_addin) {
                // Событийная модель: отправить ExternalEvent в 1С
                json eventData;
                eventData["body"] = msg.body;
                eventData["deliveryTag"] = msg.deliveryTag;
                eventData["routingKey"] = msg.routingKey;
                eventData["priority"] = msg.priority;
                eventData["redelivered"] = msg.redelivered;
                eventData["correlationId"] = msg.props.correlationId;
                eventData["messageId"] = msg.props.messageId;
                eventData["contentType"] = msg.props.contentType;

                std::u16string u16data = toU16(eventData.dump());
                const bool accepted = m_addin->ExternalEvent(
                    const_cast<WCHAR_T*>(reinterpret_cast<const WCHAR_T*>(u"BlackRabbitMQ")),
                    const_cast<WCHAR_T*>(reinterpret_cast<const WCHAR_T*>(u"MessageReceived")),
                    const_cast<WCHAR_T*>(reinterpret_cast<const WCHAR_T*>(u16data.c_str()))
                );

                // Буфер событий платформы конечен. Если 1С не успевает его
                // разгребать (длинная операция в обработчике, поток сообщений
                // быстрее обработки), ExternalEvent начинает отказывать —
                // и раньше сообщение молча пропадало: брокер считал его
                // доставленным, а до кода 1С оно не доходило. Замерено на
                // потоке в 50 сообщений: доходило одно.
                if (!accepted) {
                    std::lock_guard<std::mutex> lock(m_queueMutex);
                    ++m_eventsDropped;
                    // В журнал — обязательно: без него потеря выглядит как
                    // «компонента иногда не доставляет сообщения».
                    BRMQ_LOG_WARN("External event rejected by 1C (buffer overflow), dropped "
                                  + std::to_string(m_eventsDropped)
                                  + " message(s), deliveryTag " + std::to_string(msg.deliveryTag));
                    m_consumerError = "External event buffer overflow: "
                        + std::to_string(m_eventsDropped)
                        + " message(s) not delivered to 1C. Reduce prefetch, "
                          "speed up the event handler or switch to polling mode";
                }
                // В polling-очередь не дублируем: в событийном режиме её никто
                // не читает, и она росла бы до исчерпания памяти процесса 1С.
                return;
            }

            std::lock_guard<std::mutex> lock(m_queueMutex);
            if (m_messageQueue.size() >= kMaxQueuedMessages) {
                // 1С не успевает читать (или включён noConfirm, и брокер шлёт
                // без ограничения prefetch). Явная ошибка лучше, чем рост до OOM.
                if (m_consumerError.empty()) {
                    m_consumerError = "Message queue overflow ("
                        + std::to_string(kMaxQueuedMessages)
                        + " messages): BasicConsumeMessage is not called often enough";
                }
                return;
            }
            m_messageQueue.push(msg);
            m_cvDataArrived.notify_all();
        },
        // onCancelled
        [this](const std::string& consumerTag) {
            BRMQ_LOG_WARN("Consumer cancelled by broker: " + consumerTag);
            std::lock_guard<std::mutex> lock(m_queueMutex);
            m_consumerError = "Consumer cancelled: " + consumerTag;
            if (m_useExternalEvent && m_addin) {
                // Уведомить 1С об отмене потребителя
                std::u16string u16tag = toU16(consumerTag);
                m_addin->ExternalEvent(
                    const_cast<WCHAR_T*>(reinterpret_cast<const WCHAR_T*>(u"BlackRabbitMQ")),
                    const_cast<WCHAR_T*>(reinterpret_cast<const WCHAR_T*>(u"ConsumerCancelled")),
                    const_cast<WCHAR_T*>(reinterpret_cast<const WCHAR_T*>(u16tag.c_str()))
                );
            }
        }
    );
}

void RabbitApi1S::basicConsumeMessageImpl(CallContext& ctx) {
    checkConnection();

    if (!m_consumer || !m_consumer->isActive()) {
        throw std::runtime_error("No active consumers. Use BasicConsume() first");
    }

    ctx.skipParam();
    tVariant* outData = ctx.skipParam();
    tVariant* outMessageTag = ctx.skipParam();
    int timeout = ctx.intParam();

    ctx.setEmptyResult(outData);
    ctx.setIntResult(0, outMessageTag);

    {
        std::unique_lock<std::mutex> lock(m_queueMutex);

        if (m_messageQueue.empty()) {
            if (!m_consumerError.empty()) {
                std::string err = std::move(m_consumerError);
                throw std::runtime_error(err);
            }
            if (!m_cvDataArrived.wait_for(lock, std::chrono::milliseconds(timeout),
                                          [this] { return !m_messageQueue.empty(); })) {
                ctx.setBoolResult(false);
                return;
            }
        }

        if (m_messageQueue.empty()) {
            throw std::runtime_error("Empty consume message");
        }

        m_lastMessage = m_messageQueue.front();
        m_messageQueue.pop();
    }

    ctx.setStringResult(m_converter.from_bytes(m_lastMessage.body), outData);
    ctx.setLongResult(static_cast<int64_t>(m_lastMessage.deliveryTag), outMessageTag);
    ctx.setBoolResult(true);
}

// --- Ack / Reject ---

// Ack/Reject обязаны идти по тому же каналу, что доставил сообщение
// (иначе брокер отвечает PRECONDITION_FAILED и закрывает канал),
// поэтому — только через Consumer, который этим каналом владеет.

void RabbitApi1S::checkConsumer(const char* method) {
    if (!m_consumer || !m_consumer->isActive()) {
        throw std::runtime_error(std::string(method)
            + ": no active consumer. Use BasicConsume() first");
    }
    if (!m_consumer->canAck()) {
        throw std::runtime_error(std::string(method)
            + ": consumer channel is closed, message cannot be confirmed."
              " Use BasicConsume() again");
    }
}

void RabbitApi1S::basicAckImpl(CallContext& ctx) {
    checkConnection();
    uint64_t tag = static_cast<uint64_t>(ctx.longParam());
    if (tag == 0) {
        throw std::runtime_error("Message tag cannot be empty!");
    }
    checkConsumer("BasicAck");
    m_consumer->ack(tag);
}

void RabbitApi1S::basicRejectImpl(CallContext& ctx) {
    checkConnection();
    uint64_t tag = static_cast<uint64_t>(ctx.longParam());
    if (tag == 0) {
        throw std::runtime_error("Message tag cannot be empty!");
    }
    // Второй параметр — requeue: в типовых решениях его обычно нет
    bool requeue = ctx.boolParam();
    checkConsumer("BasicReject");
    m_consumer->reject(tag, requeue);
}

// --- Cancel ---

void RabbitApi1S::basicCancelImpl(CallContext& /*ctx*/) {
    checkConnection();
    clear();
    m_consumeParams = ConsumeParams{}; // отменённого потребителя не поднимаем
}

// --- Reconnect ---

void RabbitApi1S::reconnectImpl(CallContext& /*ctx*/) {
    if (!m_client) {
        throw std::runtime_error("Connection is not established! Use Connect() first");
    }
    restoreConnection();
}

void RabbitApi1S::restoreConnection() {
    // Каналы и потребитель умерли вместе с соединением: сбрасываем до
    // переподключения, иначе Consumer попытается закрыть мёртвый канал.
    const ConsumeParams params = m_consumeParams;
    clear();

    if (!m_client->reconnect()) {
        throw std::runtime_error(m_client->lastError().empty()
            ? "Reconnect failed" : m_client->lastError());
    }

    // Потребитель поднимается с прежними параметрами; тег брокер присвоит новый,
    // а неподтверждённые сообщения он вернёт в очередь сам.
    if (params.valid) {
        startConsumer(params);
        m_consumeParams = params;
    }
    m_reconnectCount.fetch_add(1, std::memory_order_relaxed);
}

// --- Автоматическое переподключение ---

void RabbitApi1S::startWatchdog() {
    if (m_watchdog.joinable()) return;
    m_watchdogStop.store(false, std::memory_order_release);
    m_watchdog = std::thread([this]() { watchdogLoop(); });
}

void RabbitApi1S::stopWatchdog() {
    if (!m_watchdog.joinable()) return;
    {
        std::lock_guard<std::mutex> lock(m_watchdogMutex);
        m_watchdogStop.store(true, std::memory_order_release);
    }
    m_watchdogCv.notify_all();
    m_watchdog.join();
}

void RabbitApi1S::watchdogLoop() {
    int delayMs = m_reconnectDelayMs;
    // Проверяем связь чаще, чем переподключаемся: обрыв надо заметить быстро,
    // а долбиться в недоступный брокер — незачем.
    const int kCheckIntervalMs = 500;

    auto sleepFor = [this](int ms) {
        std::unique_lock<std::mutex> lock(m_watchdogMutex);
        m_watchdogCv.wait_for(lock, std::chrono::milliseconds(ms),
            [this]() { return m_watchdogStop.load(std::memory_order_acquire); });
        return !m_watchdogStop.load(std::memory_order_acquire);
    };

    bool lossReported = false;
    while (sleepFor(kCheckIntervalMs)) {
        if (!m_autoReconnect.load(std::memory_order_acquire)) {
            delayMs = m_reconnectDelayMs;
            continue;
        }
        if (!m_client || m_client->isConnected()) {
            delayMs = m_reconnectDelayMs;
            lossReported = false;
            continue;
        }

        if (!lossReported) {
            BRMQ_LOG_WARN("Auto reconnect: connection lost (" + m_client->lastError() + ")");
            notifyConnectionEvent(u"ConnectionLost", m_client->lastError());
            lossReported = true;
        }

        // try_lock, а не lock: пока платформа занята своим вызовом, ждать её
        // нельзя — иначе сторож задержит поток 1С на время переподключения.
        std::unique_lock<std::mutex> lock(m_callMutex, std::try_to_lock);
        if (!lock.owns_lock()) continue;
        if (!m_client || m_client->isConnected()) continue;

        bool restored = false;
        std::string failure;
        try {
            restoreConnection();
            restored = true;
        } catch (const std::exception& e) {
            failure = e.what();
        }
        lock.unlock();

        if (restored) {
            delayMs = m_reconnectDelayMs;
            lossReported = false;
            const int count = m_reconnectCount.load(std::memory_order_relaxed);
            BRMQ_LOG_INFO("Auto reconnect: connection restored (attempt #"
                          + std::to_string(count) + ")");
            notifyConnectionEvent(u"Reconnected", std::to_string(count));
            continue;
        }

        {
            std::lock_guard<std::mutex> errLock(m_queueMutex);
            m_consumerError = "Auto reconnect failed: " + failure;
        }
        BRMQ_LOG_ERROR("Auto reconnect failed: " + failure + ", next try in "
                       + std::to_string(delayMs) + " ms");
        // Нарастающая пауза: брокер после сетевой аварии поднимается не сразу,
        // а попытка раз в полсекунды всю ночь — это лог на гигабайт.
        if (!sleepFor(delayMs)) break;
        delayMs = (delayMs * 2 < m_reconnectMaxDelayMs) ? delayMs * 2
                                                        : m_reconnectMaxDelayMs;
    }
}

void RabbitApi1S::notifyConnectionEvent(const char16_t* event, const std::string& detail) {
    if (!m_useExternalEvent || !m_addin) return;
    std::u16string payload = toU16(detail);
    m_addin->ExternalEvent(
        const_cast<WCHAR_T*>(reinterpret_cast<const WCHAR_T*>(u"BlackRabbitMQ")),
        const_cast<WCHAR_T*>(reinterpret_cast<const WCHAR_T*>(event)),
        const_cast<WCHAR_T*>(reinterpret_cast<const WCHAR_T*>(payload.c_str())));
}

// --- ExternalEvent ---

void RabbitApi1S::enableExternalEventImpl(CallContext& ctx) {
    bool enable = ctx.boolParam();
    enableExternalEvent(enable);

    // Буфера по умолчанию хватает на единичные уведомления, но не на поток:
    // при неограниченном prefetch брокер отдаёт очередь целиком, и события
    // начинают отбрасываться. Просим у платформы буфер побольше.
    if (enable && m_addin) {
        m_addin->SetEventBufferDepth(kEventBufferDepth);
    }
}

// --- Режим публикации ---

void RabbitApi1S::setPublishModeImpl(CallContext& ctx) {
    checkConnection();
    std::string mode = ctx.stringParamUtf8();
    for (auto& ch : mode) ch = static_cast<char>(::tolower(static_cast<unsigned char>(ch)));

    if (mode == "confirms" || mode.empty()) {
        m_client->setPublishMode(Channel::PublishMode::Confirms);
    } else if (mode == "transactions" || mode == "tx") {
        m_client->setPublishMode(Channel::PublishMode::Transactions);
    } else if (mode == "batch") {
        m_client->setPublishMode(Channel::PublishMode::Batch);
    } else {
        throw std::runtime_error("SetPublishMode: ожидается \"confirms\", \"transactions\" "
                                 "или \"batch\", получено: " + mode);
    }
    BRMQ_LOG_INFO("SetPublishMode: " + mode);
}

// --- Пакетная публикация ---

void RabbitApi1S::flushPublishImpl(CallContext& /*ctx*/) {
    checkConnection();
    m_client->flushPublish();
}

// --- Sleep ---

void RabbitApi1S::sleepNativeImpl(CallContext& ctx) {
    uint64_t amount = static_cast<uint64_t>(ctx.longParam());
    std::this_thread::sleep_for(std::chrono::milliseconds(amount));
}

// --- Properties ---

void RabbitApi1S::getRoutingKeyImpl(CallContext& ctx) {
    ctx.setStringResult(m_converter.from_bytes(m_lastMessage.routingKey));
}

void RabbitApi1S::getHeadersImpl(CallContext& ctx) {
    ctx.setStringResult(m_converter.from_bytes(lastMessageHeaders()));
}

void RabbitApi1S::setPriorityImpl(CallContext& ctx) {
    // Приоритет следующей публикации. Отдельно от m_lastMessage.priority:
    // иначе полученное сообщение затирало бы то, что выставила 1С для отправки.
    m_outgoingPriority = ctx.intParam();
}

void RabbitApi1S::getPriorityImpl(CallContext& ctx) {
    ctx.setIntResult(m_lastMessage.priority);
}

void RabbitApi1S::setMsgPropImpl(long propNum, CallContext& ctx) {
    std::string value = ctx.stringParamUtf8();
    switch (propNum) {
        case 1:  m_outgoingProps.correlationId = value;    break;
        case 2:  m_outgoingProps.typeName = value;         break;
        case 3:  m_outgoingProps.messageId = value;        break;
        case 4:  m_outgoingProps.appId = value;            break;
        case 5:  m_outgoingProps.contentEncoding = value;  break;
        case 6:  m_outgoingProps.contentType = value;      break;
        case 7:  m_outgoingProps.userId = value;           break;
        case 8:  m_outgoingProps.clusterId = value;        break;
        case 9:  m_outgoingProps.expiration = value;       break;
        case 10: m_outgoingProps.replyTo = value;          break;
    }
}

void RabbitApi1S::getMsgPropImpl(long propNum, CallContext& ctx) {
    const auto& props = m_lastMessage.props;
    std::string value;
    switch (propNum) {
        case 1:  value = props.correlationId;    break;
        case 2:  value = props.typeName;         break;
        case 3:  value = props.messageId;        break;
        case 4:  value = props.appId;            break;
        case 5:  value = props.contentEncoding;  break;
        case 6:  value = props.contentType;      break;
        case 7:  value = props.userId;           break;
        case 8:  value = props.clusterId;        break;
        case 9:  value = props.expiration;       break;
        case 10: value = props.replyTo;          break;
    }
    ctx.setStringOrEmptyResult(m_converter.from_bytes(value));
}

// --- Helpers ---

void RabbitApi1S::clear() {
    m_consumer.reset(nullptr);
    std::lock_guard<std::mutex> lock(m_queueMutex);
    m_consumerError.clear();
    std::queue<Message> empty;
    m_messageQueue.swap(empty);
    m_cvDataArrived.notify_all();
}

AMQP::Table RabbitApi1S::headersFromJson(const std::string& propsJson, bool /*forConsume*/) {
    AMQP::Table headers;
    if (propsJson.empty()) return headers;

    auto object = json::parse(propsJson);
    for (auto& it : object.items()) {
        auto& value = it.value();
        std::string name = it.key();
        if (value.is_boolean())
            headers.set(name, value.get<bool>());
        else if (value.is_number())
            headers.set(name, value.get<int64_t>());
        else if (value.is_string())
            headers.set(name, value.get<std::string>());
        else
            throw std::runtime_error("Unsupported json type for property " + name);
    }
    return headers;
}

std::string RabbitApi1S::lastMessageHeaders() {
    json hdr = json::object();
    AMQP::Table& tbl = m_lastMessage.headers;
    for (const std::string& key : tbl.keys()) {
        const AMQP::Field& field = tbl.get(key);
        if (field.isInteger())      hdr[key] = static_cast<int64_t>(field);
        else if (field.isDecimal()) hdr[key] = static_cast<double>(field);
        else if (field.isString())  hdr[key] = static_cast<const std::string&>(field);
        else if (field.isBoolean()) hdr[key] = static_cast<bool>(static_cast<int64_t>(field));
    }
    return hdr.dump();
}

} // namespace AddIn1S
} // namespace BlackRabbitMQ
