#include "Channel.h"
#include "Message.h"

#include <chrono>
#include <stdexcept>

namespace BlackRabbitMQ {

Channel::Channel(std::unique_ptr<AMQP::Channel> ch, ITaskRunner& runner)
    : m_runner(runner)
    , m_channel(std::move(ch))
{
    if (!m_channel) {
        throw std::runtime_error("Channel: null Channel");
    }
}

Channel::~Channel() {
    // Уничтожение канала — тоже обращение к AMQP-CPP: закрытие дёргает
    // callback'и и трогает состояние соединения.
    if (!m_channel) return;
    m_runner.runInLoop([this]() { m_channel.reset(); });
}

bool Channel::usable() const {
    if (!m_channel) return false;
    bool result = false;
    m_runner.runInLoop([this, &result]() { result = m_channel && m_channel->usable(); });
    return result;
}

// --- Private: синхронизация ---

uint64_t Channel::beginOp() {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_ready = false;
    m_error.clear();
    m_consumerTag.clear();
    return ++m_seq;
}

void Channel::wait(uint64_t seq, const char* operation) {
    std::unique_lock<std::mutex> lock(m_mutex);
    if (!m_cv.wait_for(lock, std::chrono::milliseconds(m_timeoutMs),
                       [this, seq]() { return m_ready && m_seq == seq; })) {
        // Ответ не пришёл. Сдвигаем номер прямо здесь: опоздавший callback
        // не совпадёт по номеру и будет отброшен, а не засчитан следующей операции.
        ++m_seq;
        throw std::runtime_error(std::string(operation)
            + ": timeout " + std::to_string(m_timeoutMs) + " ms waiting for broker");
    }
    m_ready = false;
    if (!m_error.empty()) {
        std::string err = std::move(m_error);
        m_error.clear();
        throw std::runtime_error(err);
    }
}

void Channel::signalSuccess(uint64_t seq) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (seq != m_seq) return; // ответ на уже отменённую (протухшую) операцию
    m_ready = true;
    m_cv.notify_all();
}

void Channel::signalError(uint64_t seq, const char* message) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (seq != m_seq) return;
    m_error = message ? message : "unknown channel error";
    m_ready = true;
    m_cv.notify_all();
}

// --- Exchange ---

void Channel::declareExchange(
    const std::string& name,
    AMQP::ExchangeType type,
    int flags,
    const AMQP::Table& args)
{
    const uint64_t seq = beginOp();
    m_runner.runInLoop([&]() {
        m_channel->declareExchange(name, type, flags, args)
            .onSuccess([this, seq]() { signalSuccess(seq); })
            .onError([this, seq](const char* msg) { signalError(seq, msg); });
    });
    wait(seq, "declareExchange");
}

void Channel::removeExchange(const std::string& name, int flags) {
    const uint64_t seq = beginOp();
    m_runner.runInLoop([&]() {
        m_channel->removeExchange(name, flags)
            .onSuccess([this, seq]() { signalSuccess(seq); })
            .onError([this, seq](const char* msg) { signalError(seq, msg); });
    });
    wait(seq, "removeExchange");
}

// --- Queue ---

Channel::QueueStats Channel::declareQueue(
    const std::string& name,
    int flags,
    const AMQP::Table& args)
{
    const uint64_t seq = beginOp();
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_queueStats = QueueStats{};
    }
    m_runner.runInLoop([&]() {
        m_channel->declareQueue(name, flags, args)
            .onSuccess([this, seq](const std::string&, uint32_t messages, uint32_t consumers) {
                {
                    std::lock_guard<std::mutex> lock(m_mutex);
                    m_queueStats.messageCount = messages;
                    m_queueStats.consumerCount = consumers;
                }
                signalSuccess(seq);
            })
            .onError([this, seq](const char* msg) { signalError(seq, msg); });
    });
    wait(seq, "declareQueue");

    std::lock_guard<std::mutex> lock(m_mutex);
    return m_queueStats;
}

uint32_t Channel::purgeQueue(const std::string& name) {
    const uint64_t seq = beginOp();
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_purgedCount = 0;
    }
    m_runner.runInLoop([&]() {
        m_channel->purgeQueue(name)
            .onSuccess([this, seq](uint32_t messages) {
                {
                    std::lock_guard<std::mutex> lock(m_mutex);
                    m_purgedCount = messages;
                }
                signalSuccess(seq);
            })
            .onError([this, seq](const char* msg) { signalError(seq, msg); });
    });
    wait(seq, "purgeQueue");

    std::lock_guard<std::mutex> lock(m_mutex);
    return m_purgedCount;
}

void Channel::removeQueue(const std::string& name, int flags) {
    const uint64_t seq = beginOp();
    m_runner.runInLoop([&]() {
        m_channel->removeQueue(name, flags)
            .onSuccess([this, seq]() { signalSuccess(seq); })
            .onError([this, seq](const char* msg) { signalError(seq, msg); });
    });
    wait(seq, "removeQueue");
}

// --- Binding ---

void Channel::bindQueue(
    const std::string& exchange,
    const std::string& queue,
    const std::string& routingKey,
    const AMQP::Table& args)
{
    const uint64_t seq = beginOp();
    m_runner.runInLoop([&]() {
        m_channel->bindQueue(exchange, queue, routingKey, args)
            .onSuccess([this, seq]() { signalSuccess(seq); })
            .onError([this, seq](const char* msg) { signalError(seq, msg); });
    });
    wait(seq, "bindQueue");
}

void Channel::unbindQueue(
    const std::string& exchange,
    const std::string& queue,
    const std::string& routingKey)
{
    const uint64_t seq = beginOp();
    m_runner.runInLoop([&]() {
        m_channel->unbindQueue(exchange, queue, routingKey)
            .onSuccess([this, seq]() { signalSuccess(seq); })
            .onError([this, seq](const char* msg) { signalError(seq, msg); });
    });
    wait(seq, "unbindQueue");
}

// --- Publish ---

// Подтверждения вместо транзакций: гарантия та же (брокер отвечает, только
// записав сообщение в очередь), но без пары лишних round-trip на tx.select
// и tx.commit для каждого сообщения.
// Подписка на возвраты нужна обоим режимам публикации: непроходное сообщение
// брокер отдаёт назад одинаково, разница только в способе подтверждения.
void Channel::ensureReturnHandler() {
    if (m_returnHandlerSet) return;
    m_returnHandlerSet = true;

    m_channel->recall()
        .onReturned([this](const AMQP::Message& msg, int16_t code, const std::string& description) {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_returnedReason = "publish: сообщение не доставлено ни в одну очередь ("
                + std::to_string(code) + " " + description
                + ", exchange '" + msg.exchange()
                + "', routing key '" + msg.routingkey() + "')";
        });
}

void Channel::ensureConfirms() {
    ensureReturnHandler();
    if (m_confirmsEnabled) return;
    m_confirmsEnabled = true;

    m_channel->confirmSelect()
        .onAck([this](uint64_t deliveryTag, bool multiple) {
            uint64_t op = 0;
            std::string returned;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                // В пакетном режиме подтверждения не относятся к конкретной
                // операции: они двигают границу подтверждённого, а ждёт её
                // flushPublish().
                if (m_publishMode == PublishMode::Batch) {
                    if (!m_returnedReason.empty() && m_batchError.empty()) {
                        m_batchError = std::move(m_returnedReason);
                        m_returnedReason.clear();
                    }
                    markConfirmed(deliveryTag, multiple);
                    m_cv.notify_all();
                    return;
                }
                op = m_pendingPublishOp;
                returned = std::move(m_returnedReason);
                m_returnedReason.clear();
            }
            if (!op) return;
            // Непроходное сообщение брокер сначала возвращает, и только потом
            // подтверждает приём. Подтверждение здесь означало бы «отправлено
            // в никуда», поэтому отдаём ошибку с причиной возврата.
            if (!returned.empty()) {
                signalError(op, returned.c_str());
            } else {
                signalSuccess(op);
            }
        })
        .onNack([this](uint64_t deliveryTag, bool multiple, bool /*requeue*/) {
            uint64_t op = 0;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_publishMode == PublishMode::Batch) {
                    // Отказ по одному сообщению делает недоверенным весь пакет:
                    // какое именно потеряно, вызывающий из 1С всё равно не
                    // восстановит, поэтому честнее сообщить об ошибке пакета.
                    if (m_batchError.empty()) {
                        m_batchError = "publish: брокер не принял сообщение (nack), номер "
                            + std::to_string(deliveryTag);
                    }
                    markConfirmed(deliveryTag, multiple);
                    m_cv.notify_all();
                    return;
                }
                op = m_pendingPublishOp;
            }
            if (op) {
                signalError(op, "publish: брокер не принял сообщение (nack)");
            }
        });

}

void Channel::markConfirmed(uint64_t deliveryTag, bool multiple) {
    if (multiple) {
        if (deliveryTag > m_confirmedNo) m_confirmedNo = deliveryTag;
    } else if (deliveryTag == m_confirmedNo + 1) {
        m_confirmedNo = deliveryTag;
    } else {
        m_ackedOutOfOrder.insert(deliveryTag);
    }

    // Подтянуть границу, если ранее пришли подтверждения не по порядку.
    auto it = m_ackedOutOfOrder.begin();
    while (it != m_ackedOutOfOrder.end() && *it == m_confirmedNo + 1) {
        m_confirmedNo = *it;
        it = m_ackedOutOfOrder.erase(it);
    }
    // Всё, что уже покрыто границей, хранить незачем.
    m_ackedOutOfOrder.erase(m_ackedOutOfOrder.begin(),
                            m_ackedOutOfOrder.upper_bound(m_confirmedNo));
}

void Channel::waitConfirms(const char* operation) {
    std::unique_lock<std::mutex> lock(m_mutex);
    if (m_confirmedNo >= m_publishedNo && m_batchError.empty()) return;

    const bool signalled = m_cv.wait_for(
        lock, std::chrono::milliseconds(m_timeoutMs),
        [this]() { return m_confirmedNo >= m_publishedNo || !m_batchError.empty(); });

    if (!m_batchError.empty()) {
        const std::string error = std::move(m_batchError);
        m_batchError.clear();
        // Пакет оборван: дальше считаем с чистого листа, иначе одна ошибка
        // будет всплывать на каждой следующей публикации.
        m_publishedNo = 0;
        m_confirmedNo = 0;
        m_ackedOutOfOrder.clear();
        throw std::runtime_error(error);
    }
    if (!signalled) {
        const uint64_t pending = m_publishedNo - m_confirmedNo;
        throw std::runtime_error(std::string(operation) + ": timeout "
            + std::to_string(m_timeoutMs) + " ms, не подтверждено "
            + std::to_string(pending) + " сообщений");
    }
}

void Channel::flushPublish() {
    if (m_publishMode != PublishMode::Batch) return;
    waitConfirms("publish flush");
}

void Channel::publish(
    const std::string& exchange,
    const std::string& routingKey,
    const AMQP::Envelope& envelope,
    bool mandatory)
{
    if (m_publishMode == PublishMode::Batch) {
        bool accepted = true;
        m_runner.runInLoop([&]() {
            ensureConfirms();
            accepted = m_channel->publish(exchange, routingKey, envelope,
                                          mandatory ? AMQP::mandatory : 0);
            if (accepted) {
                std::lock_guard<std::mutex> lock(m_mutex);
                ++m_publishedNo;
            }
        });
        if (!accepted) {
            throw std::runtime_error("publish: канал не принял сообщение");
        }

        // Окно заполнено — ждём подтверждений, иначе неподтверждённое копится
        // без предела и на стороне брокера, и в памяти процесса 1С.
        bool windowFull = false;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            windowFull = m_batchSize > 0 && (m_publishedNo - m_confirmedNo) >= m_batchSize;
        }
        if (windowFull) waitConfirms("publish batch");
        return;
    }

    const uint64_t seq = beginOp();
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_pendingPublishOp = seq;
        m_returnedReason.clear();
    }

    m_runner.runInLoop([&]() {
        const int flags = mandatory ? AMQP::mandatory : 0;

        if (m_publishMode == PublishMode::Transactions) {
            // Стандарт AMQP 0.9.1: подтверждением служит tx.commit-ok.
            // Возврат непроходного сообщения приходит до коммита, поэтому
            // причина возврата проверяется в обработчике успеха.
            ensureReturnHandler();
            m_channel->startTransaction();
            m_channel->publish(exchange, routingKey, envelope, flags);
            m_channel->commitTransaction()
                .onSuccess([this, seq]() {
                    std::string returned;
                    {
                        std::lock_guard<std::mutex> lock(m_mutex);
                        returned = std::move(m_returnedReason);
                        m_returnedReason.clear();
                    }
                    if (returned.empty()) signalSuccess(seq);
                    else signalError(seq, returned.c_str());
                })
                .onError([this, seq](const char* msg) { signalError(seq, msg); });
            return;
        }

        ensureConfirms();
        if (!m_channel->publish(exchange, routingKey, envelope, flags)) {
            signalError(seq, "publish: канал не принял сообщение");
        }
    });

    try {
        wait(seq, "publish");
    } catch (...) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_pendingPublishOp = 0;
        throw;
    }

    std::lock_guard<std::mutex> lock(m_mutex);
    m_pendingPublishOp = 0;
}

// --- QoS ---

void Channel::setQos(uint16_t prefetchCount) {
    const uint64_t seq = beginOp();
    m_runner.runInLoop([&]() {
        m_channel->setQos(prefetchCount)
            .onSuccess([this, seq]() { signalSuccess(seq); })
            .onError([this, seq](const char* msg) { signalError(seq, msg); });
    });
    wait(seq, "setQos");
}

// --- Consume ---

std::string Channel::consume(
    const std::string& queue,
    const std::string& consumerTag,
    int flags,
    const AMQP::Table& args,
    std::function<void(const Message&, uint64_t, bool)> onMessage,
    std::function<void(const std::string&)> onCancelled)
{
    const uint64_t seq = beginOp();
    m_runner.runInLoop([&]() {
    m_channel->consume(queue, consumerTag, flags, args)
        // ConsumeCallback: брокер присылает присвоенный тег потребителя
        .onSuccess([this, seq](const std::string& tag) {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_seq == seq) m_consumerTag = tag;
            }
            signalSuccess(seq);
        })
        .onMessage([onMessage = std::move(onMessage)](
            const AMQP::Message& msg, uint64_t deliveryTag, bool redelivered) {
            onMessage(Message::from(msg, deliveryTag, redelivered), deliveryTag, redelivered);
        })
        .onCancelled([onCancelled = std::move(onCancelled)](const std::string& tag) {
            if (onCancelled) onCancelled(tag);
        })
        .onError([this, seq](const char* msg) { signalError(seq, msg); });
    });
    wait(seq, "consume");

    std::lock_guard<std::mutex> lock(m_mutex);
    return m_consumerTag;
}

// --- Ack / Reject ---

// Ответа брокер не присылает, но кадр всё равно пишется в соединение —
// из потока цикла. Вызов из onMessage исполнится на месте (см. ITaskRunner).

void Channel::cancelConsumer(const std::string& consumerTag) {
    if (consumerTag.empty()) return;
    const uint64_t seq = beginOp();
    m_runner.runInLoop([&]() {
        m_channel->cancel(consumerTag)
            .onSuccess([this, seq](const std::string&) { signalSuccess(seq); })
            .onError([this, seq](const char* msg) { signalError(seq, msg); });
    });
    wait(seq, "cancelConsumer");
}

void Channel::ack(uint64_t deliveryTag, bool multiple) {
    const int flags = multiple ? AMQP::multiple : 0;

    if (m_runner.inLoopThread()) {
        m_channel->ack(deliveryTag, flags);
        return;
    }

    // Подтверждение не ждёт ответа брокера, поэтому не ждём и поток цикла:
    // синхронный прыжок между потоками на каждое сообщение стоил дороже
    // самой отправки кадра и упирал приём в тысячу сообщений в секунду.
    // Задачи исполняются в порядке постановки, а деструктор канала идёт
    // тем же путём — значит все поставленные ack успеют выполниться
    // до уничтожения канала.
    m_runner.post([this, deliveryTag, flags]() {
        if (m_channel) m_channel->ack(deliveryTag, flags);
    });
}

void Channel::reject(uint64_t deliveryTag, bool requeue) {
    m_runner.runInLoop([&]() {
        m_channel->reject(deliveryTag, requeue ? AMQP::requeue : 0);
    });
}

} // namespace BlackRabbitMQ
