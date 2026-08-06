#include "Platform/LibeventTransport.h"

#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <stdexcept>

namespace BlackRabbitMQ {

bool LibeventHandler::onSecuring(AMQP::TcpConnection*, SSL* ssl) {
    if (!tls.verifyPeer) {
        // Явный отказ от проверки — режим для отладки и закрытых стендов.
        SSL_set_verify(ssl, SSL_VERIFY_NONE, nullptr);
        return true;
    }

    SSL_set_verify(ssl, SSL_VERIFY_PEER, nullptr);

    SSL_CTX* ctx = SSL_get_SSL_CTX(ssl);
    if (!tls.caFile.empty()) {
        // Самоподписанный брокер: доверяем ровно указанному корню.
        if (SSL_CTX_load_verify_locations(ctx, tls.caFile.c_str(), nullptr) != 1) {
            fail("TLS: cannot load CA file " + tls.caFile);
            return false;
        }
    } else if (SSL_CTX_set_default_verify_paths(ctx) != 1) {
        fail("TLS: cannot load system certificate store");
        return false;
    }

    // SNI: без него брокер с несколькими сертификатами отдаст не тот.
    SSL_set_tlsext_host_name(ssl, host.c_str());

    if (tls.verifyHostname) {
        // Цепочка сама по себе ничего не говорит о том, к тому ли серверу мы
        // подключились: валидный сертификат чужого хоста прошёл бы проверку.
        X509_VERIFY_PARAM* param = SSL_get0_param(ssl);
        X509_VERIFY_PARAM_set_hostflags(param, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
        if (X509_VERIFY_PARAM_set1_host(param, host.c_str(), 0) != 1) {
            fail("TLS: cannot set expected host name " + host);
            return false;
        }
    }
    return true;
}

uint16_t LibeventHandler::onNegotiate(AMQP::TcpConnection*, uint16_t interval) {
    // По умолчанию AMQP-CPP соглашается с предложением брокера, но кадры не
    // отправляет — соединение в простое брокер разрывал через два интервала.
    uint16_t chosen = interval;
    if (desiredHeartbeat == 0) {
        chosen = 0;                                   // выключено намеренно
    } else if (desiredHeartbeat > 0) {
        const uint16_t wanted = static_cast<uint16_t>(desiredHeartbeat);
        chosen = (interval > 0 && interval < wanted) ? interval : wanted;
    }
    heartbeatSec.store(chosen, std::memory_order_release);
    return chosen;
}

bool LibeventHandler::onSecured(AMQP::TcpConnection*, const SSL* ssl) {
    if (!tls.verifyPeer) return true;

    const long result = SSL_get_verify_result(const_cast<SSL*>(ssl));
    if (result != X509_V_OK) {
        fail(std::string("TLS certificate rejected: ")
             + X509_verify_cert_error_string(result));
        return false;
    }
    return true;
}

LibeventTransport::LibeventTransport() = default;

LibeventTransport::~LibeventTransport() {
    disconnect();
}

void LibeventTransport::connect(const AMQP::Address& address, int timeoutSec) {
    if (m_connected.load(std::memory_order_acquire)) return;

    setError("");
    m_timeoutSec = timeoutSec; // тем же лимитом ограничено открытие каналов

    // 1. EventLoop — владеет event_base
    m_eventLoop.reset(new EventLoop());

    // 2. Handler на event_base из EventLoop; ему же принадлежит политика TLS
    m_handler.reset(new LibeventHandler(m_eventLoop->base(), m_tls, address.hostname()));
    m_handler->desiredHeartbeat = m_heartbeat;

    // Heartbeat отправляем сами, по тику цикла: у AMQP-CPP своего таймера нет.
    m_lastHeartbeatSent = std::chrono::steady_clock::now();
    m_eventLoop->setTickCallback([this]() { onLoopTick(); });

    // 3. AMQP соединение
    m_amqpConn.reset(new AMQP::TcpConnection(m_handler.get(), address));

    // 4. Запустить event loop
    m_eventLoop->run();

    // 5. Ждать готовности
    waitForReady(timeoutSec);
    m_connected.store(true, std::memory_order_release);
}

void LibeventTransport::disconnect() {
    // Соединение уничтожается в потоке цикла — это последнее обращение
    // к AMQP-CPP, и оно не должно пересечься с разбором входящих кадров.
    if (m_amqpConn && m_eventLoop) {
        m_eventLoop->runInLoop([this]() { m_amqpConn.reset(nullptr); });
    }
    m_amqpConn.reset(nullptr);

    if (m_eventLoop) {
        m_eventLoop->stop();
        m_eventLoop.reset(nullptr);
    }
    m_handler.reset(nullptr);
    m_connected.store(false, std::memory_order_release);
}

std::unique_ptr<AMQP::Channel> LibeventTransport::createChannel() {
    if (!m_eventLoop) {
        throw std::runtime_error("Connection not usable");
    }

    // Состояние ожидания живёт в shared_ptr: при таймауте мы выходим отсюда,
    // а callback'и AMQP-CPP могут сработать позже — ссылаться на локальные
    // переменные этой функции им уже нельзя.
    struct OpenState {
        std::mutex mutex;
        std::condition_variable cv;
        bool done = false;
        std::string error;
    };
    auto state = std::make_shared<OpenState>();

    // Создание канала и привязка callback'ов — в потоке цикла и неделимо:
    // между ними брокер успевает прислать channel.open-ok, и без callback'а
    // готовность канала потерялась бы.
    std::unique_ptr<AMQP::TcpChannel> channel;
    m_eventLoop->runInLoop([&]() {
        if (!m_amqpConn || !m_amqpConn->usable()) {
            throw std::runtime_error("Connection not usable");
        }
        channel = std::make_unique<AMQP::TcpChannel>(m_amqpConn.get());

        // notify под мьютексом — иначе возможен потерянный сигнал.
        channel->onReady([state]() {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->done = true;
            state->cv.notify_all();
        });
        channel->onError([state](const char* msg) {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->error = msg ? msg : "channel error";
            state->done = true;
            state->cv.notify_all();
        });
    });

    const int timeoutMs = (m_timeoutSec > 0 ? m_timeoutSec : 30) * 1000;
    std::string failure;
    {
        std::unique_lock<std::mutex> lock(state->mutex);
        if (!state->cv.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                                [&state]() { return state->done; })) {
            failure = "Channel open: timeout " + std::to_string(timeoutMs)
                + " ms waiting for broker";
        } else if (!state->error.empty()) {
            failure = state->error;
        }
    }

    // Проверка состояния, перевешивание onError и — при неудаче — уничтожение
    // канала тоже идут в потоке цикла: канал принадлежит ему.
    bool usable = false;
    m_eventLoop->runInLoop([&]() {
        usable = failure.empty() && channel && channel->usable();
        if (!usable) {
            channel.reset();
            return;
        }
        // Дальше ошибки канала только фиксируем: синхронно их уже никто не ждёт.
        channel->onError([this](const char* msg) { if (msg) setError(msg); });
    });

    if (!failure.empty()) {
        setError(failure);
        throw std::runtime_error(failure);
    }
    if (!usable) {
        throw std::runtime_error(error().empty() ? "Channel not opened" : error());
    }

    return channel;
}

void LibeventTransport::onLoopTick() {
    // Исполняется в потоке цикла — там же, где живёт TcpConnection.
    if (!m_amqpConn || !m_handler) return;

    const uint16_t interval = m_handler->heartbeatSec.load(std::memory_order_acquire);
    if (interval == 0) return;

    // Половина интервала: у брокера остаётся запас на потерю одного кадра.
    const auto period = std::chrono::milliseconds(interval * 500);
    const auto now = std::chrono::steady_clock::now();
    if (now - m_lastHeartbeatSent < period) return;

    m_lastHeartbeatSent = now;
    m_amqpConn->heartbeat();
}

void LibeventTransport::waitForReady(int timeoutSec) {
    // Событийное ожидание вместо опроса ready()/closed() из потока 1С:
    // состояние AMQP::TcpConnection принадлежит потоку event loop, читать его
    // снаружи — гонка (подтверждено ThreadSanitizer). Здесь ждём callback.
    std::string failure;
    {
        std::unique_lock<std::mutex> lock(m_handler->mutex);
        const bool signalled = m_handler->cv.wait_for(
            lock, std::chrono::seconds(timeoutSec > 0 ? timeoutSec : 30),
            [this]() { return m_handler->ready || m_handler->finished; });

        if (!signalled) {
            failure = m_handler->error.empty() ? "Connection timeout" : m_handler->error;
        } else if (!m_handler->ready) {
            failure = m_handler->error.empty() ? "Connection failed" : m_handler->error;
        }
    }

    if (!failure.empty()) {
        // disconnect() гасит поток цикла — вызывать под мьютексом обработчика нельзя.
        setError(failure);
        disconnect();
        throw std::runtime_error(failure);
    }
}

} // namespace BlackRabbitMQ
