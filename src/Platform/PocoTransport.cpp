#include "Platform/PocoTransport.h"

#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/DatagramSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/PollSet.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/AcceptCertificateHandler.h>
#include <Poco/Net/NetException.h>
#include <Poco/Timespan.h>

#include <chrono>
#include <thread>
#include <cstring>
#include <stdexcept>

namespace BlackRabbitMQ {

namespace {
// Метку ставит сам поток цикла. Прежняя проверка сравнивала std::thread::id,
// который писал поток цикла, а читал поток 1С — то есть была гонкой; на
// Windows-пути её некому поймать, ThreadSanitizer гоняется только на Linux.
thread_local const PocoTransport* t_loopOwner = nullptr;
} // namespace

// --- Buffer ---

size_t PocoTransport::Buffer::write(const char* src, size_t sz) {
    if (used == data.size()) return 0;
    size_t writeSz = sz;
    if (used + sz > data.size()) writeSz = data.size() - used;
    std::memcpy(data.data() + used, src, writeSz);
    used += writeSz;
    return writeSz;
}

void PocoTransport::Buffer::shift(size_t count) {
    if (count >= used) { used = 0; return; }
    size_t diff = used - count;
    std::memmove(data.data(), data.data() + count, diff);
    used = diff;
}

bool PocoTransport::Buffer::ensureSpace(size_t need, size_t limit) {
    if (data.size() - used >= need) return true;
    if (used + need > limit) return false;
    size_t grown = data.size() ? data.size() * 2 : need;
    while (grown - used < need) grown *= 2;
    if (grown > limit) grown = limit;
    data.resize(grown, 0);
    return data.size() - used >= need;
}

// --- Constructor / Destructor ---

PocoTransport::PocoTransport()
    : m_port(0), m_ssl(false)
{
}

PocoTransport::~PocoTransport() {
    disconnect();
}

// --- ITransport ---

void PocoTransport::connect(const AMQP::Address& address, int timeoutSec) {
    m_host = address.hostname();
    m_port = address.port();
    m_ssl = address.secure();

    Poco::Net::initializeSSL();

    Poco::Net::SocketAddress sockAddr(m_host, m_port);

    if (m_ssl) {
        auto* sslSocket = new Poco::Net::SecureStreamSocket();
        sslSocket->setPeerHostName(m_host);
        sslSocket->setLazyHandshake(true);
        m_socket.reset(sslSocket);
    } else {
        m_socket.reset(new Poco::Net::StreamSocket());
    }

    m_socket->connect(sockAddr);
    m_socket->setBlocking(true);
    // Без TCP_NODELAY кадры AMQP копятся в буфере Nagle до delayed ACK: замер
    // в CI дал 46.9 мс на сообщение (10 000 сообщений — 469 с против 6 с на
    // Linux). На Linux этот флаг ставит сам AMQP-CPP (tcpresolver.h).
    m_socket->setNoDelay(true);
    m_socket->setSendBufferSize(static_cast<int>(TEMP_BUFFER_SIZE));
    m_socket->setReceiveBufferSize(static_cast<int>(TEMP_BUFFER_SIZE));
    m_socket->setKeepAlive(true);

    m_inBuf.reset(new Buffer(BUFFER_SIZE));
    m_outBuf.reset(new Buffer(BUFFER_SIZE));
    m_tmpBuf.resize(TEMP_BUFFER_SIZE);

    // Канал пробуждения: датаграмма на loopback выводит цикл из poll сразу,
    // как только появилась задача (аналог self-pipe в EventLoop на Linux).
    // Порт 0 — ядро выберет свободный; типы указаны явно, иначе перегрузки
    // SocketAddress неоднозначны (литеральный 0 подходит и под UInt16, и под строку).
    const Poco::Net::SocketAddress wakeBind(std::string("127.0.0.1"),
                                            static_cast<Poco::UInt16>(0));
    m_wakeIn.reset(new Poco::Net::DatagramSocket(wakeBind, /*reuseAddress*/ false));
    m_wakeIn->setBlocking(false);
    m_wakeAddr.reset(new Poco::Net::SocketAddress(m_wakeIn->address()));
    m_wakeOut.reset(new Poco::Net::DatagramSocket(Poco::Net::SocketAddress::IPv4));

    m_pollSet.reset(new Poco::Net::PollSet());
    m_pollSet->add(*m_socket, Poco::Net::PollSet::POLL_READ);
    m_pollSet->add(*m_wakeIn, Poco::Net::PollSet::POLL_READ);

    m_amqpConn.reset(new AMQP::Connection(this, address.login(), address.vhost()));

    startLoop(m_host, m_port, m_ssl);
    waitForReady(timeoutSec);
}

void PocoTransport::waitForReady(int timeoutSec) {
    const auto deadline = std::chrono::steady_clock::now()
        + std::chrono::seconds(timeoutSec > 0 ? timeoutSec : 30);

    for (;;) {
        // Состояние соединения читаем в потоке цикла — оно принадлежит ему.
        bool ready = false;
        bool usable = false;
        runInLoop([&]() {
            ready = m_amqpConn && m_amqpConn->ready();
            usable = m_amqpConn && m_amqpConn->usable();
        });
        if (ready) break;

        if (std::chrono::steady_clock::now() > deadline) {
            std::string err = error();
            disconnect();
            throw std::runtime_error(err.empty() ? "Connection timeout" : err);
        }
        // Соединение закрылось до готовности — дальше ждать нечего.
        // (у AMQP::Connection нет closed(), в отличие от TcpConnection)
        if (!usable) {
            std::string err = error();
            disconnect();
            throw std::runtime_error(err.empty() ? "Connection closed by broker" : err);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    m_closed.store(false, std::memory_order_release);
}

void PocoTransport::disconnect() {
    if (m_amqpConn && m_thread) {
        runInLoop([this]() { m_amqpConn.reset(nullptr); });
    }
    stopLoop();
    m_amqpConn.reset(nullptr);
    m_inBuf.reset(nullptr);
    m_outBuf.reset(nullptr);
    m_pollSet.reset(nullptr);
    if (m_socket) {
        try { m_socket->close(); } catch (...) {}
        m_socket.reset(nullptr);
    }
    {
        std::lock_guard<std::mutex> lock(m_wakeMutex);
        m_wakeOut.reset(nullptr);
        m_wakeAddr.reset(nullptr);
    }
    m_wakeIn.reset(nullptr);
    m_closed.store(true, std::memory_order_release);
}

std::unique_ptr<AMQP::Channel> PocoTransport::createChannel() {
    // Как и на Linux: объект AMQP-CPP создаётся в потоке, который разбирает кадры.
    std::unique_ptr<AMQP::Channel> channel;
    runInLoop([&]() {
        if (!m_amqpConn) throw std::runtime_error("Not connected");
        channel = std::make_unique<AMQP::Channel>(m_amqpConn.get());
    });
    return channel;
}

// --- Loop ---

void PocoTransport::startLoop(const std::string& /*host*/, uint16_t /*port*/, bool /*ssl*/) {
    m_stop.store(false, std::memory_order_release);
    m_thread.reset(new std::thread(loopThread, this));
}

void PocoTransport::stopLoop() {
    m_stop.store(true, std::memory_order_release);
    wake(); // выдернуть цикл из poll, не дожидаясь таймаута
    if (m_thread && m_thread->joinable()) {
        m_thread->join();
    }
    m_thread.reset(nullptr);
    drainTasks(); // задачи, поставленные перед остановкой, не должны зависнуть
}

void PocoTransport::wake() {
    std::lock_guard<std::mutex> lock(m_wakeMutex);
    if (!m_wakeOut || !m_wakeAddr) return;
    const char byte = 'x';
    try { m_wakeOut->sendTo(&byte, 1, *m_wakeAddr); } catch (...) {}
}

void PocoTransport::drainTasks() {
    std::vector<std::function<void()>> tasks;
    {
        std::lock_guard<std::mutex> lock(m_taskMutex);
        tasks.swap(m_tasks);
    }
    for (auto& task : tasks) task();
}

bool PocoTransport::inLoopThread() const noexcept {
    return t_loopOwner == this;
}

void PocoTransport::loopThread(PocoTransport* self) {
    t_loopOwner = self;
    while (!self->m_stop.load(std::memory_order_acquire)) {
        try {
            self->loopIteration();
        } catch (const Poco::Net::ConnectionResetException& e) {
            self->setError(e.displayText());
            if (self->m_amqpConn) self->m_amqpConn->close();
        } catch (const Poco::Exception& e) {
            self->setError(std::string(e.what()) + ": " + e.displayText());
        } catch (const std::exception& e) {
            self->setError(e.what());
        }
    }
    // Поток завершается — метку снимаем, иначе повторно созданный транспорт
    // с тем же адресом принял бы чужой поток за свой.
    t_loopOwner = nullptr;
}

void PocoTransport::loopIteration() {
    drainTasks();
    if (!m_socket || !m_pollSet) return;

    // Спим до события: данные от брокера или пробуждение из post().
    // Таймаут — только страховка и точка проверки флага остановки.
    const Poco::Timespan pollTimeout(0, POLL_TIMEOUT_MS * 1000);
    const Poco::Net::PollSet::SocketModeMap ready = m_pollSet->poll(pollTimeout);

    bool dataFromBroker = false;
    for (const auto& entry : ready) {
        if (m_wakeIn && entry.first == *m_wakeIn) {
            char drain[64];
            try {
                while (m_wakeIn->available() > 0) m_wakeIn->receiveBytes(drain, sizeof(drain));
            } catch (...) {}
        } else {
            dataFromBroker = true;
        }
    }

    // Задачи, поставленные пока цикл спал.
    drainTasks();

    if (dataFromBroker) {
        int expected = m_amqpConn ? m_amqpConn->expected() : 4;
        if (expected <= 0) expected = 4;
        while (expected > 0) {
            if (m_tmpBuf.size() < static_cast<size_t>(expected))
                m_tmpBuf.resize(expected, 0);
            int received = m_socket->receiveBytes(m_tmpBuf.data(), expected);
            if (received <= 0) break;

            // Buffer::write при заполненном буфере пишет меньше запрошенного.
            // Раньше результат не проверялся: хвост пропадал, и разбор кадров
            // ломался — на плотном входящем потоке соединение приходилось
            // поднимать заново. Сначала отдаём накопленное парсеру (он освободит
            // место), при необходимости растим буфер до MAX_BUFFER_SIZE.
            const size_t total = static_cast<size_t>(received);
            size_t written = m_inBuf->write(m_tmpBuf.data(), total);
            if (written < total) {
                parseInBuffer();
                written += m_inBuf->write(m_tmpBuf.data() + written, total - written);
            }
            if (written < total) {
                const size_t rest = total - written;
                if (!m_inBuf->ensureSpace(rest, MAX_BUFFER_SIZE)) {
                    setError("Receive buffer overflow: frame does not fit into "
                             + std::to_string(MAX_BUFFER_SIZE) + " bytes");
                    if (m_amqpConn) m_amqpConn->close();
                    break;
                }
                written += m_inBuf->write(m_tmpBuf.data() + written, rest);
            }
            expected = m_socket->available();
        }
    }

    parseInBuffer();
    sendDataFromBuffer();
}

void PocoTransport::parseInBuffer() {
    if (!m_amqpConn || m_inBuf->available() == 0) return;
    const size_t parsed = m_amqpConn->parse(m_inBuf->ptr(), m_inBuf->available());
    if (parsed == m_inBuf->available()) m_inBuf->drain();
    else if (parsed > 0) m_inBuf->shift(parsed);
}

void PocoTransport::sendDataFromBuffer() {
    if (!m_socket || !m_outBuf) return;
    // sendBytes может принять меньше запрошенного. Раньше буфер очищался
    // целиком независимо от результата — неотправленный хвост исчезал, и до
    // брокера уходил обрезанный кадр. Сдвигаем ровно на отданное.
    while (m_outBuf->available() > 0) {
        const int sent = m_socket->sendBytes(m_outBuf->ptr(),
                                             static_cast<int>(m_outBuf->available()));
        if (sent <= 0) break; // сокет закрыт или временно не принимает
        m_outBuf->shift(static_cast<size_t>(sent));
    }
}

// --- AMQP::ConnectionHandler ---

void PocoTransport::onData(AMQP::Connection*, const char* data, size_t size) {
    size_t written = m_outBuf->write(data, size);
    while (written < size) {
        const size_t before = m_outBuf->available();
        sendDataFromBuffer();
        // Сокет ничего не принял (закрыт или переполнен): без роста буфера
        // цикл крутился бы вечно, а без буфера — потерялся бы хвост кадра.
        if (m_outBuf->available() == before
            && !m_outBuf->ensureSpace(size - written, MAX_BUFFER_SIZE)) {
            setError("Send buffer overflow: broker is not reading and the frame "
                     "does not fit into " + std::to_string(MAX_BUFFER_SIZE) + " bytes");
            return;
        }
        written += m_outBuf->write(data + written, size - written);
    }
}
void PocoTransport::onReady(AMQP::Connection*) {
    m_closed.store(false, std::memory_order_release);
}
void PocoTransport::onError(AMQP::Connection*, const char* message) {
    if (message) setError(message);
}
void PocoTransport::onClosed(AMQP::Connection*) {
    m_closed.store(true, std::memory_order_release);
}
uint16_t PocoTransport::onNegotiate(AMQP::Connection*, uint16_t interval) {
    return interval;
}

} // namespace BlackRabbitMQ
