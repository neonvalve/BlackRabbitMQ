#include "Logger.h"

#include <chrono>
#include <cstdio>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <thread>

namespace BlackRabbitMQ {

namespace {

// Локальное время с миллисекундами: инцидент у клиента сопоставляют
// с журналом регистрации 1С, а он ведётся в локальном времени.
std::string timestamp() {
    using namespace std::chrono;
    const auto now = system_clock::now();
    const auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
    const std::time_t seconds = system_clock::to_time_t(now);

    std::tm tm{};
#if defined(_WIN32) || defined(_WIN64)
    localtime_s(&tm, &seconds);
#else
    localtime_r(&seconds, &tm);
#endif

    std::ostringstream out;
    out << std::put_time(&tm, "%Y-%m-%d %H:%M:%S")
        << '.' << std::setfill('0') << std::setw(3) << ms.count();
    return out.str();
}

std::string threadId() {
    std::ostringstream out;
    out << std::this_thread::get_id();
    return out.str();
}

} // namespace

Logger& Logger::instance() {
    static Logger logger;
    return logger;
}

Logger::~Logger() {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_file.is_open()) m_file.close();
}

void Logger::configure(const std::string& path, LogLevel level, std::size_t maxBytes) {
    std::lock_guard<std::mutex> lock(m_mutex);

    if (m_file.is_open()) m_file.close();
    m_path = path;
    m_maxBytes = maxBytes > 0 ? maxBytes : (5 * 1024 * 1024);
    m_written = 0;

    if (path.empty()) {
        m_level.store(LogLevel::Off, std::memory_order_release);
        return;
    }

    // Дописываем в конец: перезапуск сеанса 1С не должен стирать историю
    // предыдущего инцидента.
    m_file.open(path, std::ios::out | std::ios::app | std::ios::binary);
    if (!m_file.is_open()) {
        // Недоступный путь не должен ронять компоненту: журнал — вспомогательная
        // вещь, обмен важнее. Просто остаёмся выключенными.
        m_level.store(LogLevel::Off, std::memory_order_release);
        m_path.clear();
        return;
    }
    m_written = static_cast<std::size_t>(m_file.tellp());
    m_level.store(level, std::memory_order_release);
}

std::string Logger::path() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_path;
}

void Logger::rotateIfNeeded() {
    if (m_written < m_maxBytes || m_path.empty()) return;

    m_file.close();
    const std::string backup = m_path + ".1";
    std::remove(backup.c_str());
    std::rename(m_path.c_str(), backup.c_str());

    m_file.open(m_path, std::ios::out | std::ios::trunc | std::ios::binary);
    m_written = 0;
    if (!m_file.is_open()) {
        m_level.store(LogLevel::Off, std::memory_order_release);
        m_path.clear();
    }
}

void Logger::write(LogLevel level, const std::string& message) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_file.is_open()) return;

    rotateIfNeeded();
    if (!m_file.is_open()) return;

    std::string line = timestamp();
    line += " [";
    line += levelToString(level);
    line += "] [";
    line += threadId();
    line += "] ";
    line += message;
    line += '\n';

    m_file << line;
    // Сброс на диск сразу: журнал нужен именно тогда, когда процесс 1С падает,
    // а буферизованный хвост в этом случае теряется.
    m_file.flush();
    m_written += line.size();
}

LogLevel Logger::levelFromString(const std::string& text) {
    if (text == "error") return LogLevel::Error;
    if (text == "warn" || text == "warning") return LogLevel::Warn;
    if (text == "info") return LogLevel::Info;
    if (text == "debug") return LogLevel::Debug;
    return LogLevel::Off;
}

const char* Logger::levelToString(LogLevel level) {
    switch (level) {
        case LogLevel::Error: return "ERR";
        case LogLevel::Warn:  return "WRN";
        case LogLevel::Info:  return "INF";
        case LogLevel::Debug: return "DBG";
        case LogLevel::Off:   break;
    }
    return "OFF";
}

} // namespace BlackRabbitMQ
