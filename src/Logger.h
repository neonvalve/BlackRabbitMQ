#pragma once

#include <atomic>
#include <cstddef>
#include <fstream>
#include <mutex>
#include <string>

namespace BlackRabbitMQ {

enum class LogLevel {
    Off = 0,
    Error,
    Warn,
    Info,
    Debug
};

// Журнал компоненты.
//
// Компонента исполняется внутри процесса 1С, и при разборе инцидента у клиента
// нет ничего, кроме GetLastError и пересказа по телефону. Журнал пишет то, что
// иначе не восстановить: когда установилось и когда оборвалось соединение,
// сколько было попыток переподключения, отказала ли платформа в приёме события.
//
// Один экземпляр на процесс: соединение живёт в своём потоке, потребитель
// в своём, сторож в третьем — писать они должны в один файл и по порядку.
// Запись сбрасывается на диск сразу: если платформа упадёт, журнал должен
// пережить падение, иначе он бесполезен именно там, где нужен.
class Logger {
public:
    static Logger& instance();

    // Включает журнал. Пустой путь выключает его.
    // maxBytes — порог ротации: файл переименовывается в <path>.1, прежний .1
    // удаляется. Двух файлов достаточно, чтобы журнал не съел диск.
    void configure(const std::string& path, LogLevel level,
                   std::size_t maxBytes = 5 * 1024 * 1024);

    void setLevel(LogLevel level) { m_level.store(level, std::memory_order_release); }
    LogLevel level() const { return m_level.load(std::memory_order_acquire); }
    std::string path() const;

    // Быстрая проверка до сборки строки: на выключенном журнале вызов
    // не должен стоить ни одной конкатенации.
    bool enabled(LogLevel level) const {
        return level <= m_level.load(std::memory_order_acquire);
    }

    void write(LogLevel level, const std::string& message);

    // Разбор значения свойства LogLevel из 1С: "off", "error", "warn",
    // "info", "debug". Неизвестное значение — Off, чтобы опечатка не включила
    // болтливый режим в проде.
    static LogLevel levelFromString(const std::string& text);
    static const char* levelToString(LogLevel level);

private:
    Logger() = default;
    ~Logger();
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    void rotateIfNeeded();

    mutable std::mutex m_mutex;
    std::ofstream m_file;
    std::string m_path;
    std::size_t m_maxBytes = 5 * 1024 * 1024;
    std::size_t m_written = 0;
    std::atomic<LogLevel> m_level{LogLevel::Off};
};

// Уровень проверяется до сборки сообщения: выключенный журнал не стоит ничего.
#define BRMQ_LOG(lvl, expr)                                                    \
    do {                                                                       \
        if (::BlackRabbitMQ::Logger::instance().enabled(lvl)) {                \
            ::BlackRabbitMQ::Logger::instance().write((lvl), (expr));          \
        }                                                                      \
    } while (false)

#define BRMQ_LOG_ERROR(expr) BRMQ_LOG(::BlackRabbitMQ::LogLevel::Error, (expr))
#define BRMQ_LOG_WARN(expr)  BRMQ_LOG(::BlackRabbitMQ::LogLevel::Warn,  (expr))
#define BRMQ_LOG_INFO(expr)  BRMQ_LOG(::BlackRabbitMQ::LogLevel::Info,  (expr))
#define BRMQ_LOG_DEBUG(expr) BRMQ_LOG(::BlackRabbitMQ::LogLevel::Debug, (expr))

} // namespace BlackRabbitMQ
