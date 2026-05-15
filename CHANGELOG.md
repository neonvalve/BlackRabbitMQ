# Changelog

Все значимые изменения проекта документируются в этом файле.

Формат основан на [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
версии по [SemVer](https://semver.org/spec/v2.0.0.html).

## [0.2.0] — 2026-05-15

### Добавлено
- ITransport — абстрактный интерфейс транспорта (ООП-рефакторинг)
- LibeventTransport — реализация для Linux/macOS (libevent)
- PocoTransport — реализация для Windows (POCO)
- Интеграционные тесты с реальным RabbitMQ (7 тестов)
- onCancelled — callback отмены потребителя (AMQP-CPP v4.3.19)
- Сборка на macOS (Clang)

### Исправлено
- EventLoop deadlock: m_running=false перед loopbreak
- Ack/Reject через канал потребителя (было на publish-канале)
- AMQP-CPP v4.3.19 на всех платформах

## [0.1.0] — 2026-05-13

### Добавлено
- EventLoop: RAII-обёртка над libevent, блокирующий режим, 0% CPU в простое
- Connection: RAII соединение с RabbitMQ, правильный порядок уничтожения
- Channel: синхронная обёртка над AMQP::TcpChannel с condition_variable
- Message: бинарно-безопасный value-объект с типобезопасными MessageProperties
- Consumer: RAII-потребитель с событийными callback'ами
- Client: главный фасад (connect, declare, publish, consume, ack, reject)
- RabbitApi1S: совместимый с 1С API (Connect, BasicPublish, BasicConsume, ...)
- RabbitMQClientNative: реализация IComponentBase (таблицы методов/свойств)
- AddInNative.cpp: точка входа DLL (GetClassObject/DestroyObject)
- CMake проект с FetchContent (AMQP-CPP, nlohmann/json, Google Test)
- Опции AddressSanitizer и UBSanitizer
- Юнит-тесты (Message, CallContext, MemoryManager)
- Документация (architecture, memory_safety, usage_examples_1s)
- CI/CD: GitHub Actions (Linux GCC/Clang + ASan/UBSan)

### Исправлено (против upstream PinkRabbitMQ v2.2.0.53)
- Event loop: `EVLOOP_NONBLOCK` + busy-wait → блокирующий `event_base_loop(base, 0)`
- Потокобезопасность: `volatile bool` → `std::atomic<bool>` во всех флагах
- Тело сообщения: `strlen()` → `std::string::size()` (бинарно-безопасно)
- BasicReject: добавлен параметр `requeue` (отсутствовал в upstream)
- Свойства: `map<int,string>` с магическими числами → типобезопасная структура
- Владение: ручные `new`/`delete` → `unique_ptr`/`make_unique`
- Порядок уничтожения: явный через порядок полей класса

### Ожидает реализации
- Интеграционные тесты с реальным RabbitMQ
