# CODEMAP.md — BlackRabbitMQ

> Последнее обновление: 2026-05-13
> Проект: Форк PinkRabbitMQ (BITERP), переписанный на C++17/20

## Структура проекта

```
BlackRabbitMQ/
├── CMakeLists.txt                 — корневой CMake (C++17, FetchContent)
├── Main.MD                        — PRD проекта
├── .gitignore
│
├── src/                           — исходный код библиотеки
│   ├── EventLoop.h/.cpp           — RAII event loop (libevent, блокирующий)
│   ├── TcpTransport.h             — платформенный селектор транспорта
│   ├── Connection.h/.cpp          — RAII соединение с RabbitMQ
│   └── Platform/
│       ├── TcpTransportLinux.h/.cpp   — Linux: AMQP::LibEventHandler
│       └── TcpTransportWindows.h/.cpp — Windows: AMQP::ConnectionHandler (заглушка)
│
├── tests/                         — юнит-тесты (см. CMakeLists.txt)
│
├── docs/                          — документация (см. PRD, Этап 6)
│
└── reference/                     — клон upstream PinkRabbitMQ (не коммитится)
```

## Ключевые классы

### EventLoop (`src/EventLoop.h`)
Владеет `event_base*` и потоком. Запускает блокирующий `event_base_loop(base, 0)`
— без busy-wait, 0% CPU в простое. `std::atomic<bool>` для статуса.
- Используется: `Connection`

### TcpTransport (`src/TcpTransport.h`)
Платформенный `using`-селектор:
- Linux: `TcpTransportLinux` (расширяет `AMQP::LibEventHandler`)
- Windows: `TcpTransportWindows` (расширяет `AMQP::ConnectionHandler`)

### TcpTransportLinux (`src/Platform/TcpTransportLinux.h`)
Обёртка над `AMQP::LibEventHandler`. Добавляет: `std::atomic<bool>` для `isLost()`,
сообщение об ошибке. Вся работа с TCP/TLS/буферами — внутри `LibEventHandler`.
- Зависит от: `libevent`, `OpenSSL`, `amqpcpp/libevent.h`
- Используется: `Connection`

### TcpTransportWindows (`src/Platform/TcpTransportWindows.h`)
Заглушка. Реализует `AMQP::ConnectionHandler` через POCO.
- Зависит от: `POCO`, `OpenSSL`, `amqpcpp.h`
- Статус: TODO

### Connection (`src/Connection.h`)
RAII-соединение с RabbitMQ. Владеет (в порядке уничтожения):
1. `AMQP::TcpConnection` — уничтожается первым
2. `TcpTransport` — уничтожается вторым
3. `EventLoop` — уничтожается последним (после остановки потока)

Методы: `connect()`, `disconnect()`, `reconnect()`, `createChannel()`.
- Использует: `EventLoop`, `TcpTransport`, `AMQP::TcpConnection`
- Используется: будущим `Client`

## Исправления против upstream (BITERP/PinkRabbitMQ v2.2.0.53)

| Проблема | Было | Стало |
|---|---|---|
| Event loop | `EVLOOP_NONBLOCK` в while → 25% CPU | `event_base_loop(base, 0)` блокирующий → 0% CPU |
| Stop flag | `volatile bool` | `std::atomic<bool>` |
| Lost flag | `bool` (гонка) | `std::atomic<bool>` |
| Владение event_base | Сырой указатель, ручное освобождение | `unique_ptr`-стиль через `EventLoop` |
| Порядок удаления | Неочевидный, возможны гонки | Явный: Conn → Transport → EventLoop |

## Зависимости

| Библиотека | Назначение | Платформа |
|---|---|---|
| AMQP-CPP (4.3.x) | AMQP-протокол | Linux, Windows |
| libevent | Event loop + TCP | Linux |
| POCO (Foundation, Net) | Event loop + TCP | Windows |
| OpenSSL | TLS | Linux, Windows |
| nlohmann/json (3.11.x) | JSON для headers | Linux, Windows |

## Ближайшие шаги (по PRD)

1. ✅ EventLoop, TcpTransport, Connection — ядро
2. Channel — унифицированный канал (publish + consume)
3. Message + MessageProperties
4. Client (фасад)
5. Consumer (событийная модель)
6. Граница с 1С (RabbitApi1S, RabbitMQClientNative)
