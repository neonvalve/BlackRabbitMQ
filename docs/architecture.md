# Архитектура BlackRabbitMQ

Форк [PinkRabbitMQ](https://github.com/BITERP/PinkRabbitMQ) (v2.2.0.53),
переписанный на C++17 с упором на RAII, потокобезопасность и событийную модель.

## Слои

```
┌─────────────────────────────────────────┐
│ 1С (платформа)                           │
│   GetClassObject / CallAsProc / CallAsFunc│
└───────────────┬─────────────────────────┘
                │
┌───────────────▼─────────────────────────┐
│ AddInNative.cpp                          │  Точка входа DLL
│   GetClassObject / DestroyObject         │
└───────────────┬─────────────────────────┘
                │
┌───────────────▼─────────────────────────┐
│ RabbitMQClientNative : IComponentBase    │  Таблицы методов/свойств
│   FindMethod / CallAsProc / CallAsFunc   │  Диспетчеризация
└───────────────┬─────────────────────────┘
                │
┌───────────────▼─────────────────────────┐
│ RabbitApi1S : Component                  │  Совместимый API
│   Connect / Publish / BasicConsume ...   │  wrapCall → lastError
└───────┬───────────────────┬─────────────┘
        │                   │
┌───────▼──────┐    ┌───────▼──────┐
│ Client        │    │ Consumer      │        Бизнес-логика
│ (фасад)       │    │ (потребитель) │
└───────┬──────┘    └───────┬──────┘
        │                   │
┌───────▼──────┐    ┌───────▼──────┐
│ Channel       │    │ Channel       │        AMQP каналы
└───────┬──────┘    └───────┬──────┘
        │                   │
┌───────▼───────────────────▼──────┐
│ Connection                         │        Соединение
│   EventLoop + TcpTransport        │
└───────────────────────────────────┘
```

## Событийный цикл (EventLoop)

EventLoop — это RAII-обёртка над `event_base` (libevent).

```
Главный поток (1С):          Поток EventLoop:
│                             │
│ Client::connect()           │
│   eventLoop->run() ────────► event_base_loop(base, 0)
│   waitForReady()            │   └─ poll() — спит, 0% CPU
│                             │   └─ обработка событий AMQP
│ Channel::declareExchange()  │
│   channel->declareExchange()│
│   wait() ────────────────┐  │
│                          │  │
│   (cv.wait) ◄────────────┼── onSuccess → signalSuccess()
│                          │  │   └─ cv.notify_all()
│   return                 │  │
│                             │
│ ~Connection()               │
│   eventLoop->stop() ───────► event_base_loopbreak(base)
│                             │   └─ выход из poll()
│   thread.join() ◄───────────│
```

## Владение ресурсами

Порядок создания (конструктор Connection):
1. `EventLoop` — владеет `event_base*`
2. `TcpTransport` — использует `event_base` из EventLoop
3. `AMQP::TcpConnection` — использует TcpTransport как handler

Порядок уничтожения (деструктор Connection) — **строгий, гарантирован полями**:
1. `AMQP::TcpConnection` — закрывает AMQP handshake
2. `TcpTransport` — закрывает TCP соединение
3. `EventLoop` — останавливает поток, освобождает `event_base`

Поля объявлены в порядке, обратном уничтожению (C++ гарантирует).

## Потокобезопасность

- Все флаги между потоками: `std::atomic<bool>` (не `volatile`)
- `EventLoop::m_running` — статус event loop
- `TcpTransport::m_lost` — статус TCP соединения
- `Channel::m_ready` — готовность операции
- `Client::m_connected` — статус подключения

Порядок памяти: `acquire` при чтении, `release` при записи.

## Синхронные операции AMQP

Channel оборачивает асинхронный API AMQP-CPP в синхронные вызовы:

```
Channel::declareExchange(name, type, flags, args)
  │
  ├─ m_channel->declareExchange(...)
  │     .onSuccess(→ signalSuccess)
  │     .onError(→ signalError)
  │
  └─ wait()
       └─ cv.wait() ← ждёт signalSuccess/signalError из потока EventLoop
```

## Отличия от upstream (PinkRabbitMQ v2.2.0.53)

| Проблема | Upstream | BlackRabbitMQ |
|---|---|---|
| Event loop | `EVLOOP_NONBLOCK` + `while` → 25% CPU | `event_base_loop(base, 0)` → 0% CPU |
| Stop flag | `volatile bool` | `std::atomic<bool>` |
| Владение event_base | Сырой указатель, ручной `free` | `unique_ptr` через EventLoop |
| Порядок удаления | Неочевидный | Явный (поля класса) |
| `new`/`delete` | `Connection.cpp`, `AddInNative.cpp` | `unique_ptr`/`make_unique` |
| Тело сообщения | `strlen()` — обрезает на `\0` | `std::string::size()` — бинарно-безопасно |
| Свойства | `map<int,string>` с магическими числами | Типобезопасная структура `MessageProperties` |
| `BasicReject` | Только `tag` | `reject(tag, requeue)` |
| Heartbeat | Отсутствует | Встроен в AMQP-CPP |
| Каналы | `TcpChannel` (Linux) vs `Channel` (Windows) | Унифицированная обёртка `Channel` |

## Сборка

```
cmake -B build -DCMAKE_BUILD_TYPE=Debug -DENABLE_ASAN=ON
cmake --build build
ctest --test-dir build
```

Поддерживаемые платформы: Linux (GCC/Clang), Windows (MSVC).
