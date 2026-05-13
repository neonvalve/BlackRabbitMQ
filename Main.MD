# План: Форк и переписывание PinkRabbitMQ на современную архитектуру

## Цели

- Устранить утечки памяти и хрупкость текущей реализации.
- Сделать архитектуру современной: C++17/20, RAII, умные указатели, exceptions-safe.
- Снизить требования к программистам 1С: меньше опасных мест, меньше ручного обнуления,
  уйти от polling к событийной модели.
- Улучшить тестирование и CI: Valgrind, ASan, юнит-тесты.
- Сохранить совместимость с API 1С, чтобы существующие конфиги работали с минимальными
  изменениями.

## Соглашение о коммитах

- Каждое логическое изменение фиксируется отдельным коммитом.
- Комментарии к коммитам — на русском языке.
- Формат: `<глагол в наст. времени>: <что сделано и зачем>`.
  Пример: `Добавить: поток EventLoop внутри DLL, убрать busy-wait`

---

## 1. Результаты анализа upstream (BITERP/PinkRabbitMQ v2.2.0.53)

### 1.1. Ключевые найденные проблемы

#### Память и владение
| Проблема | Где | Суть |
|---|---|---|
| `new`/`delete` вручную | `AddInNative.cpp:34` | `new RabbitMQClientNative()` в `GetClassObject`, нет умных указателей |
| `new`/`delete` вручную | `Connection.cpp:20,24` | `pimpl = new ConnectionImpl` / `delete pimpl` |
| Сырой указатель канала | `RabbitMQClient.h:153` | `AMQP::Channel* consumeChannel` — висячий указатель при пересоздании |

#### Потокобезопасность
| Проблема | Где | Суть |
|---|---|---|
| `volatile bool` вместо `std::atomic` | 4+ места | `Connection::broken`, `ConnectionImpl::stop`, `SimplePocoHandler::stop`, `openChannel::ready` |
| Busy-wait event loop (25% CPU) | `linux/ConnectionImpl.cpp:35` | `event_base_loop(loop, EVLOOP_NONBLOCK)` в тесном `while`, нагружает ядро на 100% |
| Polling-подключение | `ConnectionImpl::connect()` | `sleep_for(100ms)` в цикле проверки `connection->ready()` 15 секунд |
| `sleep_for(10us)` хак | `RabbitMQClient.cpp:392,402` | Маскирует гонку между ack/reject и следующей операцией |

#### Функциональные баги
| Проблема | Issue | Суть |
|---|---|---|
| `BasicReject` без `requeue` | #64 | Сообщение всегда удаляется, нельзя вернуть в очередь |
| Крах на 2-м/3-м Connect | #79 | Процесс 1С падает целиком при повторном подключении |
| Бинарные сообщения бьются | — | `strlen(message.c_str())` в `AMQP::Envelope` обрезает тело на `\0` |
| Падение на 123KB сообщении | #103 | Компонента падает при отправке, но сообщение доходит |

#### Архитектурные проблемы
| Проблема | Суть |
|---|---|
| Нет heartbeat/keepalive | Соединения отваливаются при простое |
| Один поток на всё | Event loop один на Connection, блокирует все операции |
| Платформенная дивергенция | Linux: `AMQP::TcpChannel`, Windows: `AMQP::Channel` — разные типы |
| Нет переподключения | При обрыве соединения нужно пересоздавать клиент вручную |
| API свойств через магические числа | `setMsgProp`/`getMsgProp` с константами 1-10 вместо структур |

### 1.2. Карта текущих ресурсов и владения

```
IAddInDefBase (1С)
  └─ RabbitMQClientNative (AddInNative.cpp)   — точка входа, владеет RabbitMQClient
       └─ RabbitMQClient (RabbitMQClient.h)    — основной класс
            ├─ Connection (unique_ptr)          — владеет ConnectionImpl (сырой new/delete)
            │    └─ ConnectionImpl (pimpl)
            │         ├─ event_base*            — libevent event loop (Linux)
            │         ├─ TCPHandler (unique_ptr) — TCP соединение (Linux)
            │         ├─ SimplePocoHandler      — TCP соединение (Windows)
            │         ├─ AMQP::TcpConnection    — AMQP соединение (Linux)
            │         ├─ AMQP::Connection       — AMQP соединение (Windows)
            │         ├─ TcpChannel (unique_ptr) — канал публикации
            │         ├─ TcpChannel (unique_ptr) — канал чтения
            │         └─ std::thread            — поток event loop
            ├─ MessageObject (struct)           — последнее полученное сообщение
            ├─ std::queue<MessageObject>        — очередь входящих сообщений
            ├─ std::vector<std::string>         — активные consumers
            └─ AMQP::Channel* (сырой указатель) — consumeChannel
```

---

## 2. Проектирование новой архитектуры

### 2.1. Основные принципы

1. **RAII**: ресурс создаётся в конструкторе, освобождается в деструкторе.
2. **Умные указатели**: `std::unique_ptr` для эксклюзивного владения,
   `std::shared_ptr` для разделяемого.
3. **Исключения-safe**: объект не остаётся в частично сломанном состоянии.
4. **Событийная модель (push)**: EventLoop живёт внутри DLL, при сообщении
   дёргает `ExternalEvent` в 1С. Никакого polling со стороны 1С.
5. **Типобезопасность на границе 1С**: минимизация выходных параметров, внутри
   C++ чистый API.
6. **`std::atomic` вместо `volatile`**: все флаги, разделяемые между потоками.
7. **Бинарно-безопасные строки**: `std::string` с `.size()`, никакого `strlen`.

### 2.2. Архитектура EventLoop внутри DLL

```
┌─────────────────────────────────────────────────────┐
│ DLL (процесс 1С)                                      │
│                                                       │
│  ┌─────────────────────────────────────────────┐     │
│  │ EventLoop Thread (один на Connection)         │     │
│  │                                               │     │
│  │  event_base_loop(loop, BLOCKING)  ← спит на   │     │
│  │  poll(), не жжёт CPU                          │     │
│  │                                               │     │
│  │  События:                                     │     │
│  │  ├─ onMessage  → messageQueue + ExternalEvent │     │
│  │  ├─ onError    → consumerError + ExternalEvent│     │
│  │  ├─ onCancel   → очистка consumer             │     │
│  │  └─ heartbeat  → встроен в AMQP-CPP           │     │
│  └─────────────────────────────────────────────┘     │
│                       │                               │
│                       ▼ ExternalEvent()               │
│  ┌─────────────────────────────────────────────┐     │
│  │ 1С: ОбработкаВнешнегоСобытия                 │     │
│  │   Источник = "RabbitMQ", Сообщение = "Msg"   │     │
│  │   Данные = json(тело, deliveryTag, ...)      │     │
│  └─────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────┘
```

Преимущества:
- Нет busy-wait → CPU 0% в простое
- Нет `BasicConsumeMessage` с таймаутом, блокирующего поток 1С
- Нет гонки ack/reject (канал только для consume, publish отдельно)
- Heartbeat работает автоматически через AMQP-CPP

### 2.3. Целевая структура классов

```
BlackRabbitMQ/
├─ src/
│  ├─ Connection.h / .cpp        — соединение с RabbitMQ, TLS
│  ├─ Channel.h / .cpp           — канал (publish или consume)
│  ├─ Consumer.h / .cpp          — потребитель с тегом и каналом
│  ├─ Message.h / .cpp           — value-объект сообщения
│  ├─ Client.h / .cpp            — фасад (connect, publish, ack, reject, ...)
│  ├─ EventLoop.h / .cpp         — абстракция event loop (libevent)
│  ├─ TcpTransport.h / .cpp      — унифицированный TCP транспорт (Linux/Windows)
│  └─ Platform/
│       ├─ TcpTransportLinux.cpp
│       └─ TcpTransportWindows.cpp
│
├─ src/AddIn1S/
│  ├─ RabbitApi1S.h / .cpp       — граница с 1С (совместимый API)
│  ├─ RabbitMQClientNative.h/.cpp— точка входа GetClassObject/DestroyObject
│  ├─ CallContext.h/.cpp         — адаптер параметров 1С ↔ C++
│  ├─ MemoryManager.h/.cpp       — управление памятью 1С
│  ├─ Component.h/.cpp           — базовый класс (init, done, GetLastError)
│  └─ Converters.h/.cpp          — UTF-8 ↔ UTF-16, JSON ↔ AMQP::Table
│
├─ tests/
│  ├─ test_connection.cpp
│  ├─ test_channel.cpp
│  ├─ test_consumer.cpp
│  ├─ test_memory_leaks.cpp
│  ├─ test_event_loop.cpp
│  └─ test_binary_messages.cpp
│
├─ cmake/
│  └─ CMakeLists.txt
│
├─ docs/
│  ├─ architecture.md
│  ├─ memory_safety.md
│  └─ usage_examples_1s.md
│
├─ .github/
│  └─ workflows/
│       ├─ build-linux.yml
│       └─ build-windows.yml
│
├─ .gitignore
├─ CMakeLists.txt
├─ CHANGELOG.md
└─ README.md
```

### 2.4. Классы

#### Connection
- Создаёт соединение с RabbitMQ через `AMQP::TcpConnection`
- Поддерживает TLS
- Встраивает heartbeat (через AMQP-CPP `onNegotiate`)
- Освобождает соединение в деструкторе
- Метод `reconnect()` для безопасного переподключения

#### EventLoop
- Абстрагирует `event_base` (libevent) или аналог
- Запускается в отдельном `std::thread`
- **Блокирующий** режим: `event_base_loop(loop, 0)` — спит на `poll()`,
  никакого `EVLOOP_NONBLOCK`
- Принимает callback'и на события канала

#### Channel
- Оборачивает `AMQP::TcpChannel` / `AMQP::Channel`
- Унифицирует тип: единый интерфейс для Linux и Windows
- Методы: `declareExchange`, `declareQueue`, `bindQueue`, `publish`, `consume`,
  `ack`, `reject`
- `reject(deliveryTag, requeue)` — **обязательный** параметр `requeue`

#### Consumer
- Создаёт и отменяет потребителя
- Хранит: `tag`, `queueName`
- При отмене (onCancelled) шлёт `ExternalEvent` в 1С

#### Message
- Value-объект:
  - `body` — `std::string` (бинарно-безопасный, размер через `.size()`)
  - `deliveryTag` — `uint64_t`
  - `routingKey` — `std::string`
  - `contentType` — `std::string`
  - `correlationId` — `std::string`
  - `messageId` — `std::string`
  - `headers` — `AMQP::Table`
  - `priority` — `int`
  - `redelivered` — `bool`
- Свойства сообщения — структура `MessageProperties`, а не `map<int, string>`

#### Client
- Главный фасад:
  - `connect()`
  - `disconnect()`
  - `reconnect()`
  - `declareExchange()`
  - `declareQueue()`
  - `bindQueue()`
  - `publish()`
  - `startConsumer()` — запускает event-driven consume
  - `stopConsumer()`
  - `ack()`
  - `reject(deliveryTag, requeue)`

#### RabbitApi1S (граница с 1С)
- Сохраняет совместимые имена методов: `Connect`, `Publish`, `BasicConsume`,
  `BasicAck`, `BasicReject`, `GetLastError`...
- `BasicReject(ID, Requee)` — **добавлен** второй параметр `Requeue` (bool)
- `BasicConsume` запускает EventLoop и подписывается, вместо polling
- Не прокидывает C++ исключения в 1С
- Все ошибки — через `GetLastError` и код возврата `false`
- Выходные параметры очищаются в любом сценарии

#### Прослойка 1С (на стороне языка 1С)
Общий модуль `BlackRabbitMQ`:
- `Подключиться()`
- `Отключиться()`
- `ОпубликоватьСобытие()`
- `НачатьСлушатьОчередь()` — регистрирует обработчик внешнего события
- `ОстановитьСлушатьОчередь()`
- Внутри: централизованный retry, логирование, очистка параметров

---

## 3. Ключевые исправления против upstream

| Проблема | Решение |
|---|---|
| `volatile bool` | `std::atomic<bool>` во всех потокоразделяемых флагах |
| Busy-wait event loop | Блокирующий `event_base_loop(loop, 0)`, поток спит на poll |
| Polling `connect()` | Событийное ожидание через callback `onReady` |
| `sleep_for(10us)` в ack/reject | Убрать, канал consume выделен и не пересекается с publish |
| `strlen()` на теле сообщения | `std::string` с `.size()`, никаких C-строк |
| `BasicReject` без requeue | Добавлен параметр `requeue` (bool) |
| Крах на reconnect | Деструктор корректно гасит event loop до освобождения ресурсов |
| `AMQP::TcpChannel` vs `AMQP::Channel` | Единая абстракция `Channel` над обеими платформами |
| `map<int,string>` для свойств | `MessageProperties` — типобезопасная структура |
| Нет heartbeat | Heartbeat через `onNegotiate` в AMQP-CPP |

---

## 4. Реализация

### 4.1. Базовая инфраструктура
1. Настроить CMake (миграция с `.sln`/makefile).
2. Подключить: AMQP-CPP, libevent (Linux), POCO (Windows).
3. Создать базовые классы: `EventLoop`, `TcpTransport`.
4. Настроить логирование.

### 4.2. EventLoop
1. Создать класс-обёртку над `event_base` (libevent).
2. Режим: **блокирующий** `event_base_loop(base, 0)` — без флагов.
3. Поток запускается при `connect()`, останавливается в деструкторе.
4. Все callback'и каналов регистрируются через EventLoop.

### 4.3. TcpTransport (унификация платформ)
1. Единый интерфейс для Linux (libevent + openssl) и Windows (POCO).
2. Скрывает разницу между `AMQP::TcpConnection` и `AMQP::Connection`.
3. Методы: `connect(address)`, `close()`, `isReady()`, `getError()`.

### 4.4. Переписать ключевые методы
1. **Connect**: создание через `std::make_unique`, ошибки безопасны,
   поддержка повторного подключения.
2. **Publish**: через `Channel::publish`, бинарно-безопасное тело.
3. **Consume**: событийная модель — callback через `ExternalEvent`.
4. **Ack / Reject**: раздельные каналы, нет гонки, `reject` с `requeue`.

### 4.5. Обработка ошибок
1. Хранить `lastError` как `std::string` (UTF-8).
2. На границе с 1С: возвращать `false`, обновлять `GetLastError`.
3. Не выбрасывать исключения наружу в 1С.
4. Все ошибки event loop пишутся в лог + `consumerError`.

---

## 5. Тестирование и память

### 5.1. Юнит-тесты
- connect / disconnect
- declare / bind / publish
- consume (событийная модель) / ack / reject (с requeue и без)
- переподключение (reconnect)
- бинарные сообщения (null byte в теле)
- ошибки (несуществующий exchange, обрыв соединения)

### 5.2. Тесты на утечки
- Valgrind на Linux
- AddressSanitizer (ASan)
- Многократные циклы: создать/удалить соединение, потребителя, читать/ack

Критерий: утечек быть не должно.

### 5.3. Нагрузочное тестирование
- CPU в простое: строго 0% (нет busy-wait)
- 10 000+ сообщений без потерь
- Переподключение без краша

---

## 6. Документация
- `architecture.md` — архитектура, event loop, потоки, владение
- `memory_safety.md` — RAII, умные указатели, отсутствие raw new/delete
- `usage_examples_1s.md` — примеры из 1С: подключение, публикация,
  событийное чтение, обработка ошибок

---

## 7. Совместимость с 1С

### 7.1. API
1. Сохранить привычные названия методов.
2. `BasicReject(ID, Requeue)` — добавлен второй параметр, старый код с
   одним параметром тоже работает (Requeue = false по умолчанию).
3. `BasicConsume` переходит на событийную модель:
   - Старый polling `BasicConsumeMessage` остаётся для совместимости
   - Новый способ: `BasicConsume` + обработчик `ОбработкаВнешнегоСобытия`

### 7.2. Breaking changes (честно оформлены как V2)
- Событийная модель — новый способ чтения (старый polling опционален)
- `BasicReject` с requeue — расширение сигнатуры

---

## 8. CI/CD и релизы

### 8.1. Сборка
- Собирать на Windows (MSVC) и Linux (GCC/Clang).
- CMake как единая система сборки.
- GitHub Actions: build + test при каждом push.

### 8.2. Версии
- SemVer.
- CHANGELOG.md.
- Тегировать релизы.

---

## 9. Этапы работ

### Этап 1. Проект и git
- Инициализация репозитория
- CMake проект с компиляцией пустой DLL
- CI (сборка на Linux + Windows)

### Этап 2. Ядро: Connection + EventLoop + TcpTransport
- `EventLoop` (блокирующий, без busy-wait)
- `TcpTransport` (унификация платформ)
- `Connection` (с heartbeat)

### Этап 3. Channel + Message
- `Channel` (унифицированный)
- `Message` + `MessageProperties`

### Этап 4. Client (фасад)
- `connect` / `disconnect` / `reconnect`
- `publish` (бинарно-безопасный)
- `declare*` / `bind*`

### Этап 5. Consumer (событийная модель)
- `Consumer` с `ExternalEvent`
- `ack` / `reject(requeue)`

### Этап 6. Граница с 1С
- `RabbitApi1S` (совместимый API)
- `RabbitMQClientNative` (GetClassObject/DestroyObject)
- `MemoryManager`, `CallContext`, конвертеры

### Этап 7. Тестирование
- Юнит-тесты
- Valgrind / ASan
- Стресс-тесты

### Этап 8. Документация и релиз
- README, docs/
- CHANGELOG
- Первый стабильный релиз

---

## 10. Критерии успеха
1. Нет утечек памяти (Valgrind + ASan чисто).
2. Ресурсы закрываются автоматически (RAII).
3. CPU в простое: 0%.
4. Ошибки не ломают компонент (exceptions-safe).
5. 1С-код работает предсказуемо.
6. Совместимость либо сохранена, либо честно оформлена как V2.
7. Проект собирается и тестируется на Windows и Linux.
8. Бинарные сообщения передаются без искажений.
9. Переподключение работает без краша.
10. Каждое логическое изменение — отдельный коммит с русским комментарием.
