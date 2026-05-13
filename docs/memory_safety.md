# Безопасность памяти в BlackRabbitMQ

## Принципы

1. **Никакого ручного `new`/`delete`.** Все ресурсы через `std::unique_ptr`
   и `std::make_unique`.
2. **RAII.** Ресурс создаётся в конструкторе, освобождается в деструкторе.
   Исключение в конструкторе не оставляет частично сконструированный объект.
3. **Явный порядок уничтожения.** Поля класса объявлены в порядке,
   обратном порядку уничтожения. C++ гарантирует вызов деструкторов
   в этом порядке.
4. **Бинарно-безопасные строки.** `std::string` с `.size()`, без `strlen`.

## Карта владения

```
Client
├─ unique_ptr<Connection>         — владеет
│   ├─ unique_ptr<EventLoop>      — владеет
│   │   └─ event_base*            — сырой, освобождается в ~EventLoop
│   ├─ unique_ptr<TcpTransport>   — владеет
│   └─ unique_ptr<TcpConnection>  — владеет (AMQP-CPP)
├─ unique_ptr<Channel>            — владеет (publish)
└─ unique_ptr<Channel>            — владеет (consume)
    └─ unique_ptr<TcpChannel>     — владеет (AMQP-CPP)

Consumer
└─ unique_ptr<Channel>            — владеет

RabbitApi1S
├─ unique_ptr<Client>             — владеет
└─ unique_ptr<Consumer>           — владеет
```

Никаких сырых указателей между объектами разного времени жизни.
`TcpTransport*` передаётся в `AMQP::TcpConnection` как handler, но
`TcpTransport` гарантированно живёт дольше (поле выше в `Connection`).

## Порядок уничтожения Connection

```cpp
class Connection {
private:
    std::unique_ptr<EventLoop> m_eventLoop;       // 3. Уничтожается последним
    std::unique_ptr<TcpTransport> m_transport;     // 2. Уничтожается вторым
    std::unique_ptr<AMQP::TcpConnection> m_conn;   // 1. Уничтожается первым
};
```

Порядок важен:
1. `m_conn` должен быть уничтожен до `m_transport` (использует handler из него)
2. `m_transport` должен быть уничтожен до `m_eventLoop` (использует event_base)
3. `m_eventLoop` должен быть уничтожен последним (останавливает поток)

## Исключения и safety

- **Конструкторы:** при ошибке бросают исключение, деструкторы уже созданных
  полей вызываются автоматически.
- **Channel::wait():** если callback вызвал `signalError`, бросается исключение.
  Channel остаётся в консистентном состоянии.
- **Client::connect():** при ошибке соединение разрушается, `m_connected = false`.
- **RabbitApi1S::wrapCall():** ловит `std::exception`, пишет в `lastError`,
  возвращает `false`. Исключения не проходят в 1С.

## Инструменты проверки

### AddressSanitizer (ASan)
```sh
cmake -B build -DENABLE_ASAN=ON
cmake --build build
ctest --test-dir build
```

Ловит: use-after-free, heap buffer overflow, stack buffer overflow, memory leaks.

### UndefinedBehaviorSanitizer (UBSan)
```sh
cmake -B build -DENABLE_UBSAN=ON
cmake --build build
ctest --test-dir build
```

Ловит: целочисленное переполнение, разыменование nullptr, сдвиги.

### Valgrind (Linux)
```sh
valgrind --leak-check=full --show-leak-kinds=all ./test_message
valgrind --leak-check=full --show-leak-kinds=all ./test_call_context
```

Критерий приёмки: `All heap blocks were freed -- no leaks are possible`.

## Критические места для проверки при рефакторинге

- `AddInNative.cpp::GetClassObject` — `new RabbitMQClientNative()`. Это контракт 1С,
  нельзя заменить на `unique_ptr`. Но `RabbitMQClientNative` использует `unique_ptr`
  для всех внутренних ресурсов.
- `Connection::createChannel()` — возвращает `unique_ptr<TcpChannel>`. Вызывающий
  должен сохранить или передать владение.
- `Channel::consume()` — сохраняет лямбды-функции. Лямбды не должны захватывать
  указатели на объекты с меньшим временем жизни.
- `RabbitApi1S::basicConsumeImpl()` — лямбда `onMessage` захватывает `this`.
  Гарантируется, что `Consumer` (и его канал) живут дольше лямбды.
