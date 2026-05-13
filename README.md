# BlackRabbitMQ

Современный форк [PinkRabbitMQ](https://github.com/BITERP/PinkRabbitMQ) —
внешняя Native API компонента для взаимодействия с RabbitMQ из 1С.

Переписан на C++17 с упором на RAII, потокобезопасность и событийную модель.

## Что исправлено

- **CPU 0% в простое** — блокирующий event loop вместо busy-wait (было 25%)
- **`volatile` → `std::atomic`** — безопасная синхронизация между потоками
- **Бинарные сообщения** — тело передаётся по размеру, не обрезается на `\0`
- **`BasicReject(ID, Requeue)`** — добавлен отсутствовавший параметр `requeue`
- **RAII** — ресурсы освобождаются автоматически, без ручного `new`/`delete`
- **Типобезопасные свойства** — `MessageProperties` вместо `map<int,string>`

## Быстрый старт

### 1С

```1c
ПодключитьВнешнююКомпоненту("ОбщийМакет.BlackRabbitMQ");
Клиент = Новый("AddIn.BlackRabbitMQ");

Клиент.Connect("localhost", 5672, "guest", "guest", "/", 0, Ложь, 30);
Клиент.BasicPublish("myexchange", "orders.new", "{""orderId"": 123}", 0, Истина, "");
Клиент = Неопределено;
```

### Сборка

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

## Платформы

- Linux (GCC/Clang, libevent) — основная целевая платформа (1С сервер)
- Windows (MSVC, POCO) — в разработке

## Документация

- [Архитектура](docs/architecture.md) — слои, EventLoop, потоки, владение
- [Безопасность памяти](docs/memory_safety.md) — RAII, порядок уничтожения, ASan/Valgrind
- [Примеры для 1С](docs/usage_examples_1s.md) — публикация, чтение, reject, ошибки

## Лицензия

MIT — как и исходный PinkRabbitMQ.
