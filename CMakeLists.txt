cmake_minimum_required(VERSION 3.16)
project(BlackRabbitMQ
    VERSION 0.0.3
    DESCRIPTION "Компонента RabbitMQ для 1С:Предприятие: C++17/20, RAII, событийная модель"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Platform detection
set(PLATFORM_UNIX OFF)
set(PLATFORM_WINDOWS OFF)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux" OR CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(PLATFORM_UNIX ON)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    set(PLATFORM_WINDOWS ON)
else()
    message(FATAL_ERROR "Unsupported platform: ${CMAKE_SYSTEM_NAME}")
endif()

# CRT на Windows линкуется статически.
#
# 1С кладёт собственный msvcp140.dll в каталог bin (в 8.3.27 это 14.16, сборка
# VS 2017) и он перекрывает системный. Компонента с динамическим CRT получает
# чужой рантайм: ABI std::mutex там другой, и первое же обращение к мьютексу
# роняет процесс 1С внутри _Mtx_trylock. Проверено на живой платформе:
# подключение и чтение свойств проходят, а Connect убивает сеанс с дампом.
if(MSVC)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()

# Dependencies
include(FetchContent)

# CMake 4.0 удалил поддержку cmake_minimum_required(VERSION < 3.5), а
# nlohmann/json 3.11.3 объявляет 3.1 — без этого конфигурация падает на
# сборке зависимости, а не на нашем коде. В CI не видно: там CMake 3.2x.
if(CMAKE_VERSION VERSION_GREATER_EQUAL 4.0 AND NOT DEFINED CMAKE_POLICY_VERSION_MINIMUM)
    set(CMAKE_POLICY_VERSION_MINIMUM 3.5)
endif()

# nlohmann/json
# GIT_SHALLOW: нужен один тег, а полная история репозитория — сотни мегабайт.
FetchContent_Declare(json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(json)

# AMQP-CPP
# MSVC: message.h использует size_t без <cstddef>
if(PLATFORM_WINDOWS)
    set(AMQP_PATCH_CMD sed -i.bak "s/VERSION 3\\.[0-9]/VERSION 3.16/" CMakeLists.txt && sed -i.bak "s/std::numeric_limits<size_t>::max()/(std::numeric_limits<size_t>::max)()/g" include/amqpcpp/message.h && sed -i.bak "s/std::min(/(std::min)(/g" include/amqpcpp/message.h)
else()
    set(AMQP_PATCH_CMD sed -i.bak "s/VERSION 3\\.[0-9]/VERSION 3.16/" CMakeLists.txt)
endif()

FetchContent_Declare(amqpcpp
    GIT_REPOSITORY https://github.com/CopernicaMarketingSoftware/AMQP-CPP.git
    GIT_TAG v4.3.19
    GIT_SHALLOW TRUE
    PATCH_COMMAND ${AMQP_PATCH_CMD}
)
set(AMQP-CPP_BUILD_SHARED OFF CACHE BOOL "" FORCE)
if(PLATFORM_UNIX)
    set(AMQP-CPP_LINUX_TCP ON CACHE BOOL "" FORCE)
endif()
FetchContent_MakeAvailable(amqpcpp)

# Platform-specific dependencies
if(PLATFORM_UNIX)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(LIBEVENT REQUIRED libevent)
    pkg_check_modules(OPENSSL REQUIRED openssl)
elseif(PLATFORM_WINDOWS)
    find_package(Poco REQUIRED Foundation Net NetSSL Crypto Util)
    find_package(OpenSSL REQUIRED)
endif()

# Sanitizers
# Объявлены ДО целей: add_compile_options действует только на цели,
# созданные после вызова, — иначе библиотека собиралась без инструментации
# и ASan-джоба в CI проверяла только тесты.
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)

if(ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()
if(ENABLE_UBSAN)
    add_compile_options(-fsanitize=undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=undefined)
endif()
if(ENABLE_TSAN)
    # Несовместим с ASan — включать отдельной сборкой.
    add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
    add_link_options(-fsanitize=thread)
endif()

# Source files
set(SOURCES
    src/Logger.cpp
    src/EventLoop.cpp
    src/Connection.cpp
    src/Channel.cpp
    src/Consumer.cpp
    src/Client.cpp
    src/AddInNative.cpp
    src/AddIn1S/RabbitApi1S.cpp
    src/AddIn1S/RabbitMQClientNative.cpp
)

set(HEADERS
    src/ITransport.h
    src/EventLoop.h
    src/TcpTransport.h
    src/Connection.h
    src/Channel.h
    src/Message.h
    src/Consumer.h
    src/Client.h
    src/AddIn1S/MemoryManager.h
    src/AddIn1S/CallContext.h
    src/AddIn1S/Component.h
    src/AddIn1S/RabbitApi1S.h
    src/AddIn1S/RabbitMQClientNative.h
    src/AddIn1S/sdk/AddInDefBase.h
    src/AddIn1S/sdk/types.h
    src/AddIn1S/sdk/IMemoryManager.h
    src/AddIn1S/sdk/ComponentBase.h
    src/AddIn1S/sdk/Error.hpp
)

if(PLATFORM_UNIX)
    list(APPEND SOURCES src/Platform/LibeventTransport.cpp)
    list(APPEND HEADERS src/Platform/LibeventTransport.h)
elseif(PLATFORM_WINDOWS)
    list(APPEND SOURCES src/Platform/PocoTransport.cpp)
endif()
list(APPEND HEADERS src/Platform/PocoTransport.h)

# Зависимости и includes перечислены один раз: их получают и ядро, и компонента.
set(BRMQ_LIBS nlohmann_json::nlohmann_json amqpcpp)
set(BRMQ_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/src)

if(PLATFORM_UNIX)
    list(APPEND BRMQ_LIBS ${LIBEVENT_LIBRARIES} ${OPENSSL_LIBRARIES})
    list(APPEND BRMQ_INCLUDES ${LIBEVENT_INCLUDE_DIRS} ${OPENSSL_INCLUDE_DIRS})
elseif(PLATFORM_WINDOWS)
    list(APPEND BRMQ_LIBS
        Poco::Foundation Poco::Net Poco::NetSSL Poco::Crypto Poco::Util
        OpenSSL::SSL OpenSSL::Crypto
    )
endif()

# ------------------------------------------------------------------
# Ядро — объектная библиотека
#
# Компонента для 1С экспортирует только точки входа (на Windows — через
# AddInNative.def), поэтому линковать с ней тесты нельзя: внутренних классов
# в экспортах DLL нет. Тесты линкуются с объектами напрямую.
# ------------------------------------------------------------------
add_library(brmq_core OBJECT ${SOURCES} ${HEADERS})
target_include_directories(brmq_core PUBLIC ${BRMQ_INCLUDES})
target_link_libraries(brmq_core PUBLIC ${BRMQ_LIBS})

# Версия из одного источника: project() → компонента (свойство Version в 1С)
# и MANIFEST.XML пакета.
target_compile_definitions(brmq_core PRIVATE BLACKRABBITMQ_VERSION="${PROJECT_VERSION}")

if(PLATFORM_WINDOWS)
    # PUBLIC: без NOMINMAX заголовки amqpcpp ломаются о макросы windows.h,
    # а их включают и тесты.
    # Минимальная целевая версия Windows — 7 / Server 2008 R2. Без явного
    # значения заголовки берут версию из установленного SDK, и в DLL попадают
    # импорты, которых на старых системах нет: клиент 1С такую компоненту
    # просто не загрузит, причём без внятной диагностики.
    target_compile_definitions(brmq_core PUBLIC
        NOMINMAX WIN32_LEAN_AND_MEAN _WINSOCKAPI_
        _WIN32_WINNT=0x0601 WINVER=0x0601)
endif()

# Compiler flags
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(brmq_core PRIVATE -Wall -Wextra -Wpedantic)
elseif(MSVC)
    target_compile_options(brmq_core PRIVATE /W4)
endif()

# ------------------------------------------------------------------
# Компонента для 1С
# ------------------------------------------------------------------
set(ADDIN_EXPORTS "")
if(PLATFORM_WINDOWS)
    # Модульный файл в списке исходников — MSVC сам передаст его линковщику
    # как /DEF. Так экспортируются точки входа для 1С (см. AddInNative.def).
    set(ADDIN_EXPORTS src/AddInNative.def)
endif()

add_library(BlackRabbitMQ SHARED $<TARGET_OBJECTS:brmq_core> ${ADDIN_EXPORTS})
target_include_directories(BlackRabbitMQ PUBLIC ${BRMQ_INCLUDES})
target_link_libraries(BlackRabbitMQ PUBLIC ${BRMQ_LIBS})

# ------------------------------------------------------------------
# Пакет внешней компоненты для 1С (MANIFEST.XML + бинарник в ZIP)
# ------------------------------------------------------------------
# На Windows CMAKE_SYSTEM_PROCESSOR описывает хост (всегда AMD64), а целевую
# платформу задаёт -A: без этой ветки 32-битная сборка уезжала в MANIFEST.XML
# как x86_64, и платформа отказывалась её грузить.
if(MSVC AND CMAKE_GENERATOR_PLATFORM)
    if(CMAKE_GENERATOR_PLATFORM STREQUAL "Win32")
        set(ADDIN_ARCH "i386")
    elseif(CMAKE_GENERATOR_PLATFORM MATCHES "ARM64")
        set(ADDIN_ARCH "arm64")
    else()
        set(ADDIN_ARCH "x86_64")
    endif()
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64|ARM64")
    set(ADDIN_ARCH "arm64")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "arm")
    set(ADDIN_ARCH "arm")
elseif(CMAKE_SIZEOF_VOID_P EQUAL 4)
    set(ADDIN_ARCH "i386")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64|amd64|i.86|x86|X86")
    set(ADDIN_ARCH "x86_64")
else()
    set(ADDIN_ARCH "x86_64")
    message(WARNING "Неизвестная архитектура ${CMAKE_SYSTEM_PROCESSOR}, в MANIFEST.XML пойдёт x86_64")
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(ADDIN_OS "Linux")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    set(ADDIN_OS "Windows")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(ADDIN_OS "MacOS")
endif()

find_package(Python3 COMPONENTS Interpreter)
if(Python3_Interpreter_FOUND)
    # Пакет только под текущую платформу: кросс-платформенный ZIP собирает
    # релизный workflow из артефактов Linux и Windows тем же скриптом.
    add_custom_target(package1c
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/packaging/make_package.py
            --version ${PROJECT_VERSION}
            --output ${CMAKE_BINARY_DIR}/dist/BlackRabbitMQ-${PROJECT_VERSION}-${ADDIN_OS}-${ADDIN_ARCH}.zip
            --component ${ADDIN_OS}:${ADDIN_ARCH}:$<TARGET_FILE:BlackRabbitMQ>
        DEPENDS BlackRabbitMQ
        COMMENT "Сборка ZIP-пакета внешней компоненты для 1С"
        VERBATIM
    )
else()
    message(STATUS "Python3 не найден — цель package1c недоступна")
endif()

# Tests
option(BUILD_TESTS "Build unit tests" ON)
option(BUILD_INTEGRATION_TESTS "Build integration tests (require RabbitMQ)" OFF)

if(BUILD_TESTS AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/CMakeLists.txt")
    enable_testing()
    add_subdirectory(tests)
endif()
