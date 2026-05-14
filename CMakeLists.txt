cmake_minimum_required(VERSION 3.16)
project(BlackRabbitMQ
    VERSION 0.1.0
    DESCRIPTION "Современный форк PinkRabbitMQ: C++17/20, RAII, событийная модель EventLoop для 1С"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

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

# Dependencies
include(FetchContent)

# nlohmann/json
FetchContent_Declare(json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
)
FetchContent_MakeAvailable(json)

# AMQP-CPP
FetchContent_Declare(amqpcpp
    GIT_REPOSITORY https://github.com/CopernicaMarketingSoftware/AMQP-CPP.git
    GIT_TAG v4.1.4
    PATCH_COMMAND sed -i.bak "s/VERSION 3\\.[0-9]/VERSION 3.16/" CMakeLists.txt
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
    find_package(Poco REQUIRED Foundation Net)
    find_package(OpenSSL REQUIRED)
endif()

# Source files
set(SOURCES
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
    list(APPEND SOURCES src/Platform/TcpTransportLinux.cpp)
    list(APPEND HEADERS src/Platform/TcpTransportLinux.h)
elseif(PLATFORM_WINDOWS)
    list(APPEND SOURCES src/Platform/TcpTransportWindows.cpp)
endif()
list(APPEND HEADERS src/Platform/TcpTransportWindows.h)

# Library target
add_library(BlackRabbitMQ SHARED ${SOURCES} ${HEADERS})
target_include_directories(BlackRabbitMQ PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(BlackRabbitMQ PUBLIC
    nlohmann_json::nlohmann_json
    amqpcpp
)

if(PLATFORM_UNIX)
    target_link_libraries(BlackRabbitMQ PUBLIC
        ${LIBEVENT_LIBRARIES}
        ${OPENSSL_LIBRARIES}
    )
    target_include_directories(BlackRabbitMQ PUBLIC
        ${LIBEVENT_INCLUDE_DIRS}
        ${OPENSSL_INCLUDE_DIRS}
    )
elseif(PLATFORM_WINDOWS)
    target_link_libraries(BlackRabbitMQ PUBLIC
        Poco::Foundation
        Poco::Net
        OpenSSL::SSL
        OpenSSL::Crypto
    )
endif()

# Compiler flags
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(BlackRabbitMQ PRIVATE -Wall -Wextra -Wpedantic)
    target_link_options(BlackRabbitMQ PRIVATE -v)
elseif(MSVC)
    target_compile_options(BlackRabbitMQ PRIVATE /W4)
endif()

# Sanitizers
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)

if(ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()
if(ENABLE_UBSAN)
    add_compile_options(-fsanitize=undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=undefined)
endif()

# Tests
option(BUILD_TESTS "Build unit tests" ON)
option(BUILD_INTEGRATION_TESTS "Build integration tests (require RabbitMQ)" OFF)

if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
