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
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(PLATFORM_LINUX ON)
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
    GIT_TAG v4.3.19
)
set(AMQP-CPP_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(AMQP-CPP_LINUX_TCP ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(amqpcpp)

# Platform-specific dependencies
if(PLATFORM_LINUX)
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
    src/Client.cpp
)

set(HEADERS
    src/EventLoop.h
    src/TcpTransport.h
    src/Connection.h
    src/Channel.h
    src/Message.h
    src/Client.h
    src/Platform/TcpTransportLinux.h
    src/Platform/TcpTransportWindows.h
)

if(PLATFORM_LINUX)
    list(APPEND SOURCES src/Platform/TcpTransportLinux.cpp)
    list(APPEND HEADERS src/Platform/TcpTransportLinux.h)
elseif(PLATFORM_WINDOWS)
    list(APPEND SOURCES src/Platform/TcpTransportWindows.cpp)
    list(APPEND HEADERS src/Platform/TcpTransportWindows.h)
endif()

# Library target
add_library(BlackRabbitMQ SHARED ${SOURCES} ${HEADERS})
target_include_directories(BlackRabbitMQ PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(BlackRabbitMQ PUBLIC
    nlohmann_json::nlohmann_json
    amqpcpp
)

if(PLATFORM_LINUX)
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
elseif(MSVC)
    target_compile_options(BlackRabbitMQ PRIVATE /W4)
endif()

# Tests (optional)
option(BUILD_TESTS "Build unit tests" ON)
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
