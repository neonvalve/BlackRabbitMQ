# Test framework: Google Test
include(FetchContent)
FetchContent_Declare(googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Base include directories (add AMQP-CPP build dir for headers)
set(TEST_INCLUDES
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/AddIn1S
    ${CMAKE_SOURCE_DIR}/src/AddIn1S/sdk
    ${amqpcpp_SOURCE_DIR}/include
)

# ------------------------------------------------------------------
# test_message — Message value object (без зависимостей от AMQP)
# ------------------------------------------------------------------
add_executable(test_message test_message.cpp)
target_include_directories(test_message PRIVATE ${TEST_INCLUDES})
target_link_libraries(test_message PRIVATE GTest::gtest_main amqpcpp)
add_test(NAME Message COMMAND test_message)

# ------------------------------------------------------------------
# test_call_context — CallContext, MemoryManager (без AMQP)
# ------------------------------------------------------------------
add_executable(test_call_context test_call_context.cpp)
target_include_directories(test_call_context PRIVATE ${TEST_INCLUDES})
target_link_libraries(test_call_context PRIVATE GTest::gtest_main)
add_test(NAME CallContext COMMAND test_call_context)

# ------------------------------------------------------------------
# test_integration — Интеграционные тесты с RabbitMQ
# Требуют: RABBITMQ_HOST (по умолчанию localhost:5672)
# Запуск: cmake -DBUILD_INTEGRATION_TESTS=ON ..
# ------------------------------------------------------------------
if(BUILD_INTEGRATION_TESTS)
    add_executable(test_integration test_integration.cpp)
    target_include_directories(test_integration PRIVATE ${TEST_INCLUDES})
    target_link_libraries(test_integration PRIVATE BlackRabbitMQ GTest::gtest_main)
    add_test(NAME Integration COMMAND test_integration)
endif()
