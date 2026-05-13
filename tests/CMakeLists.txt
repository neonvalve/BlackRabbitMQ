# Test framework: Google Test
include(FetchContent)
FetchContent_Declare(googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Base include directories
set(TEST_INCLUDES
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/AddIn1S
    ${CMAKE_SOURCE_DIR}/src/AddIn1S/sdk
)

# ------------------------------------------------------------------
# test_message — Message value object (без зависимостей от AMQP)
# ------------------------------------------------------------------
add_executable(test_message test_message.cpp)
target_include_directories(test_message PRIVATE ${TEST_INCLUDES})
target_link_libraries(test_message PRIVATE GTest::gtest_main)
add_test(NAME Message COMMAND test_message)

# ------------------------------------------------------------------
# test_call_context — CallContext, MemoryManager (без AMQP)
# ------------------------------------------------------------------
add_executable(test_call_context test_call_context.cpp)
target_include_directories(test_call_context PRIVATE ${TEST_INCLUDES})
target_link_libraries(test_call_context PRIVATE GTest::gtest_main)
add_test(NAME CallContext COMMAND test_call_context)

# ------------------------------------------------------------------
# test_client — Client интеграционные тесты (требуют RabbitMQ)
# ------------------------------------------------------------------
# Запускаются только при наличии переменной окружения RABBITMQ_HOST
# add_executable(test_client test_client.cpp)
# target_include_directories(test_client PRIVATE ${TEST_INCLUDES})
# target_link_libraries(test_client PRIVATE BlackRabbitMQ GTest::gtest_main)
# add_test(NAME Client COMMAND test_client)
