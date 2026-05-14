#include <gtest/gtest.h>
#include "CallContext.h"
#include "MemoryManager.h"
#include "sdk/types.h"
#include <vector>

using namespace BlackRabbitMQ::AddIn1S;

// Тесты CallContext и MemoryManager.
// Не требуют AMQP-CPP или RabbitMQ — тестируют границу 1С.

// ------------------------------------------------------------------
// MemoryManager — без реального IMemoryManager (handle = nullptr)
// ------------------------------------------------------------------

TEST(MemoryManager, NewManagerHasNoHandle) {
    MemoryManager mm;
    EXPECT_EQ(mm.handle(), nullptr);
}

TEST(MemoryManager, SetHandle) {
    MemoryManager mm;
    // Мы не можем создать реальный IMemoryManager без платформы 1С,
    // но можем проверить, что handle корректно устанавливается.
    EXPECT_EQ(mm.handle(), nullptr);
    // setHandle с nullptr не падает
    mm.setHandle(nullptr);
    EXPECT_EQ(mm.handle(), nullptr);
}

TEST(MemoryManager, AllocReturnsFalseWithoutHandle) {
    MemoryManager mm;
    void* ptr = reinterpret_cast<void*>(0x1);
    EXPECT_FALSE(mm.alloc(&ptr, 100));
}

TEST(MemoryManager, VariantFromStringReturnsFalseWithoutHandle) {
    MemoryManager mm;
    tVariant var;
    var.vt = VTYPE_I4;
    std::u16string str = u"test";
    EXPECT_FALSE(mm.variantFromString(&var, str));
    // var не изменён при ошибке
    // (vt очищается только при успехе)
}

// ------------------------------------------------------------------
// CallContext — чтение параметров из tVariant массива
// ------------------------------------------------------------------

// Вспомогательная функция для создания tVariant со строкой.
// Строки хранятся в статическом векторе, чтобы указатели не висели.
std::vector<std::u16string> g_stringStorage;

tVariant makeStringParam(const std::u16string& str) {
    g_stringStorage.push_back(str);
    const auto& stored = g_stringStorage.back();
    tVariant v;
    v.vt = VTYPE_PWSTR;
    v.pwstrVal = const_cast<WCHAR_T*>(reinterpret_cast<const WCHAR_T*>(stored.c_str()));
    v.wstrLen = static_cast<uint32_t>(stored.length());
    return v;
}

tVariant makeIntParam(int value) {
    tVariant v;
    v.vt = VTYPE_I4;
    v.lVal = value;
    return v;
}

tVariant makeLongParam(int64_t value) {
    tVariant v;
    v.vt = VTYPE_I8;
    v.llVal = value;
    return v;
}

tVariant makeBoolParam(bool value) {
    tVariant v;
    v.vt = VTYPE_BOOL;
    v.bVal = value;
    return v;
}

tVariant makeDoubleParam(double value) {
    tVariant v;
    v.vt = VTYPE_R8;
    v.dblVal = value;
    return v;
}

tVariant makeEmptyParam() {
    tVariant v;
    v.vt = VTYPE_EMPTY;
    return v;
}

TEST(CallContext, ReadStringParam) {
    MemoryManager mm;
    std::u16string expected = u"localhost";
    tVariant params[] = { makeStringParam(expected) };

    CallContext ctx(mm, params, 1);
    std::u16string result = ctx.stringParam();

    EXPECT_EQ(result, expected);
}

TEST(CallContext, ReadStringParamUtf8) {
    MemoryManager mm;
    std::u16string input = u"hello";
    tVariant params[] = { makeStringParam(input) };

    CallContext ctx(mm, params, 1);
    std::string result = ctx.stringParamUtf8();

    EXPECT_EQ(result, "hello");
}

TEST(CallContext, ReadEmptyStringAsNullable) {
    MemoryManager mm;
    tVariant params[] = { makeEmptyParam() };

    CallContext ctx(mm, params, 1);
    std::u16string result = ctx.stringParam(true);

    EXPECT_TRUE(result.empty());
}

TEST(CallContext, ReadIntParam) {
    MemoryManager mm;
    tVariant params[] = { makeIntParam(5672) };

    CallContext ctx(mm, params, 1);
    int result = ctx.intParam();

    EXPECT_EQ(result, 5672);
}

TEST(CallContext, ReadLongParam) {
    MemoryManager mm;
    int64_t expected = 12345678901234LL;
    tVariant params[] = { makeLongParam(expected) };

    CallContext ctx(mm, params, 1);
    int64_t result = ctx.longParam();

    EXPECT_EQ(result, expected);
}

TEST(CallContext, ReadBoolParam) {
    MemoryManager mm;
    tVariant params[] = { makeBoolParam(true) };

    CallContext ctx(mm, params, 1);
    bool result = ctx.boolParam();

    EXPECT_TRUE(result);
}

TEST(CallContext, ReadDoubleParam) {
    MemoryManager mm;
    tVariant params[] = { makeDoubleParam(3.14) };

    CallContext ctx(mm, params, 1);
    double result = ctx.doubleParam();

    EXPECT_DOUBLE_EQ(result, 3.14);
}

TEST(CallContext, ReadMultipleParamsInSequence) {
    MemoryManager mm;
    tVariant params[] = {
        makeStringParam(u"myexchange"),
        makeStringParam(u"myrouting"),
        makeIntParam(42),
        makeBoolParam(false)
    };

    CallContext ctx(mm, params, 4);

    EXPECT_EQ(ctx.stringParam(), u"myexchange");
    EXPECT_EQ(ctx.stringParam(), u"myrouting");
    EXPECT_EQ(ctx.intParam(), 42);
    EXPECT_FALSE(ctx.boolParam());
}

TEST(CallContext, SkipParam) {
    MemoryManager mm;
    tVariant params[] = {
        makeStringParam(u"skipme"),
        makeIntParam(100)
    };

    CallContext ctx(mm, params, 2);
    ctx.skipParam(); // пропустить первый
    EXPECT_EQ(ctx.intParam(), 100);
}

TEST(CallContext, ThrowsOnExtraParam) {
    MemoryManager mm;
    tVariant params[] = { makeIntParam(1) };

    CallContext ctx(mm, params, 1);
    ctx.intParam(); // ok
    EXPECT_THROW(ctx.intParam(), std::runtime_error);
}

TEST(CallContext, ThrowsOnWrongType) {
    MemoryManager mm;
    tVariant params[] = { makeIntParam(42) };

    CallContext ctx(mm, params, 1);
    // stringParam ждёт VTYPE_PWSTR, но получает VTYPE_I4
    EXPECT_THROW(ctx.stringParam(false), TypeError);
}

// ------------------------------------------------------------------
// CallContext — запись результата
// ------------------------------------------------------------------

TEST(CallContext, SetBoolResult) {
    MemoryManager mm;
    tVariant retVal;
    tVariant* params = nullptr;

    CallContext ctx(mm, params, 0, &retVal);
    ctx.setBoolResult(true);

    EXPECT_EQ(retVal.vt, VTYPE_BOOL);
    EXPECT_TRUE(retVal.bVal);
}

TEST(CallContext, SetIntResult) {
    MemoryManager mm;
    tVariant retVal;

    CallContext ctx(mm, nullptr, 0, &retVal);
    ctx.setIntResult(42);

    EXPECT_EQ(retVal.vt, VTYPE_I4);
    EXPECT_EQ(retVal.lVal, 42);
}

TEST(CallContext, SetDoubleResult) {
    MemoryManager mm;
    tVariant retVal;

    CallContext ctx(mm, nullptr, 0, &retVal);
    ctx.setDoubleResult(2.718);

    EXPECT_EQ(retVal.vt, VTYPE_R8);
    EXPECT_DOUBLE_EQ(retVal.dblVal, 2.718);
}

TEST(CallContext, SetEmptyResult) {
    MemoryManager mm;
    tVariant retVal;

    CallContext ctx(mm, nullptr, 0, &retVal);
    ctx.setEmptyResult();

    EXPECT_EQ(retVal.vt, VTYPE_EMPTY);
}

TEST(CallContext, SetStringResultRequiresMemoryManager) {
    // Без реального MemoryManager (handle == nullptr)
    // variantFromString вернёт false → исключение
    MemoryManager mm;
    tVariant retVal;
    retVal.vt = VTYPE_I4;

    CallContext ctx(mm, nullptr, 0, &retVal);
    EXPECT_THROW(ctx.setStringResult(u"test"), std::runtime_error);
}
