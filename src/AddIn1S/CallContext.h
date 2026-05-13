#pragma once

#include "sdk/types.h"
#include "MemoryManager.h"
#include "sdk/Error.hpp"

#include <string>
#include <codecvt>
#include <locale>
#include <stdexcept>

namespace BlackRabbitMQ {
namespace AddIn1S {

// Ошибка несоответствия типа параметра при вызове из 1С.
class TypeError : public std::runtime_error {
public:
    TypeError(int index, const std::string& expected, int actual)
        : std::runtime_error("Parameter " + std::to_string(index)
            + ": expected " + expected + ", got type " + std::to_string(actual))
    {}
};

// Адаптер параметров 1С → C++.
// Обходит массив tVariant*, извлекая типизированные значения.
class CallContext {
public:
    CallContext(const CallContext&) = delete;
    CallContext& operator=(const CallContext&) = delete;

    CallContext(MemoryManager& memManager, tVariant* paParams, long lSizeArray,
                tVariant* pvarRetValue = nullptr)
        : m_memManager(memManager)
        , m_params(paParams)
        , m_retValue(pvarRetValue)
        , m_count(lSizeArray)
        , m_index(0)
    {}

    // --- Чтение параметров ---

    tVariant* currentParam() {
        if (m_index >= m_count) {
            throw std::runtime_error("Extra parameter requested: " + std::to_string(m_index));
        }
        return &m_params[m_index];
    }

    tVariant* skipParam() {
        return &m_params[m_index++];
    }

    std::u16string stringParam(bool nullable = true) {
        tVariant* p = currentParam();
        if (p->vt == VTYPE_PWSTR) {
            m_index++;
            return std::u16string(reinterpret_cast<const char16_t*>(p->pwstrVal), p->wstrLen);
        }
        if (nullable && (p->vt == VTYPE_EMPTY || p->vt == VTYPE_NULL)) {
            m_index++;
            return u"";
        }
        throw TypeError(m_index, "string", p->vt);
    }

    std::string stringParamUtf8(bool nullable = true) {
        return m_converter.to_bytes(stringParam(nullable));
    }

    int intParam() {
        return static_cast<int>(longParam());
    }

    int64_t longParam() {
        tVariant* p = currentParam();
        switch (p->vt) {
            case VTYPE_UI1: case VTYPE_I1:
            case VTYPE_UI2: case VTYPE_I2:
            case VTYPE_UI4: case VTYPE_I4:
                m_index++;
                return p->lVal;
            case VTYPE_UI8: case VTYPE_I8:
                m_index++;
                return p->llVal;
            default:
                return static_cast<int64_t>(doubleParam());
        }
    }

    double doubleParam() {
        tVariant* p = currentParam();
        if (p->vt == VTYPE_R4 || p->vt == VTYPE_R8) {
            m_index++;
            return p->dblVal;
        }
        throw TypeError(m_index, "number", p->vt);
    }

    bool boolParam() {
        tVariant* p = currentParam();
        if (p->vt == VTYPE_BOOL) {
            m_index++;
            return p->bVal;
        }
        throw TypeError(m_index, "bool", p->vt);
    }

    // --- Запись результата ---

    void setEmptyResult(tVariant* param = nullptr) {
        checkResultParam(param);
    }

    void setBoolResult(bool value, tVariant* param = nullptr) {
        param = checkResultParam(param);
        param->vt = VTYPE_BOOL;
        param->bVal = value;
    }

    void setIntResult(int value, tVariant* param = nullptr) {
        param = checkResultParam(param);
        param->vt = VTYPE_I4;
        param->lVal = value;
    }

    void setLongResult(int64_t value, tVariant* param = nullptr) {
        setDoubleResult(static_cast<double>(value), param);
    }

    void setDoubleResult(double value, tVariant* param = nullptr) {
        param = checkResultParam(param);
        param->vt = VTYPE_R8;
        param->dblVal = value;
    }

    void setStringResult(const std::u16string& value, tVariant* param = nullptr, bool nullable = false) {
        param = checkResultParam(param);
        if (value.empty() && nullable) return;
        if (!m_memManager.variantFromString(param, value)) {
            throw std::runtime_error("Error saving string result to variant");
        }
    }

    void setStringOrEmptyResult(const std::u16string& value, tVariant* param = nullptr) {
        setStringResult(value, param, true);
    }

private:
    tVariant* checkResultParam(tVariant* param) {
        if (!param) param = m_retValue;
        if (!param) throw std::runtime_error("Return value variable is null");
        param->vt = VTYPE_EMPTY;
        return param;
    }

    MemoryManager& m_memManager;
    tVariant* m_params;
    tVariant* m_retValue;
    long m_count;
    int m_index;
    std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> m_converter;
};

} // namespace AddIn1S
} // namespace BlackRabbitMQ
