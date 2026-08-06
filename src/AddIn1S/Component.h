#pragma once

#include "sdk/AddInDefBase.h"
#include "CallContext.h"
#include "MemoryManager.h"
#include "sdk/Error.hpp"

#include <codecvt>
#include <locale>
#include <mutex>
#include <string>

namespace BlackRabbitMQ {
namespace AddIn1S {

// Код ошибки Native API компоненты для 1С.
constexpr unsigned short NATIVE_ERROR = 1006;

// Версию задаёт CMake (project VERSION) — одно место на код, MANIFEST.XML и релиз.
#ifndef BLACKRABBITMQ_VERSION
#define BLACKRABBITMQ_VERSION "0.0.0-dev"
#endif

// Базовый класс для 1С-компоненты.
// Предоставляет: init/done, wrapCall/lastError, MemoryManager.
class Component {
public:
    explicit Component(const char* className)
        : m_addin(nullptr)
        , m_skipAddError(false)
        , m_version(BLACKRABBITMQ_VERSION)
    {
        m_className = m_converter.from_bytes(className);
    }

    virtual ~Component() = default;

    // Инициализация: 1С передаёт IAddInDefBase для AddError и MemoryManager.
    virtual bool init(IAddInDefBase* addin) {
        m_addin = addin;
        return true;
    }

    virtual void done() {}

    void setMemoryManager(IMemoryManager* manager) {
        m_memManager.setHandle(manager);
    }

    MemoryManager& memoryManager() { return m_memManager; }

    // --- GetLastError ---

    bool getLastError(tVariant* pvarRetValue, tVariant*, long) {
        std::u16string u16err = m_converter.from_bytes(m_lastError);
        return m_memManager.variantFromString(pvarRetValue, u16err);
    }

    bool getLastError(std::string* out) const {
        *out = m_lastError;
        return !m_lastError.empty();
    }

    // --- GetVersion ---

    bool getVersion(tVariant* pvarRetValue) {
        std::u16string uver = m_converter.from_bytes(m_version);
        return m_memManager.variantFromString(pvarRetValue, uver);
    }

protected:
    // Добавить ошибку в 1С (через AddError платформы).
    void addError(const std::string& descr, const std::string& source = "") {
        m_lastError = descr;
        if (m_skipAddError) return;

        std::u16string wdescr = m_converter.from_bytes(descr);
        std::u16string wsource = !source.empty()
            ? m_converter.from_bytes(source)
            : m_className;

        if (m_addin) {
            m_addin->AddError(NATIVE_ERROR, reinterpret_cast<const WCHAR_T*>(wsource.c_str()),
                              reinterpret_cast<const WCHAR_T*>(wdescr.c_str()), E_FAIL);
        }
    }

    void setSkipAddError(bool skip = true) { m_skipAddError = skip; }
    void setVersion(const std::string& version) { m_version = version; }

public:
    // Шаблон wrapCall: вызывает метод, ловит исключения, обновляет lastError.
    // Возвращает true при успехе, false при ошибке.
    template<typename T, typename Proc>
    bool wrapCall(T* obj, Proc proc, tVariant* paParams, long lSizeArray,
                  tVariant* pvarRetValue = nullptr)
    {
        bool result = false;
        try {
            // Единственная точка входа для вызовов из 1С — здесь же берётся
            // блокировка состояния компоненты. Фоновый сторож переподключения
            // работает с тем же состоянием и захватывает мьютекс через try_lock,
            // чтобы никогда не задерживать вызывающий поток платформы.
            std::lock_guard<std::mutex> lock(m_callMutex);
            m_skipAddError = false;
            m_lastError.clear();
            CallContext ctx(m_memManager, paParams, lSizeArray, pvarRetValue);
            (obj->*proc)(ctx);
            result = true;
        } catch (const std::exception& e) {
            addError(e.what(), typeid(e).name());
        }
        return result;
    }

    IAddInDefBase* addin() const { return m_addin; }

protected:
    // Защищает состояние компоненты между потоком платформы и фоновым сторожем
    // переподключения. Сторож берёт его только через try_lock.
    std::mutex m_callMutex;
    IAddInDefBase* m_addin;
    MemoryManager m_memManager;
    std::string m_lastError;
    bool m_skipAddError;
    std::u16string m_className;
    std::string m_version;
    std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> m_converter;
};

} // namespace AddIn1S
} // namespace BlackRabbitMQ
