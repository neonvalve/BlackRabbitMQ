#pragma once

#include "sdk/types.h"
#include "sdk/IMemoryManager.h"
#include <string>
#include <cstring>

namespace BlackRabbitMQ {
namespace AddIn1S {

// Обёртка над IMemoryManager — контракт управления памятью 1С.
// Все строки, возвращаемые в 1С, должны аллоцироваться через этот класс.
class MemoryManager {
public:
    MemoryManager() : m_handle(nullptr) {}

    void setHandle(IMemoryManager* manager) { m_handle = manager; }
    IMemoryManager* handle() const noexcept { return m_handle; }

    // Аллоцировать память в куче 1С.
    bool alloc(void** pointer, size_t size) {
        return (m_handle && pointer && size)
            ? m_handle->AllocMemory(pointer, static_cast<unsigned long>(size))
            : false;
    }

    // Скопировать строку в память 1С.
    bool copyString(char16_t** pointer, const std::u16string& str) {
        size_t len = (str.length() + 1) * sizeof(char16_t);
        if (!alloc(reinterpret_cast<void**>(pointer), len)) {
            return false;
        }
        std::memcpy(*pointer, str.c_str(), len);
        return true;
    }

    // Записать строку в tVariant (1С-совместимый).
    bool variantFromString(tVariant* vOut, const std::u16string& str) {
        vOut->vt = VTYPE_EMPTY;
        size_t len = (str.length() + 1) * sizeof(char16_t);
        if (!alloc(reinterpret_cast<void**>(&vOut->pwstrVal), len)) {
            return false;
        }
        std::memcpy(vOut->pwstrVal, str.c_str(), len);
        vOut->wstrLen = static_cast<uint32_t>(str.length());
        vOut->vt = VTYPE_PWSTR;
        return true;
    }

private:
    IMemoryManager* m_handle;
};

} // namespace AddIn1S
} // namespace BlackRabbitMQ
