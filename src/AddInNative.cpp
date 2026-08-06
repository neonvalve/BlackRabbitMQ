#include "AddIn1S/RabbitMQClientNative.h"
#include "AddIn1S/sdk/ComponentBase.h"

// Точки входа обязаны быть видимы 1С, иначе «ПодключитьВнешнююКомпоненту»
// падает с «не является компонентой».
// На Windows экспорт задаётся модульным файлом AddInNative.def: заголовки SDK
// объявляют эти функции без __declspec(dllexport), и добавить его на
// определении нельзя — MSVC отвечает C2375 «different linkage».
#if defined(_WIN32) || defined(_WIN64)
#define EXPORT
#elif defined(__GNUC__) || defined(__clang__)
#define EXPORT __attribute__ ((visibility ("default")))
#else
#define EXPORT
#endif

using namespace BlackRabbitMQ::AddIn1S;

static AppCapabilities g_capabilities = eAppCapabilitiesInvalid;

//---------------------------------------------------------------------------//
EXPORT long GetClassObject(const WCHAR_T* wsName, IComponentBase** pInterface) {
    if (!*pInterface) {
        *pInterface = new RabbitMQClientNative();
        return reinterpret_cast<long>(*pInterface);
    }
    return 0;
}

//---------------------------------------------------------------------------//
EXPORT AppCapabilities SetPlatformCapabilities(const AppCapabilities capabilities) {
    g_capabilities = capabilities;
    return eAppCapabilitiesLast;
}

//---------------------------------------------------------------------------//
EXPORT long DestroyObject(IComponentBase** pInterface) {
    if (!*pInterface) return -1;
    delete *pInterface;
    *pInterface = nullptr;
    return 0;
}

//---------------------------------------------------------------------------//
EXPORT const WCHAR_T* GetClassNames() {
    return RabbitMQClientNative::componentName;
}

//---------------------------------------------------------------------------//
EXPORT AttachType GetAttachType() {
    return AttachType::eCanAttachAny;
}
