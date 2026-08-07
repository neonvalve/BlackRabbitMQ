#pragma once

#include "sdk/ComponentBase.h"
#include "sdk/AddInDefBase.h"

#include <memory>
#include <string>

namespace BlackRabbitMQ {
namespace AddIn1S {

class RabbitApi1S;

// Точка входа Native API компоненты для 1С.
// Реализует IComponentBase: регистрирует методы/свойства и диспетчеризует вызовы.
class RabbitMQClientNative : public IComponentBase {
public:
    static const WCHAR_T* componentName;

    // Имена свойств и методов платформа освобождает через IMemoryManager,
    // поэтому возвращать статические литералы нельзя: 1С отвечает на это
    // «Некорректная работа компоненты с памятью» и отказывается подключать
    // компоненту (видно только в журнале регистрации, метод подключения
    // просто возвращает Ложь).
    const WCHAR_T* allocName(const char16_t* name);

    enum Props {
        ePropVersion = 0,
        ePropCorrelationId,
        ePropType,
        ePropMessageId,
        ePropAppId,
        ePropContentEncoding,
        ePropContentType,
        ePropUserId,
        ePropClusterId,
        ePropExpiration,
        ePropReplyTo,
        // Настройки защищённого соединения. Задаются до Connect: в момент
        // рукопожатия политику проверки менять поздно.
        ePropSslCaFile,
        ePropSslVerifyPeer,
        ePropSslVerifyHostname,
        // Интервал heartbeat в секундах: -1 — как предложит брокер (умолчание),
        // 0 — выключить, >0 — своё значение. Задаётся до Connect.
        ePropHeartbeat,
        // Автоматическое переподключение после обрыва.
        ePropAutoReconnect,
        ePropReconnectDelayMs,
        ePropReconnectMaxDelayMs,
        ePropReconnectCount,        // только чтение: сколько раз подняли связь
        // Журнал компоненты: путь к файлу и уровень
        // ("off", "error", "warn", "info", "debug").
        ePropLogFile,
        ePropLogLevel,
        ePropLast
    };

    enum Methods {
        eMethGetLastError = 0,
        eMethConnect,
        eMethDeclareQueue,
        eMethBasicPublish,
        eMethBasicConsume,
        eMethBasicConsumeMessage,
        eMethBasicCancel,
        eMethBasicAck,
        eMethDeleteQueue,
        eMethBindQueue,
        eMethBasicReject,
        eMethDeclareExchange,
        eMethDeleteExchange,
        eMethUnbindQueue,
        eMethSetPriority,
        eMethGetPriority,
        eMethGetRoutingKey,
        eMethGetHeaders,
        eMethSleepNative,
        eMethEnableExternalEvent,
        eMethReconnect,
        eMethSetPublishMode,
        // Состояние очереди со слов брокера и её очистка: то, что спрашивают
        // при разборе «обмен встал» — сколько накопилось и как убрать мусор.
        eMethGetQueueInfo,
        eMethPurgeQueue,
        // Дождаться подтверждений пакетной публикации.
        eMethFlushPublish,
        eMethLast
    };

    RabbitMQClientNative();
    virtual ~RabbitMQClientNative();

    // IInitDoneBase
    virtual bool ADDIN_API Init(void* pConnection) override;
    virtual bool ADDIN_API setMemManager(void* mem) override;
    virtual long ADDIN_API GetInfo() override;
    virtual void ADDIN_API Done() override;

    // ILanguageExtenderBase
    virtual bool ADDIN_API RegisterExtensionAs(WCHAR_T** wsExtensionName) override;
    virtual long ADDIN_API GetNProps() override;
    virtual long ADDIN_API FindProp(const WCHAR_T* wsPropName) override;
    virtual const WCHAR_T* ADDIN_API GetPropName(long lPropNum, long lPropAlias) override;
    virtual bool ADDIN_API GetPropVal(const long lPropNum, tVariant* pvarPropVal) override;
    virtual bool ADDIN_API SetPropVal(const long lPropNum, tVariant* varPropVal) override;
    virtual bool ADDIN_API IsPropReadable(const long lPropNum) override;
    virtual bool ADDIN_API IsPropWritable(const long lPropNum) override;
    virtual long ADDIN_API GetNMethods() override;
    virtual long ADDIN_API FindMethod(const WCHAR_T* wsMethodName) override;
    virtual const WCHAR_T* ADDIN_API GetMethodName(const long lMethodNum, const long lMethodAlias) override;
    virtual long ADDIN_API GetNParams(const long lMethodNum) override;
    virtual bool ADDIN_API GetParamDefValue(const long lMethodNum, const long lParamNum, tVariant* pvarParamDefValue) override;
    virtual bool ADDIN_API HasRetVal(const long lMethodNum) override;
    virtual bool ADDIN_API CallAsProc(const long lMethodNum, tVariant* paParams, const long lSizeArray) override;
    virtual bool ADDIN_API CallAsFunc(const long lMethodNum, tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray) override;

    // LocaleBase
    virtual void ADDIN_API SetLocale(const WCHAR_T* loc) override;
    virtual void ADDIN_API SetUserInterfaceLanguageCode(const WCHAR_T* lang) override {}

private:
    std::unique_ptr<RabbitApi1S> m_impl;
};

} // namespace AddIn1S
} // namespace BlackRabbitMQ
