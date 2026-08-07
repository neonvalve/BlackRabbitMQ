#include "RabbitMQClientNative.h"
#include "RabbitApi1S.h"

#include <cstring>
#include <string>

namespace BlackRabbitMQ {
namespace AddIn1S {

const WCHAR_T* RabbitMQClientNative::componentName = u"BlackRabbitMQ";

// --- Init / Done ---

RabbitMQClientNative::RabbitMQClientNative() {
    m_impl.reset(new RabbitApi1S());
}

RabbitMQClientNative::~RabbitMQClientNative() = default;

bool RabbitMQClientNative::Init(void* pConnection) {
    return m_impl->init(static_cast<IAddInDefBase*>(pConnection));
}

bool RabbitMQClientNative::setMemManager(void* mem) {
    m_impl->setMemoryManager(static_cast<IMemoryManager*>(mem));
    return mem != nullptr;
}

long RabbitMQClientNative::GetInfo() {
    return 2000; // 2.0
}

void RabbitMQClientNative::Done() {
    m_impl->done();
}

// --- Properties ---

bool RabbitMQClientNative::RegisterExtensionAs(WCHAR_T** wsExtensionName) {
    return m_impl->memoryManager().copyString(
        reinterpret_cast<char16_t**>(wsExtensionName),
        reinterpret_cast<const char16_t*>(componentName));
}

long RabbitMQClientNative::GetNProps() {
    return ePropLast;
}

long RabbitMQClientNative::FindProp(const WCHAR_T* wsPropName) {
    auto* name = reinterpret_cast<const char16_t*>(wsPropName);

    if (std::u16string(name) == u"Version") return ePropVersion;
    if (std::u16string(name) == u"CorrelationId") return ePropCorrelationId;
    if (std::u16string(name) == u"Type") return ePropType;
    if (std::u16string(name) == u"MessageId") return ePropMessageId;
    if (std::u16string(name) == u"AppId") return ePropAppId;
    if (std::u16string(name) == u"ContentEncoding") return ePropContentEncoding;
    if (std::u16string(name) == u"ContentType") return ePropContentType;
    if (std::u16string(name) == u"UserId") return ePropUserId;
    if (std::u16string(name) == u"ClusterId") return ePropClusterId;
    if (std::u16string(name) == u"Expiration") return ePropExpiration;
    if (std::u16string(name) == u"ReplyTo") return ePropReplyTo;
    if (std::u16string(name) == u"SslCaFile") return ePropSslCaFile;
    if (std::u16string(name) == u"SslVerifyPeer") return ePropSslVerifyPeer;
    if (std::u16string(name) == u"SslVerifyHostname") return ePropSslVerifyHostname;
    if (std::u16string(name) == u"Heartbeat") return ePropHeartbeat;
    if (std::u16string(name) == u"AutoReconnect") return ePropAutoReconnect;
    if (std::u16string(name) == u"ReconnectDelayMs") return ePropReconnectDelayMs;
    if (std::u16string(name) == u"ReconnectMaxDelayMs") return ePropReconnectMaxDelayMs;
    if (std::u16string(name) == u"ReconnectCount") return ePropReconnectCount;
    if (std::u16string(name) == u"LogFile") return ePropLogFile;
    if (std::u16string(name) == u"LogLevel") return ePropLogLevel;

    return -1;
}

// Строку отдаём в памяти 1С: платформа освободит её сама через FreeMemory.
const WCHAR_T* RabbitMQClientNative::allocName(const char16_t* name) {
    if (!name) return nullptr;
    char16_t* copy = nullptr;
    if (!m_impl->memoryManager().copyString(&copy, name)) return nullptr;
    return reinterpret_cast<const WCHAR_T*>(copy);
}

const WCHAR_T* RabbitMQClientNative::GetPropName(long lPropNum, long /*lPropAlias*/) {
    switch (lPropNum) {
        case ePropVersion:          return allocName(u"Version");
        case ePropCorrelationId:    return allocName(u"CorrelationId");
        case ePropType:             return allocName(u"Type");
        case ePropMessageId:        return allocName(u"MessageId");
        case ePropAppId:            return allocName(u"AppId");
        case ePropContentEncoding:  return allocName(u"ContentEncoding");
        case ePropContentType:      return allocName(u"ContentType");
        case ePropUserId:           return allocName(u"UserId");
        case ePropClusterId:        return allocName(u"ClusterId");
        case ePropExpiration:       return allocName(u"Expiration");
        case ePropReplyTo:          return allocName(u"ReplyTo");
        case ePropSslCaFile:        return allocName(u"SslCaFile");
        case ePropSslVerifyPeer:    return allocName(u"SslVerifyPeer");
        case ePropSslVerifyHostname: return allocName(u"SslVerifyHostname");
        case ePropHeartbeat:        return allocName(u"Heartbeat");
        case ePropAutoReconnect:    return allocName(u"AutoReconnect");
        case ePropReconnectDelayMs: return allocName(u"ReconnectDelayMs");
        case ePropReconnectMaxDelayMs: return allocName(u"ReconnectMaxDelayMs");
        case ePropReconnectCount:   return allocName(u"ReconnectCount");
        case ePropLogFile:          return allocName(u"LogFile");
        case ePropLogLevel:         return allocName(u"LogLevel");
        default: return nullptr;
    }
}

bool RabbitMQClientNative::GetPropVal(const long lPropNum, tVariant* pvarPropVal) {
    switch (lPropNum) {
        case ePropVersion:
            return m_impl->getVersion(pvarPropVal);
        case ePropCorrelationId:
        case ePropType:
        case ePropMessageId:
        case ePropAppId:
        case ePropContentEncoding:
        case ePropContentType:
        case ePropUserId:
        case ePropClusterId:
        case ePropExpiration:
        case ePropReplyTo: {
            // Создать временный CallContext для чтения свойства
            CallContext ctx(m_impl->memoryManager(), nullptr, 0, pvarPropVal);
            m_impl->getMsgPropImpl(lPropNum, ctx);
            return true;
        }
        case ePropSslCaFile:
        case ePropSslVerifyPeer:
        case ePropSslVerifyHostname:
        case ePropHeartbeat:
        case ePropAutoReconnect:
        case ePropReconnectDelayMs:
        case ePropReconnectMaxDelayMs:
        case ePropReconnectCount:
        case ePropLogFile:
        case ePropLogLevel: {
            CallContext ctx(m_impl->memoryManager(), nullptr, 0, pvarPropVal);
            m_impl->getTlsPropImpl(lPropNum, ctx);
            return true;
        }
        default:
            return false;
    }
}

bool RabbitMQClientNative::SetPropVal(const long lPropNum, tVariant* varPropVal) {
    switch (lPropNum) {
        case ePropCorrelationId:
        case ePropType:
        case ePropMessageId:
        case ePropAppId:
        case ePropContentEncoding:
        case ePropContentType:
        case ePropUserId:
        case ePropClusterId:
        case ePropExpiration:
        case ePropReplyTo: {
            CallContext ctx(m_impl->memoryManager(), varPropVal, 1);
            m_impl->setMsgPropImpl(lPropNum, ctx);
            return true;
        }
        case ePropSslCaFile:
        case ePropSslVerifyPeer:
        case ePropSslVerifyHostname:
        case ePropHeartbeat:
        case ePropAutoReconnect:
        case ePropReconnectDelayMs:
        case ePropReconnectMaxDelayMs:
        case ePropReconnectCount:
        case ePropLogFile:
        case ePropLogLevel: {
            CallContext ctx(m_impl->memoryManager(), varPropVal, 1);
            m_impl->setTlsPropImpl(lPropNum, ctx);
            return true;
        }
        default:
            return false;
    }
}

bool RabbitMQClientNative::IsPropReadable(const long /*lPropNum*/) {
    return true;
}

bool RabbitMQClientNative::IsPropWritable(const long lPropNum) {
    // ReconnectCount — счётчик компоненты, писать его извне бессмысленно.
    if (lPropNum == ePropReconnectCount) return false;
    return lPropNum >= ePropCorrelationId && lPropNum < ePropLast;
}

// --- Methods ---

long RabbitMQClientNative::GetNMethods() {
    return eMethLast;
}

long RabbitMQClientNative::FindMethod(const WCHAR_T* wsMethodName) {
    auto* name = reinterpret_cast<const char16_t*>(wsMethodName);

    if (std::u16string(name) == u"GetLastError")         return eMethGetLastError;
    if (std::u16string(name) == u"Connect")              return eMethConnect;
    if (std::u16string(name) == u"DeclareQueue")         return eMethDeclareQueue;
    if (std::u16string(name) == u"BasicPublish")         return eMethBasicPublish;
    if (std::u16string(name) == u"BasicConsume")         return eMethBasicConsume;
    if (std::u16string(name) == u"BasicConsumeMessage")  return eMethBasicConsumeMessage;
    if (std::u16string(name) == u"BasicCancel")          return eMethBasicCancel;
    if (std::u16string(name) == u"BasicAck")             return eMethBasicAck;
    if (std::u16string(name) == u"DeleteQueue")          return eMethDeleteQueue;
    if (std::u16string(name) == u"BindQueue")            return eMethBindQueue;
    if (std::u16string(name) == u"BasicReject")          return eMethBasicReject;
    if (std::u16string(name) == u"DeclareExchange")      return eMethDeclareExchange;
    if (std::u16string(name) == u"DeleteExchange")       return eMethDeleteExchange;
    if (std::u16string(name) == u"UnbindQueue")          return eMethUnbindQueue;
    if (std::u16string(name) == u"SetPriority")          return eMethSetPriority;
    if (std::u16string(name) == u"GetPriority")          return eMethGetPriority;
    if (std::u16string(name) == u"GetRoutingKey")        return eMethGetRoutingKey;
    if (std::u16string(name) == u"GetHeaders")           return eMethGetHeaders;
    if (std::u16string(name) == u"SleepNative")          return eMethSleepNative;
    if (std::u16string(name) == u"EnableExternalEvent")  return eMethEnableExternalEvent;
    if (std::u16string(name) == u"Reconnect")            return eMethReconnect;
    if (std::u16string(name) == u"SetPublishMode")      return eMethSetPublishMode;
    if (std::u16string(name) == u"GetQueueInfo")        return eMethGetQueueInfo;
    if (std::u16string(name) == u"PurgeQueue")          return eMethPurgeQueue;

    return -1;
}

const WCHAR_T* RabbitMQClientNative::GetMethodName(const long lMethodNum, const long /*lMethodAlias*/) {
    switch (lMethodNum) {
        case eMethGetLastError:         return allocName(u"GetLastError");
        case eMethConnect:              return allocName(u"Connect");
        case eMethDeclareQueue:         return allocName(u"DeclareQueue");
        case eMethBasicPublish:         return allocName(u"BasicPublish");
        case eMethBasicConsume:         return allocName(u"BasicConsume");
        case eMethBasicConsumeMessage:  return allocName(u"BasicConsumeMessage");
        case eMethBasicCancel:          return allocName(u"BasicCancel");
        case eMethBasicAck:             return allocName(u"BasicAck");
        case eMethDeleteQueue:          return allocName(u"DeleteQueue");
        case eMethBindQueue:            return allocName(u"BindQueue");
        case eMethBasicReject:          return allocName(u"BasicReject");
        case eMethDeclareExchange:      return allocName(u"DeclareExchange");
        case eMethDeleteExchange:       return allocName(u"DeleteExchange");
        case eMethUnbindQueue:          return allocName(u"UnbindQueue");
        case eMethSetPriority:          return allocName(u"SetPriority");
        case eMethGetPriority:          return allocName(u"GetPriority");
        case eMethGetRoutingKey:        return allocName(u"GetRoutingKey");
        case eMethGetHeaders:           return allocName(u"GetHeaders");
        case eMethSleepNative:          return allocName(u"SleepNative");
        case eMethEnableExternalEvent:  return allocName(u"EnableExternalEvent");
        case eMethReconnect:            return allocName(u"Reconnect");
        case eMethSetPublishMode:       return allocName(u"SetPublishMode");
        case eMethGetQueueInfo:         return allocName(u"GetQueueInfo");
        case eMethPurgeQueue:           return allocName(u"PurgeQueue");
        default: return nullptr;
    }
}

long RabbitMQClientNative::GetNParams(const long lMethodNum) {
    switch (lMethodNum) {
        case eMethConnect:              return 8;
        case eMethDeclareQueue:         return 7;
        case eMethBasicPublish:
        case eMethDeclareExchange:
        case eMethBasicConsume:         return 6;
        case eMethBasicConsumeMessage:
        case eMethBindQueue:            return 4;
        case eMethDeleteQueue:
        case eMethUnbindQueue:          return 3;
        case eMethDeleteExchange:       return 2;
        case eMethBasicAck:
        case eMethSetPriority:
        case eMethSleepNative:          return 1;
        case eMethBasicCancel:
        case eMethReconnect:            return 0;
        case eMethBasicReject:          return 2;  // tag + requeue (НОВЫЙ!)
        case eMethEnableExternalEvent:  return 1;  // bool enable
        case eMethSetPublishMode:       return 1;  // "confirms" | "transactions"
        case eMethGetQueueInfo:
        case eMethPurgeQueue:           return 1;  // имя очереди
        default: return 0;
    }
}

bool RabbitMQClientNative::GetParamDefValue(const long lMethodNum, const long lParamNum,
                                             tVariant* pvarParamDefValue) {
    // Установить значения по умолчанию для необязательных параметров
    switch (lMethodNum) {
        case eMethConnect:
            if (lParamNum == 6) { // ssl
                TV_VT(pvarParamDefValue) = VTYPE_BOOL;
                TV_BOOL(pvarParamDefValue) = false;
                return true;
            }
            if (lParamNum == 7) { // timeout
                TV_VT(pvarParamDefValue) = VTYPE_I4;
                TV_I4(pvarParamDefValue) = 5;
                return true;
            }
            break;
        case eMethDeclareQueue:
            if (lParamNum == 5) { // maxPriority
                TV_VT(pvarParamDefValue) = VTYPE_I4;
                TV_I4(pvarParamDefValue) = 0;
                return true;
            }
            if (lParamNum == 6) { // propsJson
                TV_VT(pvarParamDefValue) = VTYPE_PWSTR;
                TV_WSTR(pvarParamDefValue) = nullptr;
                pvarParamDefValue->wstrLen = 0;
                return true;
            }
            break;
        case eMethBasicPublish:
        case eMethDeclareExchange:
        case eMethBasicConsume:
            if (lParamNum == 5) { // propsJson
                TV_VT(pvarParamDefValue) = VTYPE_PWSTR;
                TV_WSTR(pvarParamDefValue) = nullptr;
                pvarParamDefValue->wstrLen = 0;
                return true;
            }
            break;
        case eMethBindQueue:
            if (lParamNum == 3) { // propsJson
                TV_VT(pvarParamDefValue) = VTYPE_PWSTR;
                TV_WSTR(pvarParamDefValue) = nullptr;
                pvarParamDefValue->wstrLen = 0;
                return true;
            }
            break;
        case eMethBasicReject:
            if (lParamNum == 1) { // requeue (НОВЫЙ!)
                TV_VT(pvarParamDefValue) = VTYPE_BOOL;
                TV_BOOL(pvarParamDefValue) = false;
                return true;
            }
            break;
        case eMethEnableExternalEvent:
            if (lParamNum == 0) {
                TV_VT(pvarParamDefValue) = VTYPE_BOOL;
                TV_BOOL(pvarParamDefValue) = false;
                return true;
            }
            break;
    }
    return false;
}

bool RabbitMQClientNative::HasRetVal(const long lMethodNum) {
    switch (lMethodNum) {
        case eMethGetLastError:
        case eMethBasicConsume:
        case eMethBasicConsumeMessage:
        case eMethDeclareQueue:
        case eMethGetPriority:
        case eMethGetRoutingKey:
        case eMethGetHeaders:
        case eMethGetQueueInfo:
        case eMethPurgeQueue:
            return true;
        default:
            return false;
    }
}

// --- Вызовы методов (Proc = без возвращаемого значения) ---

bool RabbitMQClientNative::CallAsProc(const long lMethodNum,
                                       tVariant* paParams, const long lSizeArray) {
    switch (lMethodNum) {
        case eMethConnect:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::connectImpl, paParams, lSizeArray);
        case eMethBasicPublish:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::basicPublishImpl, paParams, lSizeArray);
        case eMethBasicCancel:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::basicCancelImpl, paParams, lSizeArray);
        case eMethBasicAck:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::basicAckImpl, paParams, lSizeArray);
        case eMethBasicReject:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::basicRejectImpl, paParams, lSizeArray);
        case eMethSetPublishMode:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::setPublishModeImpl, paParams, lSizeArray);
        case eMethDeleteQueue:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::deleteQueueImpl, paParams, lSizeArray);
        case eMethBindQueue:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::bindQueueImpl, paParams, lSizeArray);
        case eMethUnbindQueue:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::unbindQueueImpl, paParams, lSizeArray);
        case eMethDeclareExchange:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::declareExchangeImpl, paParams, lSizeArray);
        case eMethDeleteExchange:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::deleteExchangeImpl, paParams, lSizeArray);
        case eMethSetPriority:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::setPriorityImpl, paParams, lSizeArray);
        case eMethSleepNative:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::sleepNativeImpl, paParams, lSizeArray);
        case eMethEnableExternalEvent:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::enableExternalEventImpl, paParams, lSizeArray);
        case eMethReconnect:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::reconnectImpl, paParams, lSizeArray);
        default:
            return false;
    }
}

// --- Вызовы функций (Func = с возвращаемым значением) ---

bool RabbitMQClientNative::CallAsFunc(const long lMethodNum,
                                       tVariant* pvarRetValue, tVariant* paParams,
                                       const long lSizeArray) {
    switch (lMethodNum) {
        case eMethGetLastError:
            return m_impl->getLastError(pvarRetValue, paParams, lSizeArray);
        case eMethBasicConsume:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::basicConsumeImpl,
                                    paParams, lSizeArray, pvarRetValue);
        case eMethBasicConsumeMessage:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::basicConsumeMessageImpl,
                                    paParams, lSizeArray, pvarRetValue);
        case eMethDeclareQueue:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::declareQueueImpl,
                                    paParams, lSizeArray, pvarRetValue);
        case eMethGetPriority:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::getPriorityImpl,
                                    paParams, lSizeArray, pvarRetValue);
        case eMethGetRoutingKey:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::getRoutingKeyImpl,
                                    paParams, lSizeArray, pvarRetValue);
        case eMethGetQueueInfo:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::getQueueInfoImpl,
                                    paParams, lSizeArray, pvarRetValue);
        case eMethPurgeQueue:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::purgeQueueImpl,
                                    paParams, lSizeArray, pvarRetValue);
        case eMethGetHeaders:
            return m_impl->wrapCall(m_impl.get(), &RabbitApi1S::getHeadersImpl,
                                    paParams, lSizeArray, pvarRetValue);
        default:
            return false;
    }
}

void RabbitMQClientNative::SetLocale(const WCHAR_T* /*loc*/) {
    // Не требуется на Linux/macOS
}

} // namespace AddIn1S
} // namespace BlackRabbitMQ
