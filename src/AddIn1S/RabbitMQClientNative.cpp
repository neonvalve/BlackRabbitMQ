#include "RabbitMQClientNative.h"
#include "RabbitApi1S.h"

#include <cstring>
#include <string>

namespace BlackRabbitMQ {
namespace AddIn1S {

const WCHAR_T* RabbitMQClientNative::componentName = L"BlackRabbitMQ";

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

    return -1;
}

const WCHAR_T* RabbitMQClientNative::GetPropName(long lPropNum, long /*lPropAlias*/) {
    switch (lPropNum) {
        case ePropVersion:          return L"Version";
        case ePropCorrelationId:    return L"CorrelationId";
        case ePropType:             return L"Type";
        case ePropMessageId:        return L"MessageId";
        case ePropAppId:            return L"AppId";
        case ePropContentEncoding:  return L"ContentEncoding";
        case ePropContentType:      return L"ContentType";
        case ePropUserId:           return L"UserId";
        case ePropClusterId:        return L"ClusterId";
        case ePropExpiration:       return L"Expiration";
        case ePropReplyTo:          return L"ReplyTo";
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
        default:
            return false;
    }
}

bool RabbitMQClientNative::IsPropReadable(const long /*lPropNum*/) {
    return true;
}

bool RabbitMQClientNative::IsPropWritable(const long lPropNum) {
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

    return -1;
}

const WCHAR_T* RabbitMQClientNative::GetMethodName(const long lMethodNum, const long /*lMethodAlias*/) {
    switch (lMethodNum) {
        case eMethGetLastError:         return L"GetLastError";
        case eMethConnect:              return L"Connect";
        case eMethDeclareQueue:         return L"DeclareQueue";
        case eMethBasicPublish:         return L"BasicPublish";
        case eMethBasicConsume:         return L"BasicConsume";
        case eMethBasicConsumeMessage:  return L"BasicConsumeMessage";
        case eMethBasicCancel:          return L"BasicCancel";
        case eMethBasicAck:             return L"BasicAck";
        case eMethDeleteQueue:          return L"DeleteQueue";
        case eMethBindQueue:            return L"BindQueue";
        case eMethBasicReject:          return L"BasicReject";
        case eMethDeclareExchange:      return L"DeclareExchange";
        case eMethDeleteExchange:       return L"DeleteExchange";
        case eMethUnbindQueue:          return L"UnbindQueue";
        case eMethSetPriority:          return L"SetPriority";
        case eMethGetPriority:          return L"GetPriority";
        case eMethGetRoutingKey:        return L"GetRoutingKey";
        case eMethGetHeaders:           return L"GetHeaders";
        case eMethSleepNative:          return L"SleepNative";
        case eMethEnableExternalEvent:  return L"EnableExternalEvent";
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
        case eMethBasicCancel:
        case eMethBasicAck:
        case eMethSetPriority:
        case eMethSleepNative:          return 1;
        case eMethBasicReject:          return 2;  // tag + requeue (НОВЫЙ!)
        case eMethEnableExternalEvent:  return 1;  // bool enable
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
