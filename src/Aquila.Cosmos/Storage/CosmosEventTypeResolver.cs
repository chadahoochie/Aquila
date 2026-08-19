using System.Reflection;
using Aquila.Core.Serialization;
using Aquila.Core.Events;

namespace Aquila.Cosmos.Storage;

public interface ICosmosEventTypeResolver
{
    void EnsureTypedPayload(EventEnvelope<object> evt);
    Type? ResolveEventType(string eventTypeName);
}

public sealed class CosmosEventTypeResolver : ICosmosEventTypeResolver
{
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, Type?> _eventTypeCache = new();

    public static CosmosEventTypeResolver Default { get; } = new();

    public void EnsureTypedPayload(EventEnvelope<object> evt)
    {
        if (evt.Data == null) return;

        Type? targetType = null;
        string? rawJson = null;

        if (evt.Data is Newtonsoft.Json.Linq.JToken jToken)
        {
            targetType = ResolveEventType(evt.EventType);
            rawJson = jToken.ToString(Newtonsoft.Json.Formatting.None);
        }
        else if (evt.Data is System.Text.Json.JsonElement jsonElement)
        {
            targetType = ResolveEventType(evt.EventType);
            rawJson = jsonElement.GetRawText();
        }

        if (targetType == null || rawJson == null) return;

        var deserialized = Newtonsoft.Json.JsonConvert.DeserializeObject(rawJson, targetType, PrivateConstructorContractResolver.Settings);
        if (deserialized != null)
        {
            evt.Data = deserialized;
        }
    }

    public Type? ResolveEventType(string eventTypeName)
    {
        if (string.IsNullOrWhiteSpace(eventTypeName)) return null;

        return _eventTypeCache.GetOrAdd(eventTypeName, name =>
        {
            var type = Type.GetType(name);
            if (type != null) return type;

            foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
            {
                if (asm.IsDynamic) continue;

                try
                {
                    type = asm.GetType(name);
                    if (type != null) return type;
                }
                catch
                {
                    // Ignore type load or argument errors
                }

                try
                {
                    type = asm.GetTypes().FirstOrDefault(t => t.Name == name || t.FullName == name);
                    if (type != null) return type;
                }
                catch (ReflectionTypeLoadException ex)
                {
                    type = ex.Types.FirstOrDefault(t => t != null && (t.Name == name || t.FullName == name));
                    if (type != null) return type;
                }
                catch
                {
                    // Ignore assemblies that throw exceptions during type scanning
                }
            }

            return null;
        });
    }
}
