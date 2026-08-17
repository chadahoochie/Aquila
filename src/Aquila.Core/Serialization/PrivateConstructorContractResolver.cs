using System.Reflection;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Aquila.Core.Serialization;

/// <summary>
/// Domain value objects and events commonly follow the DDD idiom of a private constructor with
/// public static factory methods — invariants are enforced at creation, never via direct
/// construction. Newtonsoft's default constructor resolution only considers *public*
/// constructors for its "single constructor with arguments" fallback (non-public constructors
/// are only used when explicitly marked [JsonConstructor]), so every such type otherwise fails
/// to deserialize with "Unable to find a constructor to use". Rather than requiring every
/// consuming domain type to add a Newtonsoft-specific attribute, this resolver extends that same
/// fallback to also consider non-public constructors when no public/default one is usable.
/// </summary>
public sealed class PrivateConstructorContractResolver : DefaultContractResolver
{
    protected override JsonObjectContract CreateObjectContract(Type objectType)
    {
        var contract = base.CreateObjectContract(objectType);

        // contract.ParameterizedCreator (set by base when it finds a usable *public* multi-arg
        // constructor, e.g. a positional record's primary constructor) is protected internal on
        // Newtonsoft's side and unreadable here, so we can't just check "is a creator already
        // set" — we have to independently re-check whether base would already have found a public
        // constructor to use and bail out, otherwise we'd clobber an already-working constructor
        // (e.g. picking a record's non-public copy constructor over its public primary one).
        if (contract.OverrideCreator != null || contract.DefaultCreator != null
            || objectType.GetConstructors(BindingFlags.Instance | BindingFlags.Public).Any(c => c.GetParameters().Length > 0))
        {
            return contract;
        }

        var ctor = objectType
            .GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic)
            .OrderByDescending(c => c.GetParameters().Length)
            .FirstOrDefault();

        if (ctor == null)
        {
            return contract;
        }

        contract.OverrideCreator = args => ctor.Invoke(args);
        foreach (var property in CreateConstructorParameters(ctor, contract.Properties))
        {
            contract.CreatorParameters.AddProperty(property);
        }

        return contract;
    }

    /// <summary>
    /// Shared settings for every Newtonsoft entry point in this assembly (the <see cref="CosmosSerializer"/>
    /// used for document bodies, and the ad-hoc <c>JsonConvert.DeserializeObject</c> calls used for event
    /// envelopes) so domain types with private constructors round-trip consistently everywhere.
    /// </summary>
    public static readonly JsonSerializerSettings Settings = new()
    {
        ContractResolver = new PrivateConstructorContractResolver(),
        NullValueHandling = NullValueHandling.Ignore,
        Formatting = Formatting.None,
        Converters = new List<JsonConverter> { new Newtonsoft.Json.Converters.StringEnumConverter() }
    };
}
