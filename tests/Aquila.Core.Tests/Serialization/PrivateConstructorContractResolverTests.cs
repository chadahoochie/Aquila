using Newtonsoft.Json;
using Shouldly;
using Aquila.Core.Serialization;

namespace Aquila.Core.Tests.Serialization;

public sealed class PrivateConstructorContractResolverTests
{
    private sealed class DddValueObject
    {
        public string Name { get; }
        public int Age { get; }

        private DddValueObject(string name, int age)
        {
            Name = name;
            Age = age;
        }

        public static DddValueObject Create(string name, int age) => new(name, age);
    }

    private sealed class MultiPrivateCtorClass
    {
        public string Code { get; }
        public string Description { get; }
        public int Priority { get; }

        private MultiPrivateCtorClass(string code) : this(code, string.Empty, 0) { }

        private MultiPrivateCtorClass(string code, string description, int priority)
        {
            Code = code;
            Description = description;
            Priority = priority;
        }
    }

    private sealed class PublicCtorClass
    {
        public string Value { get; set; } = string.Empty;

        public PublicCtorClass() { }

        public PublicCtorClass(string value)
        {
            Value = value;
        }
    }

    private sealed class ParameterlessPrivateCtorClass
    {
        public string Value { get; set; } = string.Empty;

        private ParameterlessPrivateCtorClass() { }

        public static ParameterlessPrivateCtorClass Create(string val) => new() { Value = val };
    }

    private sealed record ImmutablePositionalRecord(string Id, string Description, decimal Price);

    [Fact]
    public void Deserializes_Type_With_Private_Parameterized_Constructor()
    {
        var json = "{\"Name\":\"Alice\",\"Age\":30}";
        var result = JsonConvert.DeserializeObject<DddValueObject>(json, PrivateConstructorContractResolver.Settings);

        result.ShouldNotBeNull();
        result.Name.ShouldBe("Alice");
        result.Age.ShouldBe(30);
    }

    [Fact]
    public void Deserializes_Type_With_Multiple_Private_Constructors_Selecting_Most_Parameters()
    {
        var json = "{\"Code\":\"ORD-1\",\"Description\":\"Urgent\",\"Priority\":1}";
        var result = JsonConvert.DeserializeObject<MultiPrivateCtorClass>(json, PrivateConstructorContractResolver.Settings);

        result.ShouldNotBeNull();
        result.Code.ShouldBe("ORD-1");
        result.Description.ShouldBe("Urgent");
        result.Priority.ShouldBe(1);
    }

    [Fact]
    public void Deserializes_Type_With_Public_Constructors_Normally()
    {
        var json = "{\"Value\":\"PublicValue\"}";
        var result = JsonConvert.DeserializeObject<PublicCtorClass>(json, PrivateConstructorContractResolver.Settings);

        result.ShouldNotBeNull();
        result.Value.ShouldBe("PublicValue");
    }

    [Fact]
    public void Deserializes_Type_With_Private_Parameterless_Constructor()
    {
        var json = "{\"Value\":\"ParamLess\"}";
        var result = JsonConvert.DeserializeObject<ParameterlessPrivateCtorClass>(json, PrivateConstructorContractResolver.Settings);

        result.ShouldNotBeNull();
        result.Value.ShouldBe("ParamLess");
    }

    [Fact]
    public void Deserializes_Positional_Record_Without_Breaking_Copy_Constructor()
    {
        var original = new ImmutablePositionalRecord("REC-1", "Test item", 99.99m);
        var json = JsonConvert.SerializeObject(original, PrivateConstructorContractResolver.Settings);
        var deserialized = JsonConvert.DeserializeObject<ImmutablePositionalRecord>(json, PrivateConstructorContractResolver.Settings);

        deserialized.ShouldNotBeNull();
        deserialized.Id.ShouldBe("REC-1");
        deserialized.Description.ShouldBe("Test item");
        deserialized.Price.ShouldBe(99.99m);
    }
}
