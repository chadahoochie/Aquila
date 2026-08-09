using Aquila.Core.Patching;
using Shouldly;

namespace Aquila.Core.Tests.Patching;

public class TestNestedModel
{
    public string Name { get; set; } = string.Empty;
}

public class TestPatchModel
{
    public string Status { get; set; } = string.Empty;
    public int Count { get; set; }
    public TestNestedModel Nested { get; set; } = new();
    public List<string> Items { get; set; } = new();
}

public class PatchExpressionTests
{
    [Fact]
    public void BuildJsonPointerPath_Handles_UnaryConvert_At_Root()
    {
        var expr = new PatchExpression<TestPatchModel>();

        // Expression casting property access to object creates UnaryExpression (Convert) at root
        expr.Set<object>(x => x.Count, 10);

        expr.Operations.Count.ShouldBe(1);
        expr.Operations[0].Path.ShouldBe("/Data/Count");
        expr.Operations[0].Value.ShouldBe(10);
    }

    [Fact]
    public void BuildJsonPointerPath_Handles_UnaryConvert_On_Nested_Property()
    {
        var expr = new PatchExpression<TestPatchModel>();

        // Convert or ConvertChecked inside nested member expression
        expr.Set<object>(x => ((TestNestedModel)x.Nested).Name, "NewName");

        expr.Operations.Count.ShouldBe(1);
        expr.Operations[0].Path.ShouldBe("/Data/Nested/Name");
        expr.Operations[0].Value.ShouldBe("NewName");
    }

    [Fact]
    public void BuildJsonPointerPath_Throws_ArgumentException_When_No_Member_Expression()
    {
        var expr = new PatchExpression<TestPatchModel>();

        // Expression with no property access (parts.Count == 0)
        Should.Throw<ArgumentException>(() => expr.Set(x => x, new TestPatchModel()));
        Should.Throw<ArgumentException>(() => expr.Set(x => (object)x, new object()));
    }
}
