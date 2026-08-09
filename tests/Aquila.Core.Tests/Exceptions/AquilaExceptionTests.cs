using Aquila.Core.Exceptions;
using Shouldly;

namespace Aquila.Core.Tests;

public sealed class AquilaExceptionTests
{
    [Fact]
    public void AquilaException_MessageOnly_Constructor_Sets_Message()
    {
        var ex = new AquilaException("something went wrong");

        ex.Message.ShouldBe("something went wrong");
        ex.InnerException.ShouldBeNull();
    }

    [Fact]
    public void AquilaException_MessageAndInner_Constructor_Sets_Both()
    {
        var inner = new InvalidOperationException("root cause");

        var ex = new AquilaException("wrapper message", inner);

        ex.Message.ShouldBe("wrapper message");
        ex.InnerException.ShouldBeSameAs(inner);
    }

    [Fact]
    public void AquilaConcurrencyException_FieldConstructor_Populates_Fields_And_Message()
    {
        var ex = new AquilaConcurrencyException("doc-1", "3", "5");

        ex.DocumentId.ShouldBe("doc-1");
        ex.ExpectedVersion.ShouldBe("3");
        ex.ActualVersion.ShouldBe("5");
        ex.Message.ShouldContain("doc-1");
        ex.Message.ShouldContain("3");
        ex.Message.ShouldContain("5");
    }

    [Fact]
    public void AquilaConcurrencyException_MessageAndInner_Constructor_Leaves_Fields_Empty()
    {
        var inner = new InvalidOperationException("cause");

        var ex = new AquilaConcurrencyException("concurrency failure", inner);

        ex.Message.ShouldBe("concurrency failure");
        ex.InnerException.ShouldBeSameAs(inner);
        ex.DocumentId.ShouldBe(string.Empty);
        ex.ExpectedVersion.ShouldBe(string.Empty);
        ex.ActualVersion.ShouldBe(string.Empty);
    }
}
