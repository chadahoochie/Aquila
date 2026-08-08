using System.Diagnostics.CodeAnalysis;
using AutoFixture;
using AutoFixture.AutoNSubstitute;
using AutoFixture.Xunit3;

namespace Aquila.Cosmos.Tests;

[ExcludeFromCodeCoverage]
public sealed class AutoNSubstituteDataAttribute : AutoDataAttribute
{
    public AutoNSubstituteDataAttribute()
        : base(() => new Fixture().Customize(new AutoNSubstituteCustomization()))
    {
    }
}
