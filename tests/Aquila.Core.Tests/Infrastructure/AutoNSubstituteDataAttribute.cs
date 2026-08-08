using AutoFixture;
using AutoFixture.AutoNSubstitute;
using AutoFixture.Xunit3;

namespace Aquila.Core.Tests;

public sealed class AutoNSubstituteDataAttribute : AutoDataAttribute
{
    public AutoNSubstituteDataAttribute()
        : base(() => new Fixture().Customize(new AutoNSubstituteCustomization()))
    {
    }
}
