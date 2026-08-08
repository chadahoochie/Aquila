using System;
using System.Threading.Tasks;
using Shouldly;
using Xunit;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests;

public class DirtyTrackingTestEntity
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public int Age { get; set; }
}

public class DirtyTrackingTests
{
    private readonly IDocumentStore _store;

    public DirtyTrackingTests()
    {
        var options = new StoreOptions();
        options.UseInMemoryStorage();
        _store = new DocumentStore(options);
    }

    [Fact]
    public async Task DirtyTracking_Mode_Automatically_Persists_Mutated_Entities_On_SaveChangesAsync()
    {
        // Arrange
        using (var setupSession = _store.OpenSession())
        {
            setupSession.Store(new DirtyTrackingTestEntity { Id = "dt-1", Name = "Original", Age = 30 });
            await setupSession.SaveChangesAsync();
        }

        // Act - Load and mutate entity in DirtyTracking session without calling Store()
        using (var session = _store.OpenSession(TrackingMode.DirtyTracking))
        {
            session.TrackingMode.ShouldBe(TrackingMode.DirtyTracking);
            var entity = await session.LoadAsync<DirtyTrackingTestEntity>("dt-1");
            entity.ShouldNotBeNull();
            entity.Name = "Mutated Automatically";
            entity.Age = 31;
            await session.SaveChangesAsync();
        }

        // Assert - Verify state was persisted automatically
        using (var verifySession = _store.OpenSession())
        {
            var reloaded = await verifySession.LoadAsync<DirtyTrackingTestEntity>("dt-1");
            reloaded.ShouldNotBeNull();
            reloaded.Name.ShouldBe("Mutated Automatically");
            reloaded.Age.ShouldBe(31);
        }
    }

    [Fact]
    public async Task IdentityMap_Mode_Requires_Explicit_Store_To_Persist_Changes()
    {
        // Arrange
        using (var setupSession = _store.OpenSession())
        {
            setupSession.Store(new DirtyTrackingTestEntity { Id = "im-1", Name = "Original IM", Age = 25 });
            await setupSession.SaveChangesAsync();
        }

        // Act - Load and mutate entity in IdentityMap mode without calling Store()
        using (var session = _store.OpenSession(TrackingMode.IdentityMap))
        {
            session.TrackingMode.ShouldBe(TrackingMode.IdentityMap);
            var entity = await session.LoadAsync<DirtyTrackingTestEntity>("im-1");
            entity.ShouldNotBeNull();
            entity.Name = "Mutated IM";
            await session.SaveChangesAsync();
        }

        // Assert - Changes should NOT be persisted because Store() was not called
        using (var verifySession = _store.OpenSession())
        {
            var reloaded = await verifySession.LoadAsync<DirtyTrackingTestEntity>("im-1");
            reloaded.ShouldNotBeNull();
            reloaded.Name.ShouldBe("Original IM");
        }

        // Act 2 - Mutate and explicitly call Store()
        using (var session = _store.OpenSession(TrackingMode.IdentityMap))
        {
            var entity = await session.LoadAsync<DirtyTrackingTestEntity>("im-1");
            entity.ShouldNotBeNull();
            entity.Name = "Explicitly Stored";
            session.Store(entity);
            await session.SaveChangesAsync();
        }

        // Assert 2 - Changes should be persisted
        using (var verifySession = _store.OpenSession())
        {
            var reloaded = await verifySession.LoadAsync<DirtyTrackingTestEntity>("im-1");
            reloaded.ShouldNotBeNull();
            reloaded.Name.ShouldBe("Explicitly Stored");
        }
    }

    [Fact]
    public async Task LightweightSession_Bypasses_IdentityMap_Allocations_And_Loads_Fresh_Instances()
    {
        // Arrange
        using (var setupSession = _store.OpenSession())
        {
            setupSession.Store(new DirtyTrackingTestEntity { Id = "lw-1", Name = "Lightweight Original", Age = 40 });
            await setupSession.SaveChangesAsync();
        }

        // Act & Assert
        using (var session = _store.LightweightSession())
        {
            session.TrackingMode.ShouldBe(TrackingMode.Lightweight);

            var firstLoad = await session.LoadAsync<DirtyTrackingTestEntity>("lw-1");
            var secondLoad = await session.LoadAsync<DirtyTrackingTestEntity>("lw-1");

            firstLoad.ShouldNotBeNull();
            secondLoad.ShouldNotBeNull();

            // Lightweight sessions do not cache instances in identity map
            ReferenceEquals(firstLoad, secondLoad).ShouldBeFalse();

            // Mutating loaded entity in lightweight session without Store() does not auto persist
            firstLoad.Name = "Lightweight Mutated";
            await session.SaveChangesAsync();
        }

        using (var verifySession = _store.OpenSession())
        {
            var reloaded = await verifySession.LoadAsync<DirtyTrackingTestEntity>("lw-1");
            reloaded.ShouldNotBeNull();
            reloaded.Name.ShouldBe("Lightweight Original");
        }
    }

    [Fact]
    public async Task DirtyTracking_Does_Not_Save_Unmodified_Entities()
    {
        // Arrange
        using (var setupSession = _store.OpenSession())
        {
            setupSession.Store(new DirtyTrackingTestEntity { Id = "dt-clean", Name = "Clean Entity", Age = 50 });
            await setupSession.SaveChangesAsync();
        }

        // Act - Load in DirtyTracking session but do NOT mutate
        using (var session = _store.OpenSession(TrackingMode.DirtyTracking))
        {
            var entity = await session.LoadAsync<DirtyTrackingTestEntity>("dt-clean");
            entity.ShouldNotBeNull();
            // No modifications made
            await session.SaveChangesAsync();
        }

        // Assert - Verify unchanged
        using (var verifySession = _store.OpenSession())
        {
            var reloaded = await verifySession.LoadAsync<DirtyTrackingTestEntity>("dt-clean");
            reloaded.ShouldNotBeNull();
            reloaded.Name.ShouldBe("Clean Entity");
        }
    }

    [Fact]
    public async Task DirtyTracking_Allows_Multiple_Mutations_And_Save_Cycles()
    {
        // Arrange
        using (var setupSession = _store.OpenSession())
        {
            setupSession.Store(new DirtyTrackingTestEntity { Id = "dt-multi", Name = "Phase 0", Age = 1 });
            await setupSession.SaveChangesAsync();
        }

        // Act
        using (var session = _store.OpenSession(TrackingMode.DirtyTracking))
        {
            var entity = await session.LoadAsync<DirtyTrackingTestEntity>("dt-multi");
            entity!.Name = "Phase 1";
            await session.SaveChangesAsync();

            entity.Name = "Phase 2";
            await session.SaveChangesAsync();
        }

        // Assert
        using (var verifySession = _store.OpenSession())
        {
            var reloaded = await verifySession.LoadAsync<DirtyTrackingTestEntity>("dt-multi");
            reloaded.ShouldNotBeNull();
            reloaded.Name.ShouldBe("Phase 2");
        }
    }
}
