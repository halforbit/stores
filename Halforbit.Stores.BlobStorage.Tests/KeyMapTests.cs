namespace Halforbit.Stores.Tests;

public class KeyMapTests
{
    readonly Guid _tenantId = Guid.Parse("f812d24c65434cd18452395cb6d803b6");

    readonly int _projectId = 42;

    readonly Guid _entityId = Guid.Parse("788bd9ca5b7a486386447098a109a3e0");

    [Fact]
    void TryMapKeyToString_TupleKey_Success()
    {        
        var map = KeyMap<(Guid TenantId, int ProjectId, Guid EntityId)>.Define(
            k => $"vehicles/{k.TenantId:N}/{k.ProjectId}/{k.EntityId:N}",
            ".json");

        var success = map.TryMapKeyToString(
            (TenantId: _tenantId,
                ProjectId: _projectId,
                EntityId: _entityId),
            out var str);

        Assert.True(success);

        Assert.Equal(
            "vehicles/f812d24c65434cd18452395cb6d803b6/42/788bd9ca5b7a486386447098a109a3e0.json",
            str);
    }

    [Fact]
    void TryMapPartialKeyToPrefixString_TupleKey_TuplePartialKey_Success()
    {
        var map = KeyMap<(Guid TenantId, int ProjectId, Guid EntityId)>.Define(
            k => $"vehicles/{k.TenantId:N}/{k.ProjectId}/{k.EntityId:N}",
			".json");

		var success = map.TryMapPartialKeyToPrefixString(
            (TenantId: _tenantId,
                ProjectId: _projectId),
            out var str);

        Assert.True(success);

        Assert.Equal(
            "vehicles/f812d24c65434cd18452395cb6d803b6/42/",
            str);
    }

    [Fact]
    void TryMapPartialKeyToPrefixString_TupleKey_GuidPartialKey_Success()
    {
        var map = KeyMap<(Guid TenantId, int ProjectId, Guid EntityId)>.Define(
            k => $"vehicles/{k.TenantId:N}/{k.ProjectId}/{k.EntityId:N}",
			".json");

		var success = map.TryMapPartialKeyToPrefixString(
            _tenantId,
            out var str);

        Assert.True(success);

        Assert.Equal(
            "vehicles/f812d24c65434cd18452395cb6d803b6/",
            str);
    }

    [Fact]
    void TryMapPartialKeyToPrefixString_TupleKey_NullPartialKey_Success()
    {
		var map = KeyMap<(Guid TenantId, int ProjectId, Guid EntityId)>.Define(
			k => $"vehicles/{k.TenantId:N}/{k.ProjectId}/{k.EntityId:N}",
			".json");

		var success = map.TryMapPartialKeyToPrefixString(
            null,
            out var str);

        Assert.True(success);

        Assert.Equal(
            "vehicles/",
            str);
    }

    [Fact]
    void TryMapStringToKey_TupleKey_Success()
    {
		var map = KeyMap<(Guid TenantId, int ProjectId, Guid EntityId)>.Define(
			k => $"vehicles/{k.TenantId:N}/{k.ProjectId}/{k.EntityId:N}",
			".json");

		var success = map.TryMapStringToKey(
            "vehicles/f812d24c65434cd18452395cb6d803b6/42/788bd9ca5b7a486386447098a109a3e0.json",
            out var key);

        Assert.True(success);

        Assert.Equal(_tenantId, key.TenantId);

        Assert.Equal(_projectId, key.ProjectId);

        Assert.Equal(_entityId, key.EntityId);
    }

    [Fact]
    void TryMapKeyToString_GuidKey_Success()
    {
        var map = KeyMap<Guid>.Define(
            k => $"vehicles/{k:N}",
			".json");

        var success = map.TryMapKeyToString(
            _entityId,
            out var str);

        Assert.True(success);

        Assert.Equal(
            "vehicles/788bd9ca5b7a486386447098a109a3e0.json",
            str);
    }

    [Fact]
    void TryMapPartialKeyToPrefixString_GuidKey_GuidPartialKey_Success()
    {
		var map = KeyMap<Guid>.Define(
			k => $"vehicles/{k:N}",
			".json");

		var success = map.TryMapPartialKeyToPrefixString(
            _entityId,
            out var str);

        Assert.True(success);

        Assert.Equal(
            "vehicles/788bd9ca5b7a486386447098a109a3e0.json",
            str);
    }

    [Fact]
    void TryMapPartialKeyToPrefixString_GuidKey_NullPartialKey_Success()
    {
		var map = KeyMap<Guid>.Define(
			k => $"vehicles/{k:N}",
			".json");

		var success = map.TryMapPartialKeyToPrefixString(
            null,
            out var str);

        Assert.True(success);

        Assert.Equal(
            "vehicles/",
            str);
    }

    [Fact]
    void TryMapStringToKey_GuidKey_Success()
    {
		var map = KeyMap<Guid>.Define(
			k => $"vehicles/{k:N}",
			".json");

		var success = map.TryMapStringToKey(
            "vehicles/788bd9ca5b7a486386447098a109a3e0.json",
            out var key);

        Assert.True(success);

        Assert.Equal(_entityId, key);
    }
}
