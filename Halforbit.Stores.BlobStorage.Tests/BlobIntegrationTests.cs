namespace Halforbit.Stores.Tests;

public class BlobIntegrationTests
{
    [Fact]
    public async Task General_Success()
    {
        var container = BlobRequest
            .ConnectionString("...")
            .Container("test-container");

        await container.CreateContainerIfNotExistsAsync();

        var id = Guid.NewGuid();

        var value = new Vehicle
        { 
            VehicleId = id,
            Year = 1993,
            Make = "Ford",
            Model = "Focus"
        };

        var blob = container
            .BlockBlobs()
            .JsonSerialization()
            //.GZipCompression()
            .Key($"vehicles/{id:N}")
            .Value<Vehicle>();

        var existsA = await blob.ExistsAsync();

        Assert.False(existsA);

        var deleteA = await blob.DeleteAsync();

        Assert.False(deleteA);

        var getA = await blob.GetAsync();

        Assert.Null(getA);

        //await blob.UpsertAsync(value);

        var eTag = await blob.UpsertBlobAsync(
            value, 
            new()
            {
                ["transaction"] = $"{Guid.NewGuid():N}"
            });

        var existsB = await blob.ExistsAsync();

        Assert.True(existsB);
        
        //var getB = await blob.GetAsync();

        var getB = await blob.GetBlobOrNullAsync();

        Assert.NotNull(getB);

        var deleteB = await blob.DeleteAsync();

        Assert.True(deleteB);

        var deleteC = await blob.DeleteAsync();

        Assert.False(deleteC);

        var getC = await blob.GetAsync();

        Assert.Null(getC);
    }

    record Vehicle
    {
        public required Guid VehicleId { get; init; }

        public required int Year { get; init; }

        public required string Make { get; init; }

        public required string Model { get; init; }
    }
}
