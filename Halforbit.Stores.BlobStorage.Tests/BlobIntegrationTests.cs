using MessagePack;
using Microsoft.Extensions.Configuration;

namespace Halforbit.Stores.Tests;

public class BlobIntegrationTests
{
	string ConnectionString => new ConfigurationBuilder()
		.AddUserSecrets<BlobIntegrationTests>()
		.Build()
		.GetConnectionString("IntegrationTest");

	[Fact]
    public async Task Single_Success()
    {
        var containerName = $"test-container-{Guid.NewGuid():N}";

        var container = BlobRequest
            .ConnectionString(ConnectionString)
            .Container(containerName);

        await container.CreateContainerIfNotExistsAsync();

        try
        {
            var id = Guid.NewGuid();

            var value = new Vehicle
            { 
                VehicleId = id,
                Year = 1993,
                Make = "Ford",
                Model = "Focus"
            };

			await MessagePackSerializer.SerializeAsync(new MemoryStream(), value);

            var vehicleStore = container
                .BlockBlobs()
                .MessagePackSerialization()
                //.GZipCompression()
                .Name($"vehicles/{id:N}")
                .Value<Vehicle>();

            var existsA = await vehicleStore.BlobExistsAsync();

            Assert.False(existsA);

            var deleteA = await vehicleStore.DeleteBlobAsync();

            Assert.False(deleteA);

            var getA = await vehicleStore.GetBlobOrNullAsync();

            Assert.Null(getA);

            var putResult = await vehicleStore.UpsertBlobAsync(
                value, 
                new Dictionary<string, string>
                {
                    ["transaction"] = $"{Guid.NewGuid():N}"
                });

            Assert.False(string.IsNullOrWhiteSpace(putResult.ETag));

		    Assert.False(string.IsNullOrWhiteSpace(putResult.VersionId));

		    var existsB = await vehicleStore.BlobExistsAsync();

            Assert.True(existsB);
        
            var getB = await vehicleStore.GetBlobOrNullAsync();

            Assert.NotNull(getB);
            
            var blobs = new List<Blob>();
            
            await foreach (var b in container.EnumerateBlobsAsync())
            {
                blobs.Add(b);
            }

            Assert.Single(blobs);

            var deleteB = await vehicleStore.DeleteBlobAsync();

            Assert.True(deleteB);

            var deleteC = await vehicleStore.DeleteBlobAsync();

            Assert.False(deleteC);

			var getC = await vehicleStore.GetBlobOrNullAsync();

            Assert.Null(getC);
		}
		finally
		{
			await container.DeleteContainerAsync();
		}
	}

	[Fact]
	public async Task CompositeKeyed_Success()
	{
		var containerName = $"test-container-{Guid.NewGuid():N}";

		var container = BlobRequest
			.ConnectionString(ConnectionString)
			.Container(containerName);

		await container.CreateContainerIfNotExistsAsync();

		try
		{
			var id = (
				ProjectId: Guid.NewGuid(), 
				SegmentId: 42, 
				VehicleId: Guid.NewGuid());

			var value = new Vehicle
			{
				VehicleId = id.VehicleId,
				Year = 1993,
				Make = "Ford",
				Model = "Focus"
			};

			var vehiclesStore = container
				.BlockBlobs()
				.MessagePackSerialization()
				.Key<(Guid ProjectId, int SegmentId, Guid TableId)>(k => $"vehicles/{k.ProjectId:N}/{k.SegmentId}/{k.TableId:N}")
				.Value<Vehicle>()
				.WithMetadata();

			var existsA = await vehiclesStore.BlobExistsAsync(id);

			Assert.False(existsA);

			var deleteA = await vehiclesStore.DeleteBlobAsync(id);

			Assert.False(deleteA);

			var getA = await vehiclesStore.GetBlobOrNullAsync(id);

			Assert.Null(getA);

			var putResult = await vehiclesStore.UpsertBlobAsync(
				id,
				value,
				new Dictionary<string, string>
				{
					["transaction"] = $"{Guid.NewGuid():N}"
				});

			Assert.False(string.IsNullOrWhiteSpace(putResult.ETag));

			Assert.False(string.IsNullOrWhiteSpace(putResult.VersionId));

			var existsB = await vehiclesStore.BlobExistsAsync(id);

			Assert.True(existsB);

			var getB = await vehiclesStore.GetBlobOrNullAsync(id);

			Assert.NotNull(getB);

			var blobs = new List<Blob>();

			await foreach (var b in vehiclesStore.EnumerateBlobsAsync((id.ProjectId, id.SegmentId)))
			{
				blobs.Add(b);
			}

			Assert.Single(blobs);

			blobs.Clear();

			await foreach (var b in vehiclesStore.EnumerateBlobsAsync(id.ProjectId))
			{
				blobs.Add(b);
			}

			Assert.Single(blobs);

			var deleteB = await vehiclesStore.DeleteBlobAsync(id);

			Assert.True(deleteB);

			var deleteC = await vehiclesStore.DeleteBlobAsync(id);

			Assert.False(deleteC);

			var getC = await vehiclesStore.GetBlobOrNullAsync(id);

			Assert.Null(getC);
		}
		finally
		{
			await container.DeleteContainerAsync();
		}
	}

	[Fact]
	public async Task SimpleKeyed_Success()
	{
		var containerName = $"test-container-{Guid.NewGuid():N}";

		var container = BlobRequest
			.ConnectionString(ConnectionString)
			.Container(containerName);

		await container.CreateContainerIfNotExistsAsync();

		try
		{
			var id = Guid.NewGuid();

			var value = new Vehicle
			{
				VehicleId = id,
				Year = 1993,
				Make = "Ford",
				Model = "Focus"
			};

			var vehiclesStore = container
				.BlockBlobs()
				.MessagePackSerialization()
				//.GZipCompression()
				.Key<Guid>(k => $"vehicles/{k:N}")
				.Value<Vehicle>();

			var existsA = await vehiclesStore.BlobExistsAsync(id);

			Assert.False(existsA);

			var deleteA = await vehiclesStore.DeleteBlobAsync(id);

			Assert.False(deleteA);

			var getA = await vehiclesStore.GetBlobOrNullAsync(id);

			Assert.Null(getA);

			var putResult = await vehiclesStore.UpsertBlobAsync(
				id,
				value,
				new Dictionary<string, string>
				{
					["transaction"] = $"{Guid.NewGuid():N}"
				});

			Assert.False(string.IsNullOrWhiteSpace(putResult.ETag));

			Assert.False(string.IsNullOrWhiteSpace(putResult.VersionId));

			var existsB = await vehiclesStore.BlobExistsAsync(id);

			Assert.True(existsB);

			var getB = await vehiclesStore.GetBlobOrNullAsync(id);

			Assert.NotNull(getB);

			var blobs = new List<Blob>();

			await foreach (var b in vehiclesStore.EnumerateBlobsAsync())
			{
				blobs.Add(b);
			}

			Assert.Single(blobs);

			var deleteB = await vehiclesStore.DeleteBlobAsync(id);

			Assert.True(deleteB);

			var deleteC = await vehiclesStore.DeleteBlobAsync(id);

			Assert.False(deleteC);

			var getC = await vehiclesStore.GetBlobOrNullAsync(id);

			Assert.Null(getC);
		}
		finally
		{
			await container.DeleteContainerAsync();
		}
	}

	[MessagePackObject]
	public record Vehicle
    {
		[Key(0)]
        public required Guid VehicleId { get; init; }

        [Key(1)]
        public required int Year { get; init; }

        [Key(2)]
        public required string Make { get; init; }

        [Key(3)]
        public required string Model { get; init; }
    }
}
