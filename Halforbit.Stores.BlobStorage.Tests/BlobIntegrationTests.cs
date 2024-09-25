using MessagePack;
using Microsoft.Extensions.Configuration;

namespace Halforbit.Stores.Tests;

public class BlobIntegrationTests
{
	static string ConnectionString => new ConfigurationBuilder()
		.AddUserSecrets<BlobIntegrationTests>()
		.Build()
		.GetConnectionString("IntegrationTest")!;

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
				.Key<Guid>(k => $"vehicles/{k:N}")
				.Value<Vehicle>()
				.WithMetadata()
				.WithVersions();

			var existsA = await vehiclesStore.BlobExistsAsync(id);

			Assert.False(existsA);

			var deleteA = await vehiclesStore.DeleteBlobAsync(id);

			Assert.False(deleteA);

			var getA = await vehiclesStore.GetBlobOrNullAsync(id);

			Assert.Null(getA);

			var transactionId = $"{Guid.NewGuid():N}";

            var putResult = await vehiclesStore.UpsertBlobAsync(
				id,
				value,
				new Dictionary<string, string>
				{
					["transaction"] = transactionId
				});

			var versionId = putResult.VersionId ??
				throw new ArgumentNullException(nameof(PutResult.VersionId));

			Assert.False(string.IsNullOrWhiteSpace(putResult.ETag));

			Assert.False(string.IsNullOrWhiteSpace(putResult.VersionId));

			var existsB = await vehiclesStore.BlobExistsAsync(id);

			Assert.True(existsB);

			var getB = await vehiclesStore.GetBlobOrNullAsync(id);

			Assert.NotNull(getB);

			Assert.Equal(value, getB.Value);

            Assert.Equal(
                transactionId,
                getB.Metadata?["transaction"] ??
                    throw new ArgumentNullException(nameof(Blob.Metadata)));

            var blobs = new List<Blob>();

			await foreach (var b in vehiclesStore.EnumerateBlobsAsync())
			{
				blobs.Add(b);
			}

			Assert.Single(blobs);

			Assert.Equal(
				transactionId,
				blobs.Single().Metadata?["transaction"] ?? 
					throw new ArgumentNullException(nameof(Blob.Metadata)));

			var transactionId2 = $"{Guid.NewGuid():N}";

			var value2 = value with
			{ 
				Year = 2022
			};

			var putResult2 = await vehiclesStore.UpsertBlobAsync(
				id, 
				value2,
				new Dictionary<string, string>
				{
					["transaction"] = transactionId2
				});

			var versionId2 = putResult2.VersionId ??
                throw new ArgumentNullException(nameof(PutResult.VersionId));
            
            Assert.NotEqual(versionId, versionId2);

			blobs.Clear();

            await foreach (var b in vehiclesStore.EnumerateBlobsAsync())
            {
                blobs.Add(b);
            }

            Assert.Equal(2, blobs.Count);

			Assert.Equal(versionId, blobs[0].VersionId);

			Assert.Equal(transactionId, blobs[0].Metadata?["transaction"] ??
				throw new ArgumentNullException(nameof(Blob.Metadata)));

			Assert.Equal(versionId2, blobs[1].VersionId);

            Assert.Equal(transactionId2, blobs[1].Metadata?["transaction"] ??
				throw new ArgumentNullException(nameof(Blob.Metadata)));

			var currentBlob = await vehiclesStore.GetBlobOrNullAsync(id);

			Assert.NotNull(currentBlob);

			Assert.Equal(value2, currentBlob.Value);

			Assert.Equal(versionId2, currentBlob.VersionId);

            Assert.Equal(transactionId2, currentBlob.Metadata?["transaction"] ??
                throw new ArgumentNullException(nameof(Blob.Metadata)));

			var priorBlob = await vehiclesStore
				.Version(versionId)
				.GetBlobOrNullAsync(id);

			Assert.NotNull(priorBlob);

            Assert.Equal(value, priorBlob.Value);

            Assert.Equal(versionId, priorBlob.VersionId);

            Assert.Equal(transactionId, priorBlob.Metadata?["transaction"] ??
                throw new ArgumentNullException(nameof(Blob.Metadata)));

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
    public async Task SerializationError_Throws()
    {
        var containerName = $"test-container-{Guid.NewGuid():N}";

        var container = BlobRequest
            .ConnectionString(ConnectionString)
            .Container(containerName);

        await container.CreateContainerIfNotExistsAsync();

        try
        {
            var id = Guid.NewGuid();

            var vehicleStore = container
                .BlockBlobs()
                .MessagePackSerialization()
                .Name($"not-serializable/{id:N}")
                .Value<NotSerializable>();

            await Assert.ThrowsAsync<MessagePackSerializationException>(async () =>
            {
                await vehicleStore.UpsertBlobAsync(new NotSerializable
                {
                    Message = "Not serializable."
                });
            });
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

	public record NotSerializable
	{
		public required string Message { get; init; }
	}
}
