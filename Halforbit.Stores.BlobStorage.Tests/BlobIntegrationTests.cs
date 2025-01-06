using MessagePack;
using Microsoft.Extensions.Configuration;

namespace Halforbit.Stores.Tests;

public class BlobIntegrationTests
{
	static string ConnectionString => new ConfigurationBuilder()
		.AddUserSecrets<BlobIntegrationTests>()
		.Build()
		.GetConnectionString("IntegrationTest")!;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Single_Success(bool inProcess)
    {
        var blobStorageAccount = inProcess ?
            BlobRequest.InProcess(new InMemoryBlobStorageAccount()) :
            BlobRequest.ConnectionString(ConnectionString);

        var containerName = $"test-container-{Guid.NewGuid():N}";

        var container = blobStorageAccount.Container(containerName);

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

			await Assert.ThrowsAsync<PreconditionFailedException>(async () =>
			{
				await vehicleStore
					.IfExists()
					.GetBlobOrNullAsync();
            });

            await Assert.ThrowsAsync<PreconditionFailedException>(async () =>
            {
                await vehicleStore
                    .IfExists()
                    .UpsertBlobAsync(value);
            });

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

			var getB_1 = await vehicleStore
				.IfUnmodifiedSince(putResult.LastModified)
				.GetBlobOrNullAsync();

			Assert.NotNull(getB_1);

            await Assert.ThrowsAsync<PreconditionFailedException>(async () =>
            {
                await vehicleStore
                    .IfModifiedSince(putResult.LastModified)
                    .UpsertBlobAsync(value);
            });

            await Assert.ThrowsAsync<PreconditionFailedException>(async () =>
            {
                await vehicleStore
                    .IfNotExists()
                    .UpsertBlobAsync(value);
            });

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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompositeKeyed_Success(bool inProcess)
	{
        var blobStorageAccount = inProcess ?
            BlobRequest.InProcess(new InMemoryBlobStorageAccount()) :
            BlobRequest.ConnectionString(ConnectionString);
        
        var containerName = $"test-container-{Guid.NewGuid():N}";

		var container = blobStorageAccount.Container(containerName);

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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SimpleKeyed_Success(bool inProcess)
	{
        var blobStorageAccount = inProcess ?
            BlobRequest.InProcess(new InMemoryBlobStorageAccount()) :
            BlobRequest.ConnectionString(ConnectionString);
        
        var containerName = $"test-container-{Guid.NewGuid():N}";

		var container = blobStorageAccount.Container(containerName);

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

            var deleteB = await vehiclesStore
                .Version(versionId)
                .DeleteBlobAsync(id);

			Assert.True(deleteB);

            var existsX = await vehiclesStore.BlobExistsAsync(id);

            Assert.True(existsX);

			var deleteC = await vehiclesStore
                .DeleteBlobAsync(id);

			Assert.True(deleteC);

			var getC = await vehiclesStore.GetBlobOrNullAsync(id);

			Assert.Null(getC);
		}
		finally
		{
			await container.DeleteContainerAsync();
		}
	}

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Empty_SimpleKeyed_Success(bool inProcess)
    {
        var blobStorageAccount = inProcess ?
            BlobRequest.InProcess(new InMemoryBlobStorageAccount()) :
            BlobRequest.ConnectionString(ConnectionString);

        var containerName = $"test-container-{Guid.NewGuid():N}";

        var container = blobStorageAccount.Container(containerName);

        await container.CreateContainerIfNotExistsAsync();

        try
        {
            var id = Guid.NewGuid();

            var vehiclesStore = container
                .BlockBlobs()
                .Empty()
                .Key<Guid>(k => $"vehicles/{k:N}")
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

            var putResult2 = await vehiclesStore.UpsertBlobAsync(
                id,
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

            Assert.Equal(versionId2, currentBlob.VersionId);

            Assert.Equal(transactionId2, currentBlob.Metadata?["transaction"] ??
                throw new ArgumentNullException(nameof(Blob.Metadata)));

            var priorBlob = await vehiclesStore
                .Version(versionId)
                .GetBlobOrNullAsync(id);

            Assert.NotNull(priorBlob);

            Assert.Equal(versionId, priorBlob.VersionId);

            Assert.Equal(transactionId, priorBlob.Metadata?["transaction"] ??
                throw new ArgumentNullException(nameof(Blob.Metadata)));

            var deleteB = await vehiclesStore
                .Version(versionId)
                .DeleteBlobAsync(id);

            Assert.True(deleteB);

            var deleteC = await vehiclesStore.DeleteBlobAsync(id);

            // TODO: Determine why this is true when versioned.
            //Assert.False(deleteC);

            var getC = await vehiclesStore.GetBlobOrNullAsync(id);

            Assert.Null(getC);
        }
        finally
        {
            await container.DeleteContainerAsync();
        }
    }


    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Empty_Single_Success(bool inProcess)
    {
        var blobStorageAccount = inProcess ?
            BlobRequest.InProcess(new InMemoryBlobStorageAccount()) :
            BlobRequest.ConnectionString(ConnectionString);

        var containerName = $"test-container-{Guid.NewGuid():N}";

        var container = blobStorageAccount.Container(containerName);

        await container.CreateContainerIfNotExistsAsync();

        try
        {
            var id = Guid.NewGuid();

            var vehiclesStore = container
                .BlockBlobs()
                .Empty()
                .Name($"vehicles/{Guid.NewGuid():N}")
                .WithMetadata()
                .WithVersions();

            var existsA = await vehiclesStore.BlobExistsAsync();

            Assert.False(existsA);

            var deleteA = await vehiclesStore.DeleteBlobAsync();

            Assert.False(deleteA);

            var getA = await vehiclesStore.GetBlobOrNullAsync();

            Assert.Null(getA);

            var transactionId = $"{Guid.NewGuid():N}";

            var putResult = await vehiclesStore.UpsertBlobAsync(
                new Dictionary<string, string>
                {
                    ["transaction"] = transactionId
                });

            var versionId = putResult.VersionId ??
                throw new ArgumentNullException(nameof(PutResult.VersionId));

            Assert.False(string.IsNullOrWhiteSpace(putResult.ETag));

            Assert.False(string.IsNullOrWhiteSpace(putResult.VersionId));

            var existsB = await vehiclesStore.BlobExistsAsync();

            Assert.True(existsB);

            var getB = await vehiclesStore.GetBlobOrNullAsync();

            Assert.NotNull(getB);

            Assert.Equal(
                transactionId,
                getB.Metadata?["transaction"] ??
                    throw new ArgumentNullException(nameof(Blob.Metadata)));

            var transactionId2 = $"{Guid.NewGuid():N}";

            var putResult2 = await vehiclesStore.UpsertBlobAsync(
                new Dictionary<string, string>
                {
                    ["transaction"] = transactionId2
                });

            var versionId2 = putResult2.VersionId ??
                throw new ArgumentNullException(nameof(PutResult.VersionId));

            Assert.NotEqual(versionId, versionId2);

            var currentBlob = await vehiclesStore.GetBlobOrNullAsync();

            Assert.NotNull(currentBlob);

            Assert.Equal(versionId2, currentBlob.VersionId);

            Assert.Equal(transactionId2, currentBlob.Metadata?["transaction"] ??
                throw new ArgumentNullException(nameof(Blob.Metadata)));

            var priorBlob = await vehiclesStore
                .Version(versionId)
                .GetBlobOrNullAsync();

            Assert.NotNull(priorBlob);

            Assert.Equal(versionId, priorBlob.VersionId);

            Assert.Equal(transactionId, priorBlob.Metadata?["transaction"] ??
                throw new ArgumentNullException(nameof(Blob.Metadata)));

            var deleteB = await vehiclesStore.DeleteBlobAsync();

            Assert.True(deleteB);

            var deleteC = await vehiclesStore.DeleteBlobAsync();

            Assert.False(deleteC);

            var getC = await vehiclesStore.GetBlobOrNullAsync();

            Assert.Null(getC);
        }
        finally
        {
            await container.DeleteContainerAsync();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SerializationError_Throws(bool inProcess)
    {
        var blobStorageAccount = inProcess ?
            BlobRequest.InProcess(new InMemoryBlobStorageAccount()) :
            BlobRequest.ConnectionString(ConnectionString);

        var containerName = $"test-container-{Guid.NewGuid():N}";

        var container = blobStorageAccount.Container(containerName);

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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MatchETag_Success(bool inProcess)
	{
        var blobStorageAccount = inProcess ?
            BlobRequest.InProcess(new InMemoryBlobStorageAccount()) :
            BlobRequest.ConnectionString(ConnectionString);

        var containerName = $"test-container-{Guid.NewGuid():N}";

        var container = blobStorageAccount.Container(containerName);

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

            var vehicleStore = container
                .BlockBlobs()
                .MessagePackSerialization()
                .Key<Guid>(k => $"vehicles/{k:N}")
                .Value<Vehicle>();

            var putResult = await vehicleStore.UpsertBlobAsync(id, value);

			var eTag = putResult.ETag;

			await vehicleStore
				.IfMatch(eTag)
				.UpsertBlobAsync(id, value);

			await Assert.ThrowsAsync<PreconditionFailedException>(async () =>
			{
				await vehicleStore
					.IfMatch(eTag)
					.UpsertBlobAsync(id, value);
			});
        }
        finally
        {
            await container.DeleteContainerAsync();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UpdateMetadata_Versioned_Failure(bool inProcess)
    {
        var blobStorageAccount = inProcess ?
            BlobRequest.InProcess(new InMemoryBlobStorageAccount()) :
            BlobRequest.ConnectionString(ConnectionString);

        var containerName = $"test-container-{Guid.NewGuid():N}";

        var container = blobStorageAccount.Container(containerName);

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

            var vehicleStore = container
                .BlockBlobs()
                .MessagePackSerialization()
                .Key<Guid>(k => $"vehicles/{k:N}")
                .Value<Vehicle>();

            var transactionId1 = $"{Guid.NewGuid():N}";

            var metadata = new Dictionary<string, string>
            {
                ["__transaction"] = transactionId1
            };

            var putResult = await vehicleStore.UpsertBlobAsync(
                id, 
                value, 
                metadata);

            var eTag = putResult.ETag;

            var transactionId2 = $"{Guid.NewGuid():N}";

            metadata["__transaction"] = transactionId2;

            await vehicleStore
                .IfMatch(eTag)
                .UpsertBlobAsync(
                    id, 
                    value,
                    metadata);

            var blobs = new List<Blob>();

            await foreach (var blob in vehicleStore
                .WithVersions()
                .WithMetadata()
                .EnumerateBlobsAsync())
            {
                blobs.Add(blob);
            }

            Assert.Equal(2, blobs.Count);

            Assert.Equal(transactionId1, blobs[0].Metadata?["__transaction"]);

            Assert.Equal(transactionId2, blobs[1].Metadata?["__transaction"]);

            Assert.NotNull(blobs[0].VersionId);

            metadata.Add("__success", "true");

            await Assert.ThrowsAsync<ActionFailedException>(async () =>
            {
                await vehicleStore
                    .Version(blobs[0].VersionId!)
                    .SetBlobMetadataAsync(id, metadata);
            });

            //blobs.Clear();

            //await foreach (var blob in vehicleStore
            //    .WithVersions()
            //    .WithMetadata()
            //    .EnumerateBlobsAsync())
            //{
            //    blobs.Add(blob);
            //}

            //Assert.Equal(2, blobs.Count);

            //Assert.Equal(transactionId1, blobs[0].Metadata?["__transaction"]);

            //Assert.Equal("true", blobs[0].Metadata?["__success"]);

            //Assert.Equal(transactionId2, blobs[1].Metadata?["__transaction"]);

            //Assert.False(blobs[1].Metadata?.ContainsKey("__success"));
        }
        finally
        {
            await container.DeleteContainerAsync();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Delete_Versioned_Failure(bool inProcess)
    {
        var blobStorageAccount = inProcess ?
            BlobRequest.InProcess(new InMemoryBlobStorageAccount()) :
            BlobRequest.ConnectionString(ConnectionString);

        var containerName = $"test-container-{Guid.NewGuid():N}";

        var container = blobStorageAccount.Container(containerName);

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

            var vehicleStore = container
                .BlockBlobs()
                .MessagePackSerialization()
                .Key<Guid>(k => $"vehicles/{k:N}")
                .Value<Vehicle>();

            var transactionIdA = $"{Guid.NewGuid():N}";

            var metadataA = new Dictionary<string, string>
            {
                ["__transaction"] = transactionIdA
            };

            var putResultA = await vehicleStore.UpsertBlobAsync(
                id,
                value,
                metadataA);

            var eTag = putResultA.ETag;

            var transactionIdB = $"{Guid.NewGuid():N}";

            var metadataB = new Dictionary<string, string>
            {
                ["__transaction"] = transactionIdB
            };

            var putResultB = await vehicleStore
                .IfMatch(eTag)
                .UpsertBlobAsync(
                    id,
                    value,
                    metadataB);

            var blobs = new List<Blob>();

            await foreach (var blob in vehicleStore
                .WithVersions()
                .WithMetadata()
                .EnumerateBlobsAsync())
            {
                blobs.Add(blob);
            }

            Assert.Equal(2, blobs.Count);

            Assert.Equal(transactionIdA, blobs[0].Metadata?["__transaction"]);

            Assert.Equal(transactionIdB, blobs[1].Metadata?["__transaction"]);

            Assert.NotNull(blobs[1].VersionId);

            metadataA.Add("__success", "true");

            await Assert.ThrowsAsync<ActionFailedException>(async () =>
            {
                await vehicleStore
                    .Version(blobs[1].VersionId!)
                    .DeleteBlobAsync(id);
            });

            //blobs.Clear();

            //await foreach (var blob in vehicleStore
            //    .WithVersions()
            //    .WithMetadata()
            //    .EnumerateBlobsAsync())
            //{
            //    blobs.Add(blob);
            //}

            //Assert.Single(blobs);

            //Assert.Equal(transactionId2, blobs[0].Metadata?["__transaction"]);
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
