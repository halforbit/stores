using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
//using Metadata = System.Collections.Generic.IDictionary<string, string>;

namespace Halforbit.Stores;

class AzureBlobContainerClient : IBlobContainerClient
{
    readonly BlobContainerClient _blobContainerClient;

    public AzureBlobContainerClient(
        string connectionString, 
        string blobContainerName) => _blobContainerClient = new(
            connectionString, 
            blobContainerName);

    public Task/*<Response<BlobContainerInfo>>*/ CreateIfNotExistsAsync(
        /*PublicAccessType publicAccessType = PublicAccessType.None, 
        Metadata? metadata = null, 
        BlobContainerEncryptionScopeOptions? encryptionScopeOptions = null, 
        CancellationToken cancellationToken = default*/) => 
            _blobContainerClient.CreateIfNotExistsAsync(
                /*publicAccessType,
                metadata,
                encryptionScopeOptions,
                cancellationToken*/);

    public Task/*<Response<bool>>*/ DeleteIfExistsAsync(
        /*BlobRequestConditions? conditions = null, 
        CancellationToken cancellationToken = default*/) =>
            _blobContainerClient.DeleteIfExistsAsync(
                /*conditions,
                cancellationToken*/);

    public async IAsyncEnumerable/*AsyncPageable*/<Blob/*Item*/> GetBlobsAsync(
        BlobTraits traits = BlobTraits.None,
        BlobStates states = BlobStates.None,
        string? prefix = null/*,
        CancellationToken cancellationToken = default*/)
    {
        await foreach (var blobItem in _blobContainerClient.GetBlobsAsync(
            traits, 
            states, 
            prefix/*, 
            cancellationToken*/))
        {
            yield return BlobItemConverter.ToBlob(blobItem);
        }
    }

    public IBlobClient GetBlobClient(
        string blobName) => 
            new AzureBlobClient(_blobContainerClient.GetBlobClient(
                blobName));

    public IBlockBlobClient GetBlockBlobClient(
        string blobName) => 
            new AzureBlockBlobClient(
                _blobContainerClient.GetBlockBlobClient(
                    blobName));
}

static class BlobItemConverter
{
    public static Blob ToBlob(BlobItem blobItem)
    {
        return new()
        {
            Name = blobItem.Name,

            ETag = blobItem.Properties.ETag?.ToString() ?? throw new ArgumentNullException("ETag"),

            VersionId = blobItem.VersionId,

            Metadata = blobItem.Metadata ?? new Dictionary<string, string>(),

            CreationTime = blobItem.Properties.CreatedOn?.UtcDateTime ?? throw new ArgumentNullException("CreatedOn"),

            LastModified = blobItem.Properties.LastModified?.UtcDateTime ?? throw new ArgumentNullException("LastModified"),

            BlobType = ConvertBlobType(blobItem.Properties.BlobType ?? throw new ArgumentNullException("BlobType")),

            ContentLength = blobItem.Properties.ContentLength ?? throw new ArgumentNullException("ContentLength"),

            ContentType = blobItem.Properties.ContentType ?? throw new ArgumentNullException("ContentType"),

            ContentEncoding = blobItem.Properties.ContentEncoding,

            ContentHash = blobItem.Properties.ContentHash ?? throw new ArgumentNullException("ContentHash")
        };

    }

    static BlobType ConvertBlobType(Azure.Storage.Blobs.Models.BlobType? blobType)
    {
        switch (blobType)
        {
            case null: return BlobType.Unknown;

            case Azure.Storage.Blobs.Models.BlobType.Block: return BlobType.BlockBlob;

            case Azure.Storage.Blobs.Models.BlobType.Append: return BlobType.AppendBlob;

            case Azure.Storage.Blobs.Models.BlobType.Page: return BlobType.PageBlob;

            default: throw new ArgumentException("Unhandled blob type.");
        }
    }
}

