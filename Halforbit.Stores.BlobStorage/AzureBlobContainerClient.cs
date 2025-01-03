using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Metadata = System.Collections.Generic.IDictionary<string, string>;

namespace Halforbit.Stores;

class AzureBlobContainerClient : IBlobContainerClient
{
    readonly BlobContainerClient _blobContainerClient;

    public AzureBlobContainerClient(
        string connectionString, 
        string blobContainerName) => _blobContainerClient = new(
            connectionString, 
            blobContainerName);

    public Task<Response<BlobContainerInfo>> CreateIfNotExistsAsync(
        /*PublicAccessType publicAccessType = PublicAccessType.None, 
        Metadata? metadata = null, 
        BlobContainerEncryptionScopeOptions? encryptionScopeOptions = null, 
        CancellationToken cancellationToken = default*/) => 
            _blobContainerClient.CreateIfNotExistsAsync(
                /*publicAccessType,
                metadata,
                encryptionScopeOptions,
                cancellationToken*/);

    public Task<Response<bool>> DeleteIfExistsAsync(
        /*BlobRequestConditions? conditions = null, 
        CancellationToken cancellationToken = default*/) =>
            _blobContainerClient.DeleteIfExistsAsync(
                /*conditions,
                cancellationToken*/);

    public AsyncPageable<BlobItem> GetBlobsAsync(
        BlobTraits traits = BlobTraits.None,
        BlobStates states = BlobStates.None,
        string? prefix = null/*,
        CancellationToken cancellationToken = default*/) => 
            _blobContainerClient.GetBlobsAsync(
                traits, 
                states, 
                prefix/*, 
                cancellationToken*/);

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