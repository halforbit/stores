using Azure;
//using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
//using Metadata = System.Collections.Generic.IDictionary<string, string>;

namespace Halforbit.Stores;

interface IBlobContainerClient
{
    Task<Response<BlobContainerInfo>> CreateIfNotExistsAsync(
        /*PublicAccessType publicAccessType = PublicAccessType.None,
        Metadata? metadata = default,
        BlobContainerEncryptionScopeOptions? encryptionScopeOptions = default,
        CancellationToken cancellationToken = default*/);

    Task<Response<bool>> DeleteIfExistsAsync(
        /*BlobRequestConditions? conditions = default,
        CancellationToken cancellationToken = default*/);

    AsyncPageable<BlobItem> GetBlobsAsync(
        BlobTraits traits = BlobTraits.None,
        BlobStates states = BlobStates.None,
        string? prefix = default/*,
        CancellationToken cancellationToken = default*/);

    IBlobClient GetBlobClient(string blobName);
    
    IBlockBlobClient GetBlockBlobClient(string blobName);
}
