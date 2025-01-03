using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Halforbit.Stores;

class InProcessBlobContainerClient : IBlobContainerClient
{
    public Task<Response<BlobContainerInfo>> CreateIfNotExistsAsync(
        /*PublicAccessType publicAccessType = PublicAccessType.None, 
        Metadata? metadata = null, 
        BlobContainerEncryptionScopeOptions? encryptionScopeOptions = null, 
        CancellationToken cancellationToken = default*/)
    {
        throw new NotImplementedException();
    }

    public Task<Response<bool>> DeleteIfExistsAsync(
        /*BlobRequestConditions? conditions = null, 
        CancellationToken cancellationToken = default*/)
    {
        throw new NotImplementedException();
    }

    public AsyncPageable<BlobItem> GetBlobsAsync(
        BlobTraits traits = BlobTraits.None, 
        BlobStates states = BlobStates.None, 
        string? prefix = null/*, 
        CancellationToken cancellationToken = default*/)
    {
        throw new NotImplementedException();
    }

    public IBlobClient GetBlobClient(
        string blobName)
    {
        throw new NotImplementedException();
    }

    public IBlockBlobClient GetBlockBlobClient(string blobName)
    {
        throw new NotImplementedException();
    }
}
