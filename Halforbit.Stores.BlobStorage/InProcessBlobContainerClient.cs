//using Azure;
//using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Halforbit.Stores;

class InProcessBlobContainerClient : IBlobContainerClient
{
    readonly InMemoryBlobStorageAccount _blobStorageAccount;
    
    readonly string _blobContainerName;

    public InProcessBlobContainerClient(
        InMemoryBlobStorageAccount blobStorageAccount,
        string blobContainerName)
    {
        _blobStorageAccount = blobStorageAccount;
        
        _blobContainerName = blobContainerName;
    }

    public Task/*<Response<BlobContainerInfo>>*/ CreateIfNotExistsAsync(
        /*PublicAccessType publicAccessType = PublicAccessType.None, 
        Metadata? metadata = null, 
        BlobContainerEncryptionScopeOptions? encryptionScopeOptions = null, 
        CancellationToken cancellationToken = default*/)
    {
        _blobStorageAccount.Containers.TryAdd(
            _blobContainerName, 
            new InMemoryBlobContainer
            {
                ContainerName = _blobContainerName,

                Blobs = []
            });

        return Task.CompletedTask;
    }

    public Task/*<Response<bool>>*/ DeleteIfExistsAsync(
        /*BlobRequestConditions? conditions = null, 
        CancellationToken cancellationToken = default*/)
    {
        _blobStorageAccount.Containers.TryRemove(_blobContainerName, out _);

        return Task.CompletedTask;
    }

    public IAsyncEnumerable/*AsyncPageable*/<Blob/*Item*/> GetBlobsAsync(
        /* BlobTraits.Metadata
        */BlobTraits traits = BlobTraits.None,
        /* BlobStates.Version
        */BlobStates states = BlobStates.None, 
        string? prefix = null/*, 
        CancellationToken cancellationToken = default*/)
    {
        if (!_blobStorageAccount.Containers.TryGetValue(_blobContainerName, out var blobContainer))
        {
            return Array.Empty<Blob>().ToAsyncEnumerable();
        }

        var results = new List<Blob>();

        foreach (var blob in blobContainer.Blobs.OrderBy(kv => kv.Key))
        {
            if (prefix is not null && !blob.Key.StartsWith(prefix))
            {
                continue;
            }

            var versions = states.HasFlag(BlobStates.Version) ?
                blob.Value.Versions.OrderBy(kv => kv.Value.Blob.LastModified) :
                blob.Value.Versions.OrderByDescending(kv => kv.Value.Blob.LastModified);
            
            foreach (var version in versions)
            {
                results.Add(version.Value.Blob);
            }
        }

        return results.ToAsyncEnumerable();
    }

    public IBlobClient GetBlobClient(
        string blobName)
    {
        if (!_blobStorageAccount.Containers.TryGetValue(
            _blobContainerName, 
            out var blobContainer))
        {
            throw new ArgumentException($"Container `{_blobContainerName}` does not exist.");
        }

        return new InProcessBlobClient(
            blobName, 
            null, 
            blobContainer);
    }

    public IBlockBlobClient GetBlockBlobClient(string blobName)
    {
        if (!_blobStorageAccount.Containers.TryGetValue(
            _blobContainerName,
            out var blobContainer))
        {
            throw new ArgumentException($"Container `{_blobContainerName}` does not exist.");
        }

        return new InProcessBlockBlobClient(
            blobName,
            null,
            blobContainer);
    }
}
