//using Azure;
//using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.Diagnostics;

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

    public async Task/*<Response<BlobContainerInfo>>*/ CreateIfNotExistsAsync(
        /*PublicAccessType publicAccessType = PublicAccessType.None, 
        Metadata? metadata = null, 
        BlobContainerEncryptionScopeOptions? encryptionScopeOptions = null, 
        CancellationToken cancellationToken = default*/)
    {
        var start = Stopwatch.GetTimestamp();

        _blobStorageAccount.Containers.TryAdd(
            _blobContainerName, 
            new InMemoryBlobContainer
            {
                ContainerName = _blobContainerName,

                Blobs = []
            });

        await InProcessDelay.SimulateDelayAsync(Stopwatch.GetElapsedTime(start), 1, 0);
    }

    public async Task/*<Response<bool>>*/ DeleteIfExistsAsync(
        /*BlobRequestConditions? conditions = null, 
        CancellationToken cancellationToken = default*/)
    {
        var start = Stopwatch.GetTimestamp();

        _blobStorageAccount.Containers.TryRemove(_blobContainerName, out _);

        await InProcessDelay.SimulateDelayAsync(Stopwatch.GetElapsedTime(start), 1, 0);
    }

    public async IAsyncEnumerable/*AsyncPageable*/<Blob/*Item*/> GetBlobsAsync(
        /* BlobTraits.Metadata
        */BlobTraits traits = BlobTraits.None,
        /* BlobStates.Version
        */BlobStates states = BlobStates.None, 
        string? prefix = null/*, 
        CancellationToken cancellationToken = default*/)
    {
        var start = Stopwatch.GetTimestamp();

        if (!_blobStorageAccount.Containers.TryGetValue(_blobContainerName, out var blobContainer))
        {
            yield break;
        }

        var count = 0d;

        var results = new List<Blob>();

        foreach (var blob in blobContainer.Blobs.OrderBy(kv => kv.Key))
        {
            if (prefix is not null && !blob.Key.StartsWith(prefix))
            {
                continue;
            }

            var versions = states.HasFlag(BlobStates.Version) ?
                blob.Value.Versions.OrderBy(kv => kv.Value.Blob.VersionId) :
                blob.Value.Versions.OrderByDescending(kv => kv.Value.Blob.VersionId).Take(1);
            
            foreach (var version in versions)
            {
                yield return version.Value.Blob;

                count++;
            }
        }

        var pageCount = (int)Math.Ceiling(count / 5000);

        await InProcessDelay.SimulateDelayAsync(
            Stopwatch.GetElapsedTime(start),
            pageCount,
            pageCount * 5_000_000);
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
