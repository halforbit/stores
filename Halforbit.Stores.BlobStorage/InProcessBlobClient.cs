//using Azure;
//using Azure.Storage.Blobs;
using System.Diagnostics;

namespace Halforbit.Stores;

class InProcessBlobClient : IBlobClient
{
    readonly string _blobName;

    readonly string? _versionId;

    readonly InMemoryBlobContainer _blobContainer;

    public InProcessBlobClient(
        string blobName,
        string? versionId,
        InMemoryBlobContainer blobContainer)
    {
        _blobName = blobName;

        _versionId = versionId;
        
        _blobContainer = blobContainer;
    }

    IBlobClient IBlobClient.WithVersion(string versionId)
    {
        return new InProcessBlobClient(
            _blobName,
            versionId,
            _blobContainer);
    }

    public async Task</*Response<*/bool/*>*/> DeleteIfExistsAsync()
    {
        var start = Stopwatch.GetTimestamp();

        try
        {
            if (_versionId is null)
            {
                return _blobContainer.Blobs.TryRemove(_blobName, out _);
            }
            else if (_blobContainer.Blobs.TryGetValue(_blobName, out var blob))
            {
                var latestVersion = blob.Versions
                    .OrderByDescending(v => v.Value.Blob.LastModified)
                    .FirstOrDefault();
            
                if (_versionId == latestVersion.Value.Blob.VersionId)
                {
                    throw new ActionFailedException("Cannot delete the root version of a blob.", null);
                }

                var removed = blob.Versions.TryRemove(_versionId, out _);

                if (removed && !blob.Versions.Any())
                {
                    _blobContainer.Blobs.TryRemove(_blobName, out _);
                }

                return removed;
            }

            return false;
        }
        finally
        {
            await InProcessDelay.SimulateDelayAsync(
                Stopwatch.GetElapsedTime(start),
                1,
                0);
        }
    }

    public async Task</*Response<*/bool/*>*/> ExistsAsync()
    {
        var start = Stopwatch.GetTimestamp();

        try
        {
            if (_blobContainer.Blobs.TryGetValue(_blobName, out var blob))
            {
                if (_versionId is null)
                {
                    return !blob.Versions.IsEmpty;
                }
                else
                {
                    blob.Versions.ContainsKey(_versionId);
                }
            }

            return false;
        }
        finally
        {
            await InProcessDelay.SimulateDelayAsync(
                Stopwatch.GetElapsedTime(start),
                1, 
                0);
        }
    }
}
