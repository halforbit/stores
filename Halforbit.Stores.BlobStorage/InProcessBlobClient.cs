//using Azure;
//using Azure.Storage.Blobs;
namespace Halforbit.Stores;

public class InProcessBlobClient : IBlobClient
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

    public Task</*Response<*/bool/*>*/> DeleteIfExistsAsync()
    {
        if (_versionId is null)
        {
            return Task.FromResult(
                _blobContainer.Blobs.TryRemove(_blobName, out _));
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

            return Task.FromResult(removed);
        }

        return Task.FromResult(false);
    }

    public Task</*Response<*/bool/*>*/> ExistsAsync()
    {
        if (_blobContainer.Blobs.TryGetValue(_blobName, out var blob))
        {
            if (_versionId is null)
            {
                return Task.FromResult(!blob.Versions.IsEmpty);
            }
            else
            {
                blob.Versions.ContainsKey(_versionId);
            }
        }

        return Task.FromResult(false);
    }
}
