//using Azure;
//using Azure.Storage.Blobs;
namespace Halforbit.Stores;

class InProcessBlobStorageAccountClient
{
    readonly InMemoryBlobStorageAccount _inMemoryStorageAccount = new()
    {
        Containers = []
    };

    public static InProcessBlobStorageAccountClient Instance { get; } = new InProcessBlobStorageAccountClient();

    public IBlobContainerClient GetBlobContainerClient(string blobContainerName)
    {
        return new InProcessBlobContainerClient(_inMemoryStorageAccount, blobContainerName);
    }
}
