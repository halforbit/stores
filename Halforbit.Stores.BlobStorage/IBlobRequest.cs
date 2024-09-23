namespace Halforbit.Stores;

public interface IBlobRequest
{
}

public interface IBlobStorageAccount /*: IBlobRequest*/
{
    //internal string? _ConnectionString { get; }
}

public interface IBlobContainer /*: IBlobRequestWithConnectionString*/
{
    //internal string? _ContainerName { get; }

    //internal BlobContainerClient? BlobContainerClient { get; }
}

public interface IBlockBlob /*: IBlobRequestWithContainer*/ { }

public interface ISerializedBlockBlob /*: IBlobRequestWithContainer*/
{
    //internal IBlockBlobSerializer? Serializer { get; }

    //internal string? ContentType { get; }

    //internal string? ContentTypeExtension { get; }
}

public interface ICompressedBlockBlob /*: IBlockBlobRequestWithSerialization*/
{
    //internal IBlockBlobCompressor? Compressor { get; }

    //internal string? ContentEncoding { get; }

    //internal string? ContentEncodingExtension { get; }
}

public interface INamedBlockBlob /*: IBlockBlobRequestWithCompression*/
{
    //internal string? Key { get; }

    IBlockBlob<TValue> Value<TValue>();
}

public interface IBlockBlob<TValue> /*: IBlockBlobRequestWithFixedKey*/ { }

public interface IBlockBlobs<TKey> /*: IBlockBlobRequest*/
{
    //internal KeyMap<TKey>? KeyMap { get; }

    IBlockBlobs<TKey, TValue> Value<TValue>();
}

public interface IBlockBlobs<TKey, TValue> /*: IBlockBlobRequest*/ { }

