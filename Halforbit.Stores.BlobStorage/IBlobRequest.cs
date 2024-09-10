namespace Halforbit.Stores;

public interface IBlobRequest
{
}

public interface IBlobRequestWithConnectionString /*: IBlobRequest*/
{
    //internal string? _ConnectionString { get; }
}

public interface IBlobRequestWithContainer /*: IBlobRequestWithConnectionString*/
{
    //internal string? _ContainerName { get; }

    //internal BlobContainerClient? BlobContainerClient { get; }
}

public interface IBlockBlobRequest /*: IBlobRequestWithContainer*/ { }

public interface IBlockBlobRequestWithSerialization /*: IBlobRequestWithContainer*/
{
    //internal IBlockBlobSerializer? Serializer { get; }

    //internal string? ContentType { get; }

    //internal string? ContentTypeExtension { get; }
}

public interface IBlockBlobRequestWithCompression /*: IBlockBlobRequestWithSerialization*/
{
    //internal IBlockBlobCompressor? Compressor { get; }

    //internal string? ContentEncoding { get; }

    //internal string? ContentEncodingExtension { get; }
}

public interface IBlockBlobRequestWithFixedKey /*: IBlockBlobRequestWithCompression*/
{
    //internal string? Key { get; }

    IBlockBlobRequestWithSingleValue<TValue> Value<TValue>();
}

public interface IBlockBlobRequestWithSingleValue<TValue> /*: IBlockBlobRequestWithFixedKey*/ { }

public interface IBlockBlobRequestWithKeyMap<TKey> /*: IBlockBlobRequest*/
{
    //internal KeyMap<TKey>? KeyMap { get; }

    IBlockBlobRequestWithKeyMapValue<TKey, TValue> Value<TValue>();
}

public interface IBlockBlobRequestWithKeyMapValue<TKey, TValue> /*: IBlockBlobRequest*/ { }

