namespace Halforbit.Stores;

public interface IBlobRequest { }

public interface IBlobStorageAccount { }

public interface IBlobContainer { }

public interface IBlockBlobs { }

public interface ISerializedBlockBlobs { }

public interface ICompressedBlockBlobs { }

public interface IBlockBlob
{
    IBlockBlob<TValue> Value<TValue>();
}

public interface IBlockBlob<TValue> { }

public interface IBlockBlobs<TKey> 
{
    IBlockBlobs<TKey, TValue> Value<TValue>();
}

public interface IBlockBlobs<TKey, TValue> { }

