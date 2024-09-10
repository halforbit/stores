using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Microsoft.IO;
using System.Buffers;

BenchmarkRunner.Run<PooledStreamBenchmark>();

[MemoryDiagnoser]
public class PooledStreamBenchmark
{
    //public IEnumerable<int> DataSizes => new[] { 1024, 10 * 1024, 1024 * 1024, 5 * 1024 * 1024 };

    //[ParamsSource(nameof(DataSizes))]
    //public int Size;

    //byte[] _data;

    //byte[] _buffer;

    //byte[] _bigBuffer;

    //RecyclableMemoryStreamManager _recyclableMemoryStreamManager;

    //[GlobalSetup]
    //public void Setup()
    //{
    //    _data = ArrayPool<byte>.Shared.Rent(Size);
    //    new Random(42).NextBytes(_data);

    //    _recyclableMemoryStreamManager = new RecyclableMemoryStreamManager();
    //}

    //[IterationSetup]
    //public void IterationSetup()
    //{
    //    _buffer = new byte[Size];
    //    _bigBuffer = new byte[1024 * 1024];
    //}

    //[GlobalCleanup]
    //public void Cleanup() => ArrayPool<byte>.Shared.Return(_data);

    //[Benchmark(Baseline = true)]
    //public void WriteAndRead_MemoryStream()
    //{
    //    using var stream = new MemoryStream();
    //    stream.Write(_data, 0, Size);
    //    stream.Seek(0, SeekOrigin.Begin);
    //    stream.Read(_buffer, 0, Size);
    //}

    //[Benchmark]
    //public void WriteAndRead()
    //{
    //    using var stream = new PooledStream();
    //    stream.Write(_data, 0, Size);
    //    stream.Seek(0, SeekOrigin.Begin);
    //    stream.Read(_buffer, 0, Size);
    //}

    //[Benchmark]
    //public void WriteAndReadMultipleChunks()
    //{
    //    using var stream = new PooledStream();
    //    var remaining = Size;
    //    var offset = 0;
  
    //    while (remaining > 0)
    //    {
    //        var chunkSize = Math.Min(1024 * 1024, remaining); // Write 1MB at a time
    //        stream.Write(_data, offset, chunkSize);
    //        remaining -= chunkSize;
    //        offset += chunkSize;
    //    }
  
    //    stream.Seek(0, SeekOrigin.Begin);
  
    //    remaining = Size;
    //    offset = 0;

    //    var buffer = _bigBuffer;
  
    //    while (remaining > 0)
    //    {
    //        var bytesRead = stream.Read(buffer, 0, Math.Min(buffer.Length, remaining));
    //        remaining -= bytesRead;
    //    }
    //}

    //[Benchmark]
    //public void WriteAndReadMultipleChunks_Recyclable()
    //{
    //    using var stream = _recyclableMemoryStreamManager.GetStream();
    //    var remaining = Size;
    //    var offset = 0;

    //    while (remaining > 0)
    //    {
    //        var chunkSize = Math.Min(1024 * 1024, remaining); // Write 1MB at a time
    //        stream.Write(_data, offset, chunkSize);
    //        remaining -= chunkSize;
    //        offset += chunkSize;
    //    }

    //    stream.Seek(0, SeekOrigin.Begin);

    //    remaining = Size;
    //    offset = 0;

    //    var buffer = _bigBuffer;

    //    while (remaining > 0)
    //    {
    //        var bytesRead = stream.Read(buffer, 0, Math.Min(buffer.Length, remaining));
    //        remaining -= bytesRead;
    //    }
    //}

    //[Benchmark]
    //public void WriteAndRead_RecyclableMemoryStream()
    //{
    //    using var stream = _recyclableMemoryStreamManager.GetStream();
    //    stream.Write(_data, 0, Size);
    //    stream.Seek(0, SeekOrigin.Begin);
    //    stream.Read(_buffer, 0, Size);
    //}
}
