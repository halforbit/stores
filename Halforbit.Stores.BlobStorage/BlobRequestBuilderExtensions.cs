﻿using Azure.Storage.Blobs;
using OpenTelemetry.Trace;
using System.Linq.Expressions;

namespace Halforbit.Stores;

public static class BlobRequestBuilderExtensions
{
    public static IBlobStorageAccount Trace(
        this IBlobStorageAccount request, 
        TracerProvider tracerProvider)
    {
        var q = (BlobRequest<None, None>)request;

        return q with
        {
            Tracer = tracerProvider.GetTracer("Halforbit.Stores")
        };
    }

    public static IBlobContainer Container(
        this IBlobStorageAccount request,
        string name)
    {
        var q = (BlobRequest<None, None>)request;

        return q with
        {
            _ContainerName = name,

            BlobContainerClient = new BlobContainerClient(q._ConnectionString, name)
        };
    }

    public static async Task CreateContainerIfNotExistsAsync(
        this IBlobContainer request)
    {
        var q = (BlobRequest<None, None>)request;

        if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient has not been created.");

        var response = await q.BlobContainerClient.CreateIfNotExistsAsync();
    }

    public static async Task DeleteContainerAsync(
        this IBlobContainer request)
    {
		var q = (BlobRequest<None, None>)request;

		if (q.BlobContainerClient is null) throw new Exception("BlobContainerClient has not been created.");

        await q.BlobContainerClient.DeleteIfExistsAsync();
	}

	public static IBlockBlobs BlockBlobs(
        this IBlobContainer request) => (BlobRequest<None, None>)request with
        {
            BlobType = BlobType.BlockBlob
        };

    public static ISerializedBlockBlobs JsonSerialization(
        this IBlockBlobs request) => (BlobRequest<None, None>)request with
        {
            ContentSerializer = new JsonSerializerStrategy(),

            ContentType = "application/json",

            ContentTypeExtension = ".json"
        };

    public static ICompressedBlockBlobs GZipCompression(
        this ISerializedBlockBlobs request) => (BlobRequest<None, None>)request with
        {
            CompressionStrategy = new GZipCompressionStrategy(),

            ContentEncoding = "gzip",

            ContentEncodingExtension = ".gz"
        };

    public static IBlockBlobs<TKey> Key<TKey>(
        this ISerializedBlockBlobs request,
        Expression<Func<TKey, string>> map)
    {
        var q = (BlobRequest<None, None>)request;

        return q.RecastTo<TKey, None>() with
        {
            KeyMap = KeyMap<TKey>.Define(map, $"{q.ContentTypeExtension}{q.ContentEncodingExtension}")
        };
    }

    public static IBlockBlob Name(
        this ISerializedBlockBlobs request,
        string name) => (BlobRequest<None, None>)request with
        {
            Name = name
        };

    public static IBlockBlob Name(
        this ICompressedBlockBlobs request,
        string name) => (BlobRequest<None, None>)request with
        {
            Name = name
        };

    public static IBlockBlobs<TKey, TValue> WithMetadata<TKey, TValue>(
        this IBlockBlobs<TKey, TValue> request) => ((BlobRequest<TKey, TValue>)request) with
        {
            IncludeMetadata = true
        };

    public static IBlockBlobs<TKey, TValue> WithoutMetadata<TKey, TValue>(
        this IBlockBlobs<TKey, TValue> request) => ((BlobRequest<TKey, TValue>)request) with
        {
            IncludeMetadata = false
        };

    public static IBlockBlob<TValue> WithMetadata<TValue>(
        this IBlockBlob<TValue> request) => ((BlobRequest<None, TValue>)request) with
        {
            IncludeMetadata = true
        };

    public static IBlockBlob<TValue> WithoutMetadata<TValue>(
        this IBlockBlob<TValue> request) => ((BlobRequest<None, TValue>)request) with
        {
            IncludeMetadata = false
        };

    public static IBlockBlobs<TKey, TValue> WithVersions<TKey, TValue>(
        this IBlockBlobs<TKey, TValue> request) => ((BlobRequest<TKey, TValue>)request) with
        {
            IncludeVersions = true
        };

    public static IBlockBlobs<TKey, TValue> WithoutVersions<TKey, TValue>(
        this IBlockBlobs<TKey, TValue> request) => ((BlobRequest<TKey, TValue>)request) with
        {
            IncludeVersions = false
        };

    public static IBlockBlobs<TKey, TValue> Version<TKey, TValue>(
        this IBlockBlobs<TKey, TValue> request,
        string versionId) => ((BlobRequest<TKey, TValue>)request) with
        {
            VersionId = versionId
        };
}
