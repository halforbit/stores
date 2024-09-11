using BenchmarkDotNet.Attributes;
using Halforbit.Stores;

[MemoryDiagnoser]
public class DictionaryToTypeConverterBenchmarks
{
    Dictionary<string, string> _dictionary;

    [GlobalSetup]
    public void IterationSetup()
    {
        var id = Guid.NewGuid();

        _dictionary = new Dictionary<string, string>
        {
            ["vehicle-id"] = id.ToString(),
            ["year"] = SortableNumberFormatter.IntegerToSortableString(1993, 15),
            ["make"] = "Ford",
            ["model"] = "Focus",
            ["price"] = SortableNumberFormatter.IntegerToSortableString(1993, 15)
        };
    }

    [Benchmark(Baseline = true)]
    public void DictionaryToTypeConverter_Test()
    {
        var value = DictionaryToTypeConverter<Vehicle>.FromDictionary(_dictionary);
    }

    class Vehicle
    {
        [DictionaryMember("vehicle-id")]
        public required Guid VehicleId { get; init; }

        [DictionaryMember("year", 4)]
        public required int Year { get; init; }

        [DictionaryMember("make")]
        public required string Make { get; init; }

        [DictionaryMember("model")]
        public required string Model { get; init; }

        [DictionaryMember("price")]
        public required double Price { get; init; }
    }
}
