using BenchmarkDotNet.Attributes;
using Halforbit.Stores;

[MemoryDiagnoser]
public class TypeToDictionaryConverterBenchmarks
{
	Vehicle _object;

	[GlobalSetup]
	public void IterationSetup()
	{
		_object = new()
		{ 
			VehicleId = Guid.NewGuid(),
			Year = 1993,
			Make = "Ford",
			Model = "Focus",
			Price = 12_345.67
		};
	}

	[Benchmark]
	public void Test()
	{
		var dictionary = TypeToDictionaryConverter<Vehicle>.ToDictionary(_object);
	}

	record Vehicle
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
