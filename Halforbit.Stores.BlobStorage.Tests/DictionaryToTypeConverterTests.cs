namespace Halforbit.Stores.Tests;

public class DictionaryToTypeConverterTests
{
    [Fact]
    public void DictionaryToClass_Success()
    {
        var id = Guid.NewGuid();

        var dict = new Dictionary<string, string>
        {
            ["vehicle-id"] = id.ToString(),
            ["year"] = SortableNumberFormatter.IntegerToSortableString(1993, 4),
            ["model"] = "Focus",
            ["price"] = SortableNumberFormatter.FloatingPointToSortableString(12_345.67, 15)
        };

        var value = DictionaryToTypeConverter<Vehicle>.FromDictionary(dict);

        Assert.Equal(id, value.VehicleId);
        Assert.Equal(1993, value.Year);
        Assert.Null(value.Make);
        Assert.Equal("Focus", value.Model);
        Assert.Equal(12345.67, value.Price, 2);
    }

    public class MyClass
    {
        public bool TryParse(string input, out int result)
        {
            return int.TryParse(input, out result);
        }
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
