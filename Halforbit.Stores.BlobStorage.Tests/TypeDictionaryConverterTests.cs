using Halforbit.Stores;

namespace Halforbit.Stores.Tests;

public class TypeDictionaryConverterTests
{
    [Fact]
    void RecordToDictionary_Success()
    {
        var id = Guid.NewGuid();

        var value = new Vehicle
        {
            VehicleId = id,
            Year = 1993,
            Make = "Ford",
            Model = "Focus", 
            Price = 12_345.67
        };

        var dict = TypeDictionaryConvertor<Vehicle>.ToDictionary(value);

        Assert.Equal(id.ToString(), dict["vehicle-id"]);
        Assert.Equal("+1993", dict["year"]);
        Assert.Equal("Ford", dict["make"]);
        Assert.Equal("Focus", dict["model"]);
        Assert.Equal("+000000000012345.67", dict["price"]);
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
