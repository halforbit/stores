namespace Halforbit.Stores.Tests;

public class SortableNumberFormatterTests
{
    [Theory]
    [InlineData(0, 3, "+000")]
    [InlineData(1, 3, "+001")]
    [InlineData(1.2, 3, "+001.2")]
    [InlineData(12.34, 3, "+012.34")]
    [InlineData(-12.34, 3, "-987.66")]
    [InlineData(-1.23, 3, "-998.77")]
    [InlineData(123_456_789.123_456, 15, "+000000123456789.123456")]
    public void FloatingPointToSortableString_Tests(double value, int majorDigits, string expected)
    {
        var result = SortableNumberFormatter.FloatingPointToSortableString(value, majorDigits);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(0, 3, "+000")]
    [InlineData(1, 3, "+001")]
    [InlineData(123, 3, "+123")]
    [InlineData(-1, 3, "-999")]
    [InlineData(123_456_789, 15, "+000000123456789")]
    public void IntegerToSortableString_Tests(int value, int majorDigits, string expected)
    {
        var result = SortableNumberFormatter.IntegerToSortableString(value, majorDigits);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void RandomOrdering_Tests()
    {
        var random = new Random(42);

        var numbers = Enumerable
            .Range(0, 1_000)
            .Select(i => RandomNumber(random))
            .OrderBy(n => n)
            .ToArray();

        var formatted = numbers
            .Select(n => (
                Number: n, 
                String: SortableNumberFormatter.FloatingPointToSortableString(n, 10)))
            .ToArray();

        var alphabetized = formatted
            .OrderBy(kv => kv.String)
            .ToArray();

        for (var i = 0; i < numbers.Length; i++)
        {
            Assert.Equal(numbers[i], alphabetized[i].Number);

            var parsed = SortableNumberParser.ParseNumber<double>(alphabetized[i].String, 10);

            var difference = numbers[i] - parsed;

            if (difference > 0.000000001) throw new Exception();
        }
    }

    static double RandomNumber(Random random)
    {
        var totalDigits = random.Next(1, 12);

        int minorDigits = random.Next(1, totalDigits);
        int majorDigits = totalDigits - minorDigits;

        double integerPart = random.NextDouble() * Math.Pow(10, majorDigits);
        double decimalPart = random.NextDouble() * Math.Pow(10, minorDigits);

        var sign = random.Next(2) == 0 ? 1 : -1;

        return sign * (Math.Round(integerPart) + Math.Round(decimalPart) / Math.Pow(10, minorDigits));
    }
}
