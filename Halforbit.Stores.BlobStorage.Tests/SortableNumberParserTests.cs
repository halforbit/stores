namespace Halforbit.Stores.Tests
{
    public class SortableNumberParserTests
    {
        [Theory]
        [InlineData("+000", 3, 0)]
        [InlineData("+001", 3, 1)]
        [InlineData("+001.2", 3, 1.20)]
        [InlineData("+012.34", 3, 12.34)]
        [InlineData("-987.66", 3, -12.34)]
        [InlineData("-998.77", 3, -1.23)]
        [InlineData("+000000123456789.123456", 15, 123456789.123456)]
        [InlineData("+123", 3, 123)]
        [InlineData("-999", 3, -1)]
        [InlineData("+000000123456789", 15, 123456789)]
        public void ParseNumber_Tests(string sortableString, int majorDigits, double expected)
        {
            var result = SortableNumberParser.ParseNumber<double>(sortableString, majorDigits);
            
            Assert.Equal(expected, result);
        }
    }
}
