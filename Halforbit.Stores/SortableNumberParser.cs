using System.Numerics;

namespace Halforbit.Stores
{
    public static class SortableNumberParser
    {
        public static TValue ParseNumber<TValue>(
            string value, 
            int majorDigits)
            where TValue : INumber<TValue>
        {
            var isNegative = value[0] == '-';

            value = value.Substring(1).TrimStart('0');

            if (value.Length == 0) return TValue.Zero;

            var decimalValue = decimal.Parse(value);

            if (isNegative)
            {
                var majorMaxValue = decimal.CreateChecked(Math.Pow(10, majorDigits));
                
                decimalValue = -(majorMaxValue - decimalValue);
            }

            return TValue.CreateChecked(decimalValue);
        }
    }
}
