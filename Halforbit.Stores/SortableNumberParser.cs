using System.Numerics;
using System.Globalization;

namespace Halforbit.Stores
{
    public static class SortableNumberParser
    {
        public static TValue ParseNumber<TValue>(string value, int majorDigits)
            where TValue : INumber<TValue>
        {
            var isNegative = value[0] == '-';

            value = value.Substring(1).TrimStart('0');

            if (value.Length == 0) return TValue.Zero;

            var decimalValue = decimal.Parse(value, CultureInfo.InvariantCulture);

            if (isNegative)
            {
                var majorMaxValue = decimal.CreateChecked(Math.Pow(10, majorDigits));

                decimalValue = -(majorMaxValue - decimalValue);
            }

            return TValue.CreateChecked(decimalValue);
        }

        public static object ParseNumber(string value, Type type, int majorDigits)
        {
            var method = typeof(SortableNumberParser)
                .GetMethods()
                .First(m => m.Name == nameof(ParseNumber) && 
                            m.GetGenericArguments().Length == 1 && 
                            m.GetParameters().Length == 2);
            var genericMethod = method.MakeGenericMethod(type);
            return genericMethod.Invoke(null, new object[] { value, majorDigits }) ?? throw new InvalidOperationException();
        }
    }
}
