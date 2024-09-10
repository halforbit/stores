using System;
using System.Globalization;
using System.Numerics;

namespace Halforbit.Stores;

/// <summary>
/// Turns numbers into strings that sort alphabetically.
///
/// Alphabetical sort is made possible by:
///
/// - left-padding the number with 0s so the non-decimal component of the 
///   number is of fixed length specified by `majorDigits`.
///
/// - representing negative numbers by subtracting them from the number 
///   representable by `majorDigits` + 1, 
///   so e.g. -1.23 with a `majorDigits` of 3 would be 1000 - 1.23 = `-998.77`.
///
/// - prefixing with `+` or `-` to indicate sign.
///
/// With a `majorDigits` of 3, the numbers and their strings would be:
///     12.34 --> `+012.34`
///     -12.34 --> `-987.66`, 
///     -1.23 --> `-998.77`
///     1.2 -->  `+001.2`
///     1 --> `+001`
///     -1 --> `-999`
/// </summary>
public static class SortableNumberFormatter
{
    public static string IntegerToSortableString<TValue>(TValue value, int majorDigits)
        where TValue : INumber<TValue>
    {
        string sign = GetSign(value);
        string formattedValue;

        if (value < TValue.Zero)
        {
            TValue majorMaxValue = TValue.CreateChecked(Math.Pow(10, majorDigits));
            TValue adjustedValue = majorMaxValue + value;
            formattedValue = adjustedValue.ToString(new string('0', majorDigits), CultureInfo.InvariantCulture);
        }
        else
        {
            TValue absValue = TValue.Abs(value);
            formattedValue = absValue.ToString(new string('0', majorDigits), CultureInfo.InvariantCulture);
        }

        return $"{sign}{formattedValue}";
    }

    public static string FloatingPointToSortableString<TValue>(TValue value, int majorDigits)
        where TValue : IFloatingPoint<TValue>
    {
        var decimalValue = Convert.ToDecimal(value);

        string sign = GetSign(value);
        var absValue = decimal.Abs(decimalValue);
        var integerPart = decimal.Floor(absValue);
        var decimalPart = absValue - integerPart;

        if (value < TValue.Zero)
        {
            var majorMaxValue = decimal.CreateChecked(Math.Pow(10, majorDigits));
            var invertedValue = majorMaxValue - absValue;
            integerPart = decimal.Floor(invertedValue);
            decimalPart = invertedValue - integerPart;
            sign = "-";
        }

        var formattedValue = 
            integerPart.ToString(new string('0', majorDigits), CultureInfo.InvariantCulture) + 
            GetFormattedDecimalPart(decimalPart);

        return $"{sign}{formattedValue}";
    }

    static string GetFormattedDecimalPart<TValue>(TValue value)
        where TValue : IFloatingPoint<TValue>
    {
        if(value == TValue.Zero) return "";

        int fractionalDigitCount = GetFractionalDigitCount(value);

        return value
            .ToString($"0.{new string('0', fractionalDigitCount)}", CultureInfo.InvariantCulture)
            .Substring(1)
            .TrimEnd('0')
            .TrimEnd('.');
    }

    static string GetSign<TValue>(TValue value) 
        where TValue : INumber<TValue>, IComparable<TValue>
    {
        return value.CompareTo(TValue.Zero) >= 0 ? "+" : "-";
    }

    static int GetFractionalDigitCount<TValue>(TValue value)
        where TValue: IFloatingPoint<TValue>, IComparable<TValue>
    {
        value = TValue.Abs(value);
        value -= TValue.Truncate(value);

        int count = 0;
        while (value > TValue.Zero)
        {
            value *= TValue.CreateChecked(10);
            value -= TValue.Truncate(value);
            count++;
        }

        return count;
    }
}
