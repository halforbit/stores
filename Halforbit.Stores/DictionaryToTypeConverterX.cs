using System.ComponentModel;
using System.Linq.Expressions;
using System.Reflection;
using static System.Linq.Expressions.Expression;

namespace Halforbit.Stores;

public static class DictionaryToTypeConverterX<T>
{
    private static readonly Func<Dictionary<string, string>, T> _fromDictionary = CreateMapFunction();

    public static T FromDictionary(Dictionary<string, string> dictionary)
    {
        return _fromDictionary(dictionary);
    }

    private static Func<Dictionary<string, string>, T> CreateMapFunction()
    {
        var dictionaryParam = Parameter(typeof(Dictionary<string, string>), "dictionary");
        var instance = Variable(typeof(T), "instance");
        var createInstance = Assign(instance, New(typeof(T)));

        var body = new List<Expression> { createInstance };

        var valueExp = Variable(typeof(string), "value");

        foreach (var property in typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            var getProperty = Property(instance, property);
            var tryGetValueMethod = typeof(Dictionary<string, string>).GetMethod("TryGetValue") ?? 
                throw new ArgumentException("Could not find TryGetValue method.");
            var keyExp = Constant(property.Name);

            var tryGetValueExp = Call(
                dictionaryParam,
                tryGetValueMethod,
                [keyExp, valueExp]);

            var convertValueMethod = typeof(DictionaryToTypeConverter<T>)
                .GetMethod(nameof(ConvertStringToType), BindingFlags.NonPublic | BindingFlags.Static)
                ?? throw new InvalidOperationException($"Method {nameof(ConvertStringToType)} not found.");

            var convertValueExp = Convert(
                Call(convertValueMethod, valueExp, Constant(property.PropertyType)),
                property.PropertyType);

            var ifTrueExp = IfThen(
                tryGetValueExp,
                Assign(getProperty, convertValueExp));

            body.Add(ifTrueExp);
        }

        var returnLabel = Label(typeof(T));
        body.Add(Return(returnLabel, instance));
        body.Add(Label(returnLabel, instance));

        var block = Block([instance, valueExp], body);
        var lambda = Lambda<Func<Dictionary<string, string>, T>>(block, dictionaryParam);

        return lambda.Compile();
    }

    private static object? ConvertStringToType(string value, Type targetType)
    {
        if (targetType == typeof(string))
        {
            return value;
        }
        if (targetType == typeof(Guid))
        {
            return Guid.Parse(value);
        }
        else if (targetType == typeof(int))
        {
            return int.Parse(value);
        }
        else if (targetType == typeof(double))
        {
            // Handle the special case of SortableNumberFormatter Parsing for double
            if(value[0] == '+' || value[0] == '-')
            {
                return SortableNumberParser.ParseNumber<double>(value, 15); // Assuming majorDigits is 15 for this example
            }
            return double.Parse(value);
        }

        TypeConverter converter = TypeDescriptor.GetConverter(targetType) ?? 
            throw new ArgumentException($"Cannot get TypeConverter for {targetType.Name}");

        if (converter != null && converter.CanConvertFrom(typeof(string)))
        {
            return converter.ConvertFromInvariantString(value);
        }

        throw new InvalidOperationException($"Cannot convert string to {targetType.Name}");
    }
}
