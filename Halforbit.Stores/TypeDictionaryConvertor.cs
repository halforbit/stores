using Halforbit.Stores;
using System.Linq.Expressions;
using System.Numerics;
using System.Reflection;
using static System.Linq.Expressions.Expression;

namespace Halforbit.Stores;

public static class TypeDictionaryConvertor<T>
{
    static readonly Func<T, Dictionary<string, string>> _objectToDictionary = CreateMapFunction();

    public static Dictionary<string, string> ToDictionary(T obj)
    {
        return _objectToDictionary(obj);
    }

    static Func<T, Dictionary<string, string>> CreateMapFunction()
    {
        var parameter = Parameter(typeof(T), "poco");

        var dictionaryAddMethod = typeof(Dictionary<string, string>).GetMethod("Add") ?? 
            throw new ArgumentNullException("Could not find Dictionary<,>.Add method.");

        var dictionary = Variable(typeof(Dictionary<string, string>), "dictionary");

        var dictionaryInit = Assign(dictionary, New(typeof(Dictionary<string, string>)));

        var body = new List<Expression> { dictionaryInit };

        foreach (var property in typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            Expression propertyValue;
            var dictionaryMemberAttribute = property.GetCustomAttribute<DictionaryMemberAttribute>();
            var majorDigits = dictionaryMemberAttribute?.MajorDigits ?? 15;

            if (IsNumber(property.PropertyType))
            {
                MethodInfo sortableNumberFormatterMethod;
                if (IsFloatingPoint(property.PropertyType))
                {
                    sortableNumberFormatterMethod = typeof(SortableNumberFormatter)
                        .GetMethod(nameof(SortableNumberFormatter.FloatingPointToSortableString))!
                        .MakeGenericMethod(property.PropertyType);
                }
                else
                {
                    sortableNumberFormatterMethod = typeof(SortableNumberFormatter)
                        .GetMethod(nameof(SortableNumberFormatter.IntegerToSortableString))!
                        .MakeGenericMethod(property.PropertyType);
                }

                propertyValue = Call(
                    sortableNumberFormatterMethod,
                    Property(parameter, property),
                    Constant(majorDigits));
            }
            else
            {
                var toStringMethod = property.PropertyType.GetMethod(nameof(ToString), Type.EmptyTypes);
                if (toStringMethod == null)
                {
                    throw new InvalidOperationException($"Type {property.PropertyType} does not have a valid ToString method.");
                }
                propertyValue = Call(
                    Property(parameter, property),
                    toStringMethod);
            }

            var key = dictionaryMemberAttribute?.Key ?? property.Name;

            var addToDictionary = Call(dictionary, dictionaryAddMethod,
                Constant(key), propertyValue);

            body.Add(addToDictionary);
        }

        body.Add(dictionary); // Return the dictionary

        var block = Block(new[] { dictionary }, body);

        var lambda = Lambda<Func<T, Dictionary<string, string>>>(block, parameter);

        return lambda.Compile();
    }

    static bool IsNumber(Type type)
    {
        return type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(INumber<>));
    }

    static bool IsFloatingPoint(Type type)
    {
        return type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IFloatingPoint<>));
    }
}
