using System.ComponentModel;
using System.Globalization;
using System.Linq.Expressions;
using System.Numerics;
using System.Reflection;

namespace Halforbit.Stores;

public static class DictionaryToTypeConverter<TObject>
{
    static readonly Func<Dictionary<string, string>, TObject> _fromDictionary = BuildFactoryFunc();
    
    public static TObject FromDictionary(Dictionary<string, string> dictionary) => _fromDictionary(dictionary);

    static Func<Dictionary<string, string>, TObject> BuildFactoryFunc()
    {
        var typeInfo = typeof(TObject).GetTypeInfo();

        var constructors = typeInfo
            .DeclaredConstructors
            .Select(ci => new
            {
                ConstructorInfo = ci,
                Parameters = ci.GetParameters()
            })
            .ToList();

        var constructor = constructors
            .OrderByDescending(c => c.Parameters.Count())
            .First();

        var arguments = new List<Expression>();

        var dictionaryParameter = Expression.Parameter(
            typeof(Dictionary<string, string>),
            "d");

        var properties = typeInfo.GetProperties()
            .ToDictionary(p => p.Name, p => p);

        var propertiesToSet = properties
            .Where(p => p.Value.SetMethod != null)
            .ToDictionary(p => p.Key, p => p.Value);

        var fields = typeInfo.GetFields()
            .ToDictionary(f => f.Name.ToLower(), f => f);

        foreach (var parameter in constructor.Parameters)
        {
            var key = parameter.Name.ToLower();

            var parameterType = parameter.ParameterType;

            if (properties.Any() && properties.ContainsKey(key))
            {
                var property = properties[key];

                arguments.Add(BuildPropertyArgumentExpression(
                    dictionaryParameter,
                    key,
                    property));
            }
            else if (fields.ContainsKey(key))
            {
                arguments.Add(BuildFieldArgumentExpression(
                    dictionaryParameter,
                    key,
                    fields[key],
                    parameterType));
            }

            propertiesToSet.Remove(key);
        }

        var newExpression = Expression.New(
            constructor.ConstructorInfo,
            arguments);

        var outerExpression = newExpression as Expression;

        if (propertiesToSet.Any())
        {
            var memberBindings = propertiesToSet
                .Select(p => Expression.Bind(
                    p.Value,
                    BuildPropertyArgumentExpression(
                        dictionaryParameter,
                        key: p.Key,
                        propertyInfo: p.Value)))
                .ToList();

            outerExpression = Expression.MemberInit(newExpression, memberBindings);
        }

        var lambda = Expression.Lambda<Func<Dictionary<string, string>, TObject>>(
            outerExpression,
            dictionaryParameter);

        return lambda.Compile();
    }

    static string? GetValueOrNull(Dictionary<string, string> dictionary, string key)
    {
        if (dictionary.TryGetValue(key, out var value))
        {
            return value;
        }

        return null;
    }

    static object ParseValue(Type type, string? value, int majorDigits)
    {
        if (value is null)
        {
            return type.IsValueType ? Activator.CreateInstance(type) : null;
        }

        if (typeof(string).Equals(type))
        {
            return value;
        }

        if (typeof(Guid).Equals(type))
        {
            return Guid.Parse(value);
        }

        if (typeof(INumber<>).MakeGenericType(type).IsAssignableFrom(type))
        {
            return SortableNumberParser.ParseNumber(value, type, majorDigits);
        }

        var typeConverter = TypeDescriptor.GetConverter(type);
        
        if (typeConverter != null && typeConverter.CanConvertFrom(typeof(string)))
        {
            return typeConverter.ConvertFromString(null, CultureInfo.InvariantCulture, value);
        }
        else
        {
            return Activator.CreateInstance(type);
        }
    }

    static UnaryExpression BuildPropertyArgumentExpression(
        ParameterExpression dictionaryParameter,
        string key,
        PropertyInfo propertyInfo)
    {
        var propertyType = propertyInfo.PropertyType;

        var attribute = propertyInfo.GetCustomAttribute<DictionaryMemberAttribute>();

        var majorDigits = 15;

        if (attribute is not null)
        {
            key = attribute.Key ?? key;

            majorDigits = attribute.MajorDigits;
        }

        var getValueMethod = typeof(DictionaryToTypeConverter<TObject>)
            .GetMethod(nameof(GetValueOrNull), BindingFlags.NonPublic | BindingFlags.Static) ?? 
            throw new ArgumentException("Could not find TryGetValue method.");

        var getValueCall = Expression.Call(
            getValueMethod,
            dictionaryParameter,
            Expression.Constant(key));

        var defaultValue = propertyType.IsValueType ? 
            Activator.CreateInstance(propertyType) : 
            null;

		var condition = Expression.Equal(
			getValueCall,
			Expression.Default(typeof(string)));

		var parseValueMethod = typeof(DictionaryToTypeConverter<TObject>)
			.GetMethod(nameof(ParseValue), BindingFlags.NonPublic | BindingFlags.Static) ??
			throw new ArgumentException("Could not find ParseValue method.");

		var parseValueCall = Expression.Call(
            parseValueMethod,
            Expression.Constant(propertyType),
            getValueCall,
            Expression.Constant(majorDigits));

		return Expression.Convert(parseValueCall, propertyType);
    }

    static UnaryExpression BuildFieldArgumentExpression(
        ParameterExpression dictionaryParameter,
        string key,
        FieldInfo fieldInfo,
        Type parameterType)
    {
        var tryGetValueMethod = typeof(Dictionary<string, string>).GetMethod("TryGetValue") ?? 
            throw new ArgumentException("Could not find TryGetValue method.");

        var valueParam = Expression.Variable(typeof(string), "value");

        var tryGetValueCall = Expression.Call(
            dictionaryParameter,
            tryGetValueMethod!,
            Expression.Constant(key),
            valueParam);

        var defaultValue = fieldInfo.FieldType.IsValueType ? 
            Activator.CreateInstance(fieldInfo.FieldType) : 
            null;

        var valueExpression = Expression.Condition(
            tryGetValueCall,
            Expression.Call(
                typeof(DictionaryToTypeConverter<TObject>),
                nameof(ParseValue),
                Type.EmptyTypes,
                Expression.Constant(fieldInfo.FieldType),
                valueParam),
            Expression.Constant(defaultValue, fieldInfo.FieldType));

        return Expression.Convert(valueExpression, parameterType);
    }
}
