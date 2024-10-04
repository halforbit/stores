using System.ComponentModel;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;

namespace Halforbit.Stores;

public class KeyMap<TKey>
{
    readonly IReadOnlyList<MapSegment> _mapSegments;

    readonly Func<TKey, string> _toString;

    readonly Regex _matchingRegex;

    readonly IReadOnlyList<Type> _propertyTypes;

    KeyMap(
        Expression<Func<TKey, string>> map,
        string suffix)
    {
        _mapSegments = GetMapSegments(map, suffix);

        _toString = map.Compile();

        _matchingRegex = new Regex(
            BuildMatchingRegex(_mapSegments),
            RegexOptions.Compiled);

        _propertyTypes = typeof(ITuple).IsAssignableFrom(typeof(TKey)) ?
            GetTupleTypes<TKey>() :
            [typeof(TKey)];
    }

    public static KeyMap<TKey> Define(
        Expression<Func<TKey, string>> map, 
        string suffix)
    {
        return new KeyMap<TKey>(
            map, 
            suffix);
    }

    public bool TryMapStringToKey(
        string str,
        out TKey? key)
    {
        var match = _matchingRegex.Match(str);

        if (!match.Success)
        {
            key = default;

            return false;
        }

        var properties = new List<object>();

        for (var i = 1; i < match.Groups.Count; i++)
        {
            var strValue = match.Groups[i].Value;

            var propertyType = _propertyTypes[i - 1];

            properties.Add(ConvertStringToType(strValue, propertyType));
        }

        if (properties.Count == 1)
        {
            key = (TKey)properties[0];

            return true;
        }

        key = (TKey)ConvertToTuple(properties.ToArray());

        return true;
    }

    public bool TryMapKeyToString(
        TKey key, 
        out string str) => TryMapKeyToString(
            key, 
            out str,
            allowPartial: false);

    public bool TryMapPartialKeyToPrefixString(
        object? key, 
        out string prefix) => TryMapKeyToString(
            key, 
            out prefix, 
            allowPartial: true);

    bool TryMapKeyToString(
        object? key, 
        out string str, 
        bool allowPartial)
    {
        var output = new StringBuilder();

        var tuple = key as ITuple;

        foreach (var segment in _mapSegments)
        {
            if (segment.Text.Length > 0)
            {
                output.Append(segment.Text);
            }
            else if (segment.IsParameter)
            {
                if (key is null)
                {
                    if (allowPartial)
                    {
                        str = output.ToString();

                        return true;
                    }
                    else
                    {
                        str = string.Empty;

                        return false;
                    }
                }

                if (segment.Format.Length > 0)
                {
                    if (key is not IFormattable formattable)
                    {
                        throw new ArgumentException("Format was specified but key type is not IFormattable.");
                    }

                    var s = formattable.ToString(
                        segment.Format.ToString(),
                        CultureInfo.InvariantCulture);

                    output.Append(s);
                }
                else
                {
                    output.Append(key.ToString());
                }
            }
            else if (segment.MemberIndex >= 0)
            {
                object value;

                if (tuple is null)
                {
                    if (key is null)
                    {
                        if (allowPartial)
                        {
                            str = output.ToString();

                            return true;
                        }
                        else
                        {
                            str = string.Empty;

                            return false;
                        }
                    }

                    if (segment.MemberIndex > 0)
                    {
                        if (allowPartial)
                        {
                            str = output.ToString();

                            return true;
                        }
                        else
                        {
                            str = string.Empty;

                            return false;
                        }
                    }

                    value = key;
                }
                else
                {
                    if (segment.MemberIndex >= tuple.Length)
                    {
                        if (allowPartial)
                        {
                            str = output.ToString();

                            return true;
                        }
                        else
                        {
                            str = string.Empty;

                            return false;
                        }
                    }

                    value = tuple[segment.MemberIndex];
                }

                if (segment.Format.Length > 0)
                {
                    if (value is not IFormattable formattable)
                    {
                        throw new ArgumentException("Format was specified but value type is not IFormattable.");
                    }

                    var s = formattable.ToString(
                        segment.Format.ToString(),
                        CultureInfo.InvariantCulture);

                    output.Append(s);
                }
                else
                {
                    output.Append(value.ToString());
                }
            }
            else
            {
                throw new ArgumentException("Unrecognized segment content");
            }
        }

        str = output.ToString();

        return true;
    }

    string BuildMatchingRegex(IReadOnlyList<MapSegment> segments)
    {
        var output = new StringBuilder();

        output.Append("^");

        foreach (var segment in segments)
        {
            if (segment.Text.Length > 0)
            {
                output.Append(Regex.Escape(segment.Text.ToString()));
            }
            else
            {
                output.Append("(.*?)");
            }
        }

        output.Append("$");

        return output.ToString();
    }

    IReadOnlyList<Type> GetTupleTypes<TTuple>()
    {
        Type type = typeof(TTuple);

        if (type.IsGenericType && type.FullName.StartsWith("System.ValueTuple"))
        {
            return type.GetGenericArguments();
        }
        else
        {
            throw new ArgumentException("The provided parameter is not a tuple.");
        }
    }

    static object ConvertStringToType(string value, Type targetType)
    {
        if (targetType == typeof(string))
        {
            return value;
        }
        if (targetType == typeof(Guid))
        {
            return Guid.Parse(value); // or Guid.TryParse if you want to handle errors
        }
        else if (targetType == typeof(int))
        {
            return int.Parse(value); // or int.TryParse if you want to handle errors
        }
        else if (targetType == typeof(double))
        {
            return double.Parse(value); // or double.TryParse if you want to handle errors
        }
        // Add other known types as needed

        // Fallback to TypeConverter for other types
        TypeConverter converter = TypeDescriptor.GetConverter(targetType);
        if (converter != null && converter.CanConvertFrom(typeof(string)))
        {
            return converter.ConvertFromInvariantString(value);
        }

        throw new InvalidOperationException($"Cannot convert string to {targetType.Name}");
    }

    static object ConvertToTuple(object[] values)
    {
        Type[] types = values.Select(v => v.GetType()).ToArray();
        Type tupleType = GetTupleType(types.Length).MakeGenericType(types);

        MethodInfo createMethod = typeof(ValueTuple).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == "Create" && m.GetParameters().Length == types.Length)
            .MakeGenericMethod(types);

        return createMethod.Invoke(null, values);
    }

    static Type GetTupleType(int length)
    {
        return length switch
        {
            1 => typeof(ValueTuple<>),
            2 => typeof(ValueTuple<,>),
            3 => typeof(ValueTuple<,,>),
            4 => typeof(ValueTuple<,,,>),
            5 => typeof(ValueTuple<,,,,>),
            6 => typeof(ValueTuple<,,,,,>),
            7 => typeof(ValueTuple<,,,,,,>),
            8 => typeof(ValueTuple<,,,,,,,>), // You can chain more tuples for larger sizes
            _ => throw new ArgumentException("Tuple size is not supported."),
        };
    }

    public static MapSegment[] GetMapSegments<TKey>(
        Expression<Func<TKey, string>> expression,
        string suffix)
    {
        var body = expression.Body;

        if (body is ConstantExpression constant)
        {
            if (string.IsNullOrWhiteSpace(suffix))
            {
                return [
                    new MapSegment
                    {
                        Text = constant.Value.ToString().AsMemory()
                    }];
            }
            else
            {
                return [
                    new MapSegment
                    {
                        Text = constant.Value.ToString().AsMemory()
                    },
                    new MapSegment
                    {
                        Text = suffix.AsMemory()
                    }];
            }
        }

        if (body is MethodCallExpression methodCall)
        {
            if (methodCall.Method.DeclaringType == typeof(string) &&
                methodCall.Method.Name == nameof(string.Format))
            {
                var formatString = (methodCall.Arguments[0] as ConstantExpression).Value.ToString();

                var args = default(Expression[]);

                if (methodCall.Arguments[1] is NewArrayExpression newArray)
                {
                    args = new Expression[newArray.Expressions.Count];

                    for (var i = 0; i < newArray.Expressions.Count; i++)
                    {
                        switch (newArray.Expressions[i])
                        {
                            case UnaryExpression unary:

                                args[i] = unary.Operand;

                                break;

                            case MemberExpression member:

                                args[i] = member;

                                break;

                            default: throw ShapeError();
                        }
                    }
                }
                else
                {
                    args = new Expression[methodCall.Arguments.Count - 1];

                    for (var i = 0; i < args.Length; i++)
                    {
                        switch (methodCall.Arguments[i + 1])
                        {
                            case UnaryExpression unary:

                                args[i] = unary.Operand;

                                break;

                            case MemberExpression member:

                                args[i] = member;

                                break;

                            case ParameterExpression parameter:

                                args[i] = parameter;

                                break;

                            default: throw ShapeError();
                        }
                    }
                }

                return ParseMapSegments(
                    formatString, 
                    args, 
                    suffix);
            }
        }

        throw ShapeError();
    }

    static MapSegment[] ParseMapSegments(
        string format,
        Expression[] args,
        string suffix)
    {
        // This method is adapted from the `AppendFormatHelper` method of the `StringBuilder` class in
        // the .NET source code:
        // https://referencesource.microsoft.com/#mscorlib/system/text/stringbuilder.cs,2c3b4c2e7c43f5a4

        if (format == null)
        {
            throw new ArgumentNullException("format");
        }

        var segments = new List<MapSegment>();

        var result = new StringBuilder();

        var segmentIndex = 0;

        var curSegment = new StringBuilder();

        int pos = 0;
        int len = format.Length;
        char ch = '\x0';

        while (true)
        {
            int p = pos;
            int i = pos;
            while (pos < len)
            {
                ch = format[pos];

                pos++;
                if (ch == '}')
                {
                    if (pos < len && format[pos] == '}') // Treat as escape character for }}
                        pos++;
                    else
                        throw FormatError();
                }

                if (ch == '{')
                {
                    if (pos < len && format[pos] == '{') // Treat as escape character for {{
                        pos++;
                    else
                    {
                        pos--;
                        break;
                    }
                }

                result.Append(ch);

                curSegment.Append(ch);
            }

            if (pos == len) break;
            pos++;
            if (pos == len || (ch = format[pos]) < '0' || ch > '9') FormatError();
            int index = 0;
            do
            {
                index = index * 10 + ch - '0';
                pos++;
                if (pos == len) throw FormatError();
                ch = format[pos];
            } while (ch >= '0' && ch <= '9' && index < 1000000);
            if (index >= args.Length) throw new FormatException("Format string is invalid because index is out of range.");
            while (pos < len && (ch = format[pos]) == ' ') pos++;
            int width = 0;
            if (ch == ',')
            {
                pos++;
                while (pos < len && format[pos] == ' ') pos++;

                if (pos == len) throw FormatError();
                ch = format[pos];
                if (ch == '-')
                {
                    pos++;
                    if (pos == len) throw FormatError();
                    ch = format[pos];
                }
                if (ch < '0' || ch > '9') throw FormatError();
                do
                {
                    width = width * 10 + ch - '0';
                    pos++;
                    if (pos == len) throw FormatError();
                    ch = format[pos];
                } while (ch >= '0' && ch <= '9' && width < 1000000);
            }

            while (pos < len && (ch = format[pos]) == ' ') pos++;
            var arg = args[index];
            StringBuilder fmt = null;
            if (ch == ':')
            {
                pos++;
                p = pos;
                i = pos;
                while (true)
                {
                    if (pos == len) throw FormatError();
                    ch = format[pos];
                    pos++;
                    if (ch == '{')
                    {
                        if (pos < len && format[pos] == '{')  // Treat as escape character for {{
                            pos++;
                        else
                            throw FormatError();
                    }
                    else if (ch == '}')
                    {
                        if (pos < len && format[pos] == '}')  // Treat as escape character for }}
                            pos++;
                        else
                        {
                            pos--;
                            break;
                        }
                    }

                    if (fmt == null)
                    {
                        fmt = new StringBuilder();
                    }
                    fmt.Append(ch);
                }
            }
            if (ch != '}') throw FormatError();
            pos++;

            if (curSegment.Length > 0)
            {
                segments.Add(new MapSegment
                {
                    Text = curSegment.ToString().AsMemory()
                });

                curSegment.Clear();
            }

            string s = null;

            var isMember = false;

            var segmentFormat = default(string);

            if (arg is MemberExpression mex)
            {
                var memberIndex = int.Parse(mex.Member.Name.Substring("Item".Length)) - 1;

                if (fmt != null)
                {
                    s = $"{{{mex.Member.Name}:{fmt}}}";

                    segments.Add(new MapSegment
                    {
                        MemberIndex = memberIndex,
                        Format = fmt.ToString().AsMemory()
                    });
                }
                else
                {
                    s = $"{{{mex.Member.Name}}}";

                    segments.Add(new MapSegment
                    {
                        MemberIndex = memberIndex,
                    });
                }

                isMember = true;
            }
            else if (arg is ParameterExpression pex)
            {
                if (fmt != null)
                {
                    s = $"{{this:{fmt}}}";

                    segments.Add(new MapSegment
                    {
                        IsParameter = true,
                        Format = fmt.ToString().AsMemory()
                    });
                }
                else
                {
                    s = "{this}";

                    segments.Add(new MapSegment
                    {
                        IsParameter = true,
                    });
                }
            }
            else
            {
                throw ShapeError();
            }

            result.Append(s);
        }

        if (curSegment.Length > 0)
        {
            segments.Add(new MapSegment
            {
                Text = curSegment.ToString().AsMemory()
            });

            curSegment.Clear();
        }

        if (!string.IsNullOrWhiteSpace(suffix))
        {
            segments.Add(new() { Text = suffix.AsMemory() });
        }

        return segments.ToArray();
    }

    static Exception FormatError() => new ArgumentException("The format string is formatted incorrectly.");

    static Exception ShapeError() => new ArgumentException(
        "The map expression is not in a recognized shape. " +
        "Map expressions may be a simple string constant, or a string interpolation having nested expressions " +
        "that only reference either a property of the key parameter, or the key parameter itself. Nested " +
        "expressions may have a format string tail separated with a `:`. " +
        "For example: `key => $\"forecasts/{key.PostalCode}/{key.Date:yyyy/MM/dd}\"`");
}
