using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using DictStrObj = System.Collections.Generic.Dictionary<string, object>;

namespace Nodejobs.Client;

public class DJson
{
    public static List<string> UnpackStrings(object item)
    {
        if (item is string s)
        {
            return new List<string> { s };
        }

        if (item is IList list)
        {
            var master = new List<string>();
            foreach (var elem in list)
            {
                var expanded = UnpackStrings(elem);
                for (int i = 0; i < expanded.Count; i++)
                {
                    master.Add(expanded[i]);
                }
            }
            return master;
        }

        return new List<string>();
    }

    public static bool TrySetKeyAt(
        IDictionary<string, object> root,
        IList<string> keys,
        object value,
        bool overwrite = true)
    {
        if (root == null || keys == null || keys.Count == 0) return false;

        IDictionary<string, object> cur = root;
        for (int i = 0; i < keys.Count - 1; i++)
        {
            string seg = keys[i];
            if (string.IsNullOrWhiteSpace(seg)) return false;

            if (!cur.TryGetValue(seg, out var next) || next == null)
            {
                var created = new Dictionary<string, object>();
                cur[seg] = created;
                cur = created;
                continue;
            }

            if (next is IDictionary<string, object> nextDict)
            {
                cur = nextDict;
                continue;
            }

            if (!overwrite) return false;
            var replaced = new Dictionary<string, object>();
            cur[seg] = replaced;
            cur = replaced;
        }

        string leaf = keys[keys.Count - 1];
        if (string.IsNullOrWhiteSpace(leaf)) return false;
        if (!overwrite && cur.ContainsKey(leaf)) return false;

        cur[leaf] = value;
        return true;
    }

    public static object TryGetKeyAt(
        IDictionary<string, object> root,
        IList<string> keys,
        object null_substitute = null)
    {
        if (root == null || keys == null || keys.Count == 0) return null_substitute;

        object cur = root;
        for (int i = 0; i < keys.Count; i++)
        {
            string seg = keys[i];
            if (string.IsNullOrWhiteSpace(seg)) return null_substitute;
            if (cur is not IDictionary<string, object> dict) return null_substitute;
            if (!dict.TryGetValue(seg, out cur)) return null_substitute;
        }

        return cur ?? null_substitute;
    }

    public static void SafeCopyKey(string key, DictStrObj dest, Dictionary<string, object> src)
    {
        if (src == null)
        {
            throw new ArgumentNullException(nameof(src), "Source dictionary is null.");
        }

        if (src.TryGetValue(key, out var value))
        {
            if (value is string strValue)
            {
                dest[key] = strValue;
            }
            else if (value is DateTime dtTime)
            {
                dest[key] = dtTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ");
            }
            else
            {
                throw new InvalidCastException($"Value for key '{key}' is not a string.");
            }
        }
        else
        {
            throw new KeyNotFoundException($"Key '{key}' is missing in the source dictionary.");
        }
    }

    public static (bool isValid, string error) ValidateJsonSchema(Dictionary<string, object> json, Dictionary<string, object> schema)
    {
        foreach (var key in schema.Keys)
        {
            if (!json.ContainsKey(key))
            {
                return (false, $"Missing required key: {key}");
            }

            var expectedType = schema[key];
            var actualValue = json[key];

            if (expectedType is Dictionary<string, object> nestedSchema)
            {
                if (actualValue is not Dictionary<string, object> nestedJson)
                {
                    return (false, $"Expected '{key}' to be a nested object, but got {actualValue.GetType()}");
                }

                var (isNestedValid, nestedError) = ValidateJsonSchema(nestedJson, nestedSchema);
                if (!isNestedValid)
                {
                    return (false, nestedError);
                }
            }
            else
            {
                if (!IsValidType(actualValue, expectedType))
                {
                    return (false, $"Invalid type for '{key}': Expected {expectedType}, got {actualValue.GetType()}");
                }
            }
        }

        return (true, "");
    }

    private static bool IsValidType(object value, object expectedType)
    {
        return expectedType switch
        {
            "string" => value is string,
            "int" => value is int,
            "bool" => value is bool,
            _ => throw new InvalidOperationException($"Unknown type in schema: {expectedType}")
        };
    }

    private static bool IsRecursiveDict(object obj)
    {
        return obj is IDictionary<string, object> || obj is IDictionary<string, string>;
    }

    private static bool IsRecursiveList(object obj)
    {
        if (obj is ITuple) return true;
        return obj is IList && obj is not string;
    }

    private static List<object> AsRecursiveList(object obj)
    {
        if (obj is ITuple tup)
        {
            var list = new List<object>();
            for (int i = 0; i < tup.Length; i++)
            {
                list.Add(tup[i]!);
            }
            return list;
        }

        if (obj is IEnumerable enumerable && obj is not string)
        {
            var list = new List<object>();
            foreach (var item in enumerable)
            {
                list.Add(item!);
            }
            return list;
        }

        throw new ArgumentException("Object is not a supported list type.");
    }

    private static Dictionary<string, object> AsRecursiveDict(object obj)
    {
        if (obj is IDictionary<string, object> dso)
            return dso.ToDictionary(kv => kv.Key, kv => kv.Value);

        if (obj is IDictionary<string, string> dss)
            return dss.ToDictionary(kv => kv.Key, kv => (object)kv.Value);

        throw new ArgumentException("Object is not a supported recursive dict type.");
    }

    static string JsonEscape(string s)
    {
        var sb = new StringBuilder();
        foreach (char c in s)
        {
            switch (c)
            {
                case '\\': sb.Append("\\\\"); break;
                case '"': sb.Append("\\\""); break;
                case '\b': sb.Append("\\b"); break;
                case '\f': sb.Append("\\f"); break;
                case '\n': sb.Append("\\n"); break;
                case '\r': sb.Append("\\r"); break;
                case '\t': sb.Append("\\t"); break;
                default:
                    if (c < ' ') sb.AppendFormat("\\u{0:X4}", (int)c);
                    else sb.Append(c);
                    break;
            }
        }
        return sb.ToString();
    }

    public static string Stringify(object jsonObject, int indentLevel = 0, bool debug = false)
    {
        if (jsonObject is null) return "null";
        if (jsonObject is string s) return s;
        if (jsonObject is int i) return i.ToString();
        if (jsonObject is float f) return f.ToString();
        if (jsonObject is double d) return d.ToString();
        if (jsonObject is bool b) return b.ToString();
        if (jsonObject is byte by) return by.ToString();

        var sb = new StringBuilder();
        BuildJsonTreeString(jsonObject, sb, indentLevel, debug);
        return sb.ToString();
    }

    private static void BuildJsonTreeString(object jsonObjectIn, StringBuilder sb, int indentLevel, bool debug = false)
    {
        string indent = new string(' ', indentLevel * 4);
        object jsonObject = jsonObjectIn;
        if (jsonObjectIn is Dictionary<string, Dictionary<string, object>>)
        {
            jsonObject = new Dictionary<string, object>();
            foreach (string key2 in ((Dictionary<string, Dictionary<string, object>>)jsonObjectIn).Keys)
            {
                ((Dictionary<string, object>)jsonObject)[key2] = ((Dictionary<string, Dictionary<string, object>>)jsonObjectIn)[key2];
            }
        }

        bool first = true;

        if (IsRecursiveDict(jsonObject))
        {
            Dictionary<string, object> dict = AsRecursiveDict(jsonObject);
            sb.AppendLine($"\n{indent}{{");
            foreach (var kvp in dict)
            {
                if (first == false) sb.AppendLine(",");
                sb.Append($"{indent}    \"{kvp.Key}\": ");
                BuildJsonTreeString(kvp.Value, sb, indentLevel + 1);
                first = false;
            }
            sb.AppendLine($"\n{indent}}}");
        }
        else if (IsRecursiveList(jsonObject))
        {
            List<object> list = AsRecursiveList(jsonObject);
            string indent_li = new string(' ', (indentLevel + 1) * 4);
            sb.AppendLine($"[");
            foreach (var item in list)
            {
                if (first == false) sb.AppendLine(",");
                sb.Append($"{indent_li}");
                BuildJsonTreeString(item, sb, indentLevel + 1);
                first = false;
            }
            sb.Append($"\n{indent_li}]");
        }
        else
        {
            try
            {
                switch (jsonObject)
                {
                    case null:
                        sb.Append("null");
                        break;
                    case string s:
                        string ss = JsonEscape(s);
                        sb.Append($"\"{ss}\"");
                        break;
                    case bool b:
                        sb.Append(b ? "true" : "false");
                        break;
                    case char c:
                        sb.Append($"\"{c}\"");
                        break;
                    case var n when n is byte or sbyte or short or ushort or int or uint or long or ulong:
                        sb.Append(n.ToString());
                        break;
                    case var n when n is float or double or decimal:
                        sb.Append(Convert.ToString(n, CultureInfo.InvariantCulture));
                        break;
                    case DateTime dt:
                        sb.Append($"\"{dt:O}\"");
                        break;
                    case Guid g:
                        sb.Append($"\"{g}\"");
                        break;
                    default:
                        sb.Append($"\"{jsonObject.ToString()}\"");
                        break;
                }
            }
            catch (Exception e)
            {
                string errReport;
                if (jsonObject != null) errReport = $"Parse error ({e}):(-NULL-)";
                else errReport = $"Parse error ({e}):({jsonObject.GetType().ToString()})";
                System.Diagnostics.Debug.WriteLine(errReport);
                sb.Append($"\"{errReport}\"");
                throw;
            }
        }
    }

    public static Dictionary<string, string> ToDictStrStr(Dictionary<string, object> input)
    {
        var result = new Dictionary<string, string>();
        foreach (var kvp in input)
        {
            result[kvp.Key] = (string)kvp.Value;
        }
        return result;
    }

    public static Dictionary<string, object> Parse(string json)
    {
        JObject jsonObject = JsonConvert.DeserializeObject<JObject>(json);
        return ParseJObjectDict(jsonObject);
    }

    public static object ParseGeneric(string json)
    {
        var jsonObject = JsonConvert.DeserializeObject(json);
        if (jsonObject is JObject nestedObject)
        {
            return ParseJObjectDict(nestedObject);
        }
        else if (jsonObject is JArray array)
        {
            return ParseJObjectList(array);
        }
        return null;
    }

    private static Dictionary<string, object> ParseJObjectDict(JObject jObject)
    {
        if (jObject == null) return null;
        var dictionary = new Dictionary<string, object>();

        foreach (var property in jObject.Properties())
        {
            var value = property.Value;
            if (value is JObject nestedObject)
            {
                dictionary[property.Name] = ParseJObjectDict(nestedObject);
            }
            else if (value is JArray array)
            {
                dictionary[property.Name] = ParseJObjectList(array);
            }
            else
            {
                dictionary[property.Name] = ((JValue)value).Value;
            }
        }

        return dictionary;
    }

    private static List<object> ParseJObjectList(JArray array)
    {
        var list = new List<object>();

        foreach (var item in array)
        {
            if (item is JObject nestedObject)
            {
                list.Add(ParseJObjectDict(nestedObject));
            }
            else if (item is JArray nestedArray)
            {
                list.Add(ParseJObjectList(nestedArray));
            }
            else
            {
                list.Add(((JValue)item).Value);
            }
        }

        return list;
    }

    public static object MergeGeneric(object baseTree, object overrideTree, MergeMode mode)
    {
        if (baseTree == null) return DeepClone(overrideTree);
        if (overrideTree == null) return DeepClone(baseTree);

        if (baseTree is Dictionary<string, object> baseDict && overrideTree is Dictionary<string, object> overrideDict)
        {
            return MergeDictionaries(baseDict, overrideDict, mode);
        }

        if (baseTree is List<object> baseList && overrideTree is List<object> overrideList)
        {
            return MergeLists(baseList, overrideList, mode);
        }

        return ApplyMergeBehavior(baseTree, overrideTree, mode);
    }

    private static Dictionary<string, object> MergeDictionaries(
        Dictionary<string, object> baseDict,
        Dictionary<string, object> overrideDict,
        MergeMode mode)
    {
        var result = new Dictionary<string, object>(baseDict);

        foreach (var kvp in overrideDict)
        {
            if (result.TryGetValue(kvp.Key, out var existingValue))
            {
                if (existingValue is Dictionary<string, object> baseSubDict
                    && kvp.Value is Dictionary<string, object> overrideSubDict)
                {
                    result[kvp.Key] = MergeDictionaries(baseSubDict, overrideSubDict, mode);
                }
                else
                {
                    result[kvp.Key] = ApplyMergeBehavior(existingValue, kvp.Value, mode);
                }
            }
            else
            {
                result[kvp.Key] = DeepClone(kvp.Value);
            }
        }

        return result;
    }

    private static List<object> MergeLists(List<object> baseList, List<object> overrideList, MergeMode mode)
    {
        if (mode == MergeMode.Append)
        {
            return baseList.Concat(overrideList).Select(DeepClone).ToList();
        }
        else if (mode == MergeMode.Replace)
        {
            return DeepClone(overrideList) as List<object>;
        }

        throw new ArgumentException("Unsupported merge mode");
    }

    private static object ApplyMergeBehavior(object baseValue, object overrideValue, MergeMode mode)
    {
        if (baseValue is Dictionary<string, object> baseDict && overrideValue is Dictionary<string, object> overrideDict)
        {
            return MergeDictionaries(baseDict, overrideDict, mode);
        }

        if (mode == MergeMode.Append)
        {
            return DeepClone(baseValue);
        }
        else if (mode == MergeMode.Replace)
        {
            return DeepClone(overrideValue);
        }

        throw new ArgumentException("Unsupported merge mode");
    }

    private static object DeepClone(object obj)
    {
        if (obj is Dictionary<string, object> dict)
        {
            return dict.ToDictionary(kvp => kvp.Key, kvp => DeepClone(kvp.Value));
        }

        if (obj is List<object> list)
        {
            return list.Select(DeepClone).ToList();
        }

        return obj;
    }

    public enum MergeMode
    {
        Append,
        Replace
    }

    // ADDITIVE BaseData conversion helpers. No pre-existing DJson method is changed.
    // Converts JSON-parsed List<object> and nested dictionaries to declared types.
    public static object ConvertBaseDataValue(Type expected, object value)
    {
        if (expected == null)
            throw new ArgumentNullException(nameof(expected));
        if (expected == typeof(object))
            return value;

        var nullable = Nullable.GetUnderlyingType(expected);
        if (value == null)
        {
            if (nullable != null)
                return null;
            throw new InvalidCastException("invalid_type");
        }
        if (nullable != null)
            expected = nullable;

        if (typeof(BaseData).IsAssignableFrom(expected))
        {
            if (expected.IsInstanceOfType(value))
                return value;
            if (value is not IDictionary<string, object> source)
                throw new InvalidCastException("invalid_type");
            try
            {
                // Derived contract classes expose a public dictionary constructor.
                var instance = Activator.CreateInstance(expected,
                    new object[] { new Dictionary<string, object>(source) });
                if (instance is not BaseData)
                    throw new InvalidCastException("invalid_type");
                return instance;
            }
            catch (System.Reflection.TargetInvocationException ex)
            {
                // Preserve nested contract error identifiers, not reflection errors.
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(
                    ex.InnerException ?? ex).Throw();
                throw;
            }
        }

        if (expected.IsArray && expected.GetArrayRank() == 1)
        {
            var values = ConvertBaseDataList(expected.GetElementType()!, value);
            var array = Array.CreateInstance(expected.GetElementType()!, values.Count);
            values.CopyTo(array, 0);
            return array;
        }

        if (expected.IsGenericType && expected.GetGenericTypeDefinition() == typeof(List<>))
            return ConvertBaseDataList(expected.GetGenericArguments()[0], value);

        if (expected.IsGenericType && expected.GetGenericTypeDefinition() == typeof(Dictionary<,>))
        {
            var arguments = expected.GetGenericArguments();
            if (arguments[0] != typeof(string))
                throw new InvalidCastException("invalid_type");
            var target = (IDictionary)Activator.CreateInstance(expected)!;
            if (value is IDictionary<string, object> source)
            {
                foreach (var kv in source)
                    target.Add(kv.Key, ConvertBaseDataValue(arguments[1], kv.Value));
                return target;
            }
            if (value is IDictionary other)
            {
                foreach (DictionaryEntry kv in other)
                {
                    if (kv.Key is not string key)
                        throw new InvalidCastException("invalid_type");
                    target.Add(key, ConvertBaseDataValue(arguments[1], kv.Value));
                }
                return target;
            }
            throw new InvalidCastException("invalid_type");
        }

        // Safe, narrow JSON numeric conversions. Never parse numbers from strings.
        if (expected == typeof(int) && value is long longValue)
        {
            if (longValue < int.MinValue || longValue > int.MaxValue)
                throw new InvalidCastException("invalid_type");
            return checked((int)longValue);
        }
        if (expected == typeof(long) && value is int intValue)
            return (long)intValue;
        if (expected == typeof(double))
        {
            if (value is int i) return (double)i;
            if (value is long l) return (double)l;
            if (value is float f) return (double)f;
        }
        if (expected.IsInstanceOfType(value))
            return value;
        throw new InvalidCastException("invalid_type");
    }

    // General-purpose typed-list conversion; also used for nested List<ChildData>.
    public static IList ConvertBaseDataList(Type elementType, object value)
    {
        if (elementType == null)
            throw new ArgumentNullException(nameof(elementType));
        if (value is string || value is IDictionary || value is IDictionary<string, object>
            || value is not IEnumerable enumerable)
            throw new InvalidCastException("invalid_type");
        var result = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(elementType))!;
        foreach (var item in enumerable)
            result.Add(ConvertBaseDataValue(elementType, item));
        return result;
    }

    // Post-hook validation is STRICT: a hook must not leave a new, unconverted type.
    public static bool MatchesBaseDataType(Type expected, object value)
    {
        if (expected == typeof(object)) return true;
        var nullable = Nullable.GetUnderlyingType(expected);
        if (value == null) return nullable != null;
        if (nullable != null) expected = nullable;

        if (expected.IsArray && expected.GetArrayRank() == 1)
        {
            if (!expected.IsInstanceOfType(value)) return false;
            var elementType = expected.GetElementType()!;
            foreach (var element in (IEnumerable)value)
                if (!MatchesBaseDataType(elementType, element)) return false;
            return true;
        }
        if (expected.IsGenericType && expected.GetGenericTypeDefinition() == typeof(List<>))
        {
            if (!expected.IsInstanceOfType(value)) return false;
            var elementType = expected.GetGenericArguments()[0];
            foreach (var element in (IEnumerable)value)
                if (!MatchesBaseDataType(elementType, element)) return false;
            return true;
        }
        if (expected.IsGenericType && expected.GetGenericTypeDefinition() == typeof(Dictionary<,>))
        {
            if (!expected.IsInstanceOfType(value)) return false;
            var types = expected.GetGenericArguments();
            if (types[0] != typeof(string)) return false;
            foreach (DictionaryEntry pair in (IDictionary)value)
            {
                if (pair.Key is not string || !MatchesBaseDataType(types[1], pair.Value))
                    return false;
            }
            return true;
        }
        return expected.IsInstanceOfType(value);
    }
}
