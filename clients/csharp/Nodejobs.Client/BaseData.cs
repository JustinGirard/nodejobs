namespace Nodejobs.Client;

using System.Collections;
using System.Linq;

public abstract class BaseData
{
    private readonly Dictionary<string, object> _data;
    private readonly Dictionary<string, object> _dirty;

    protected BaseData(Dictionary<string, object> input)
    {
        if (input == null) throw new Exception("basedata_requires_object");
        _dirty = new Dictionary<string, object>(input);
        var processed = DoPreProcess(new Dictionary<string, object>(input));
        if (processed == null) throw new Exception("do_pre_process_must_return_object");

        var required = RequiredKeys();
        var optional = OptionalKeys();
        var work = new Dictionary<string, object>(processed);

        foreach (var kv in required)
        {
            if (!work.ContainsKey(kv.Key)) throw new Exception($"missing_required_key:{kv.Key}");
            work[kv.Key] = ValidateAndConvert(kv.Key, work[kv.Key], kv.Value);
        }

        foreach (var kv in optional)
        {
            if (work.ContainsKey(kv.Key))
            {
                work[kv.Key] = ValidateAndConvert(kv.Key, work[kv.Key], kv.Value);
            }
        }

        foreach (var kv in Defaults())
        {
            if (!work.ContainsKey(kv.Key))
            {
                work[kv.Key] = kv.Value;
            }
        }

        foreach (var key in work.Keys.ToList())
        {
            work[key] = DoEveryValidation(key, work[key]);
        }

        _data = work;
    }

    protected Dictionary<string, object> GetDirtyDict()
    {
        return _dirty;
    }

    protected abstract Dictionary<string, Type> RequiredKeys();
    protected virtual Dictionary<string, Type> OptionalKeys() => new();
    protected virtual Dictionary<string, object> Defaults() => new();
    protected virtual Dictionary<string, object> DoPreProcess(Dictionary<string, object> input) => input;
    protected virtual object DoValidation(string key, object value) => value;
    protected virtual object DoEveryValidation(string key, object value) => value;

    protected T Get<T>(string key)
    {
        if (!_data.ContainsKey(key)) throw new Exception($"missing_key:{key}");
        return (T)_data[key];
    }

    protected T? GetOptional<T>(string key)
    {
        if (!_data.ContainsKey(key)) return default;
        return (T)_data[key];
    }

    private object ValidateAndConvert(string key, object value, Type expected)
    {
        var coerced = Coerce(expected, value);
        var validated = DoValidation(key, coerced);
        if (!ValidType(validated, expected)) throw new Exception($"invalid_type:{key}");
        return validated;
    }

    private static object Coerce(Type expected, object value)
    {
        if (expected == typeof(double) && value is int i)
        {
            return (double)i;
        }
        if (expected == typeof(int) && value is long l)
        {
            return (int)l;
        }
        if (expected == typeof(List<string>) && value is IEnumerable enumerable && value is not string)
        {
            var outList = new List<string>();
            foreach (var item in enumerable)
            {
                if (item is string s)
                {
                    outList.Add(s);
                }
                else
                {
                    return value;
                }
            }
            return outList;
        }
        return value;
    }

    private static bool ValidType(object value, Type expected)
    {
        if (expected == typeof(object)) return true;
        if (value == null) return false;
        if (expected == typeof(string)) return value is string;
        if (expected == typeof(int)) return value is int;
        if (expected == typeof(double)) return value is double;
        if (expected == typeof(Dictionary<string, object>)) return value is Dictionary<string, object>;
        if (expected == typeof(List<string>)) return value is List<string>;
        return expected.IsAssignableFrom(value.GetType());
    }
}
