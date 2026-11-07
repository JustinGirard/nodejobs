namespace Nodejobs.Client;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Text.Json;

/// <summary>
/// Contract-owned validation with full dictionary preservation.
/// Undeclared keys survive; declared fields are validated at construction and mutation.
/// Consumers should use typed accessors or class-defined field names.
/// </summary>
public class BaseData : IDictionary<string, object>
{
    private static readonly object DiagnosticLock = new();
    private static readonly Dictionary<string, Dictionary<string, object>> Diagnostics = new();
    private static readonly Dictionary<string, Dictionary<string, object>> DiagnosticStarts = new();
    private static readonly Dictionary<string, Dictionary<string, object>> DiagnosticEnds = new();

    public sealed class FieldSpec
    {
        public Type Type { get; }
        public bool IsOptional { get; }
        public bool HasDefault { get; }
        public object DefaultValue { get; }

        public FieldSpec(Type type, bool isOptional, bool hasDefault, object defaultValue)
        {
            Type = type ?? throw new ArgumentNullException(nameof(type));
            IsOptional = isOptional;
            HasDefault = hasDefault;
            DefaultValue = defaultValue;
        }
    }

    protected static FieldSpec Required<T>() => new(typeof(T), false, false, null);
    protected static FieldSpec Optional<T>() => new(typeof(T), true, false, null);
    protected static FieldSpec Defaulted<T>(object defaultValue) => new(typeof(T), true, true, defaultValue);

    private static readonly Regex EnvPattern = new(
        @"<<([A-Z_][A-Z0-9_]*)>>", RegexOptions.Compiled);

    private readonly Dictionary<string, object> _data;
    private readonly Dictionary<string, Type> _required;
    private readonly Dictionary<string, Type> _optional;
    private readonly Dictionary<string, object> _defaults;
    private readonly Dictionary<string, Func<object, object>> _converters;
    private Dictionary<string, object> _dirty;

    // The public object boundary deliberately produces basedata_requires_object
    // for null or non-dictionary inputs, like the Python implementation.
    public BaseData(object input)
    {
        if (input is not IDictionary<string, object> source)
            throw new ArgumentException("basedata_requires_object");

        _dirty = new Dictionary<string, object>(source);
        var processed = DoPreProcess(new Dictionary<string, object>(source));
        if (processed == null)
            throw new InvalidOperationException("do_pre_process_must_return_object");

        var work = (Dictionary<string, object>)DoEnvMapping(processed);
        _required = new Dictionary<string, Type>();
        _optional = new Dictionary<string, Type>();
        _defaults = new Dictionary<string, object>();
        _converters = FieldConverters();

        if (_converters == null)
            throw new InvalidOperationException("invalid_basedata_contract");

        var fieldSpecs = Fields();
        if (fieldSpecs != null && fieldSpecs.Count > 0)
        {
            foreach (var pair in fieldSpecs)
            {
                if (pair.Value == null)
                    throw new InvalidOperationException($"invalid_field_spec:{pair.Key}");

                if (pair.Value.IsOptional)
                    _optional[pair.Key] = pair.Value.Type;
                else
                    _required[pair.Key] = pair.Value.Type;

                if (pair.Value.HasDefault)
                    _defaults[pair.Key] = pair.Value.DefaultValue;
            }
        }
        else
        {
            _required = RequiredKeys() ?? new Dictionary<string, Type>();
            _optional = OptionalKeys() ?? new Dictionary<string, Type>();
            _defaults = Defaults() ?? new Dictionary<string, object>();
        }

        _defaults = (Dictionary<string, object>)DoEnvMapping(_defaults);

        foreach (var key in _required.Keys)
        {
            if (_optional.ContainsKey(key))
                throw new InvalidOperationException($"duplicate_key_definition:{key}");
        }

        // Validate defaults are declared contract keys, but do not inject yet.
        // Python BaseData applies defaults after validation hooks.
        foreach (var item in _defaults)
        {
            if (!_required.ContainsKey(item.Key) && !_optional.ContainsKey(item.Key))
                throw new InvalidOperationException($"default_for_undeclared_key:{item.Key}");
        }

        foreach (var item in _required)
        {
            if (item.Key == "*") continue;
            if (!work.ContainsKey(item.Key))
                throw new ArgumentException($"missing_required_key:{item.Key}");
            work[item.Key] = ValidateField(item.Key, work[item.Key], item.Value);
        }
        foreach (var item in _optional)
        {
            if (item.Key == "*") continue;
            if (work.ContainsKey(item.Key))
                work[item.Key] = ValidateField(item.Key, work[item.Key], item.Value);
        }

        // Python's wildcard get_keys() contract: validate undeclared keys with '*'.
        Type wildcard = null;
        if (_required.TryGetValue("*", out var requiredWildcard))
            wildcard = requiredWildcard;
        if (_optional.TryGetValue("*", out var optionalWildcard))
            wildcard = optionalWildcard;
        if (wildcard != null)
        {
            foreach (var key in work.Keys.ToList())
            {
                if (!_required.ContainsKey(key) && !_optional.ContainsKey(key))
                    work[key] = ValidateField(key, work[key], wildcard);
            }
        }

        foreach (var key in work.Keys.ToList())
        {
            var result = DoEveryValidation(key, work[key]);
            ThrowIfMessage(result.Message);
            if (TryFieldType(key, out var type) || wildcard != null)
            {
                if (type == null)
                    type = wildcard;
                // Hook changes must preserve the declared type.
                CheckFinalType(key, result.Value, type);
            }
            work[key] = result.Value;
        }

        // Apply defaults after validation hooks for parity with Python BaseData.
        foreach (var item in _defaults)
        {
            if (!work.ContainsKey(item.Key) && item.Value != null)
                work.Add(item.Key, item.Value);
        }

        _data = work;
        _dirty = null;
    }

    // Subclasses override these declarations; never repeat them in behavior code.
    protected virtual Dictionary<string, FieldSpec> Fields() => new();
    protected virtual Dictionary<string, Type> RequiredKeys() => new();
    protected virtual Dictionary<string, Type> OptionalKeys() => new();
    protected virtual Dictionary<string, object> Defaults() => new();
    // Contract-owned conversion functions (Python also permits callable field contracts).
    protected virtual Dictionary<string, Func<object, object>> FieldConverters() => new();
    protected virtual Dictionary<string, object> DoPreProcess(Dictionary<string, object> input) => input;
    protected virtual (object Value, string Message) DoValidation(string key, object value) => (value, "");
    protected virtual (object Value, string Message) DoEveryValidation(string key, object value) => (value, "");
    protected virtual object DoGetFiltered(string key, object value) => value;
    protected Dictionary<string, object> GetDirtyDict() => _dirty;

    // Use class-defined key constants in subclasses, e.g. Get<string>(f_email).
    protected T Get<T>(string key) => (T)this[key];

    protected T GetOptional<T>(string key)
    {
        if (!_optional.ContainsKey(key))
            throw new InvalidOperationException($"not_optional_key:{key}");
        if (!_data.ContainsKey(key))
        {
            // Optional absent is not equivalent to a fabricated 0/false.
            if (typeof(T).IsValueType && Nullable.GetUnderlyingType(typeof(T)) == null)
                throw new InvalidOperationException($"optional_key_absent:{key}");
            return default;
        }
        return (T)this[key];
    }

    public object this[string key]
    {
        get
        {
            if (!_data.TryGetValue(key, out var value))
                throw new KeyNotFoundException($"missing_key:{key}");
            return DoGetFiltered(key, value);
        }
        set => SetValidated(key, value);
    }

    public void Set(string key, object value) => SetValidated(key, value);

    private void SetValidated(string key, object value)
    {
        var hasType = TryFieldType(key, out var type);
        if (!hasType && TryWildcard(out var wildcard))
        {
            type = wildcard;
            hasType = true;
        }

        if (hasType)
            value = ValidateField(key, value, type);
        var result = DoEveryValidation(key, value);
        var normalized = result.Value;
        if (hasType && normalized == null && _defaults.TryGetValue(key, out var declaredDefault) && declaredDefault == null)
            normalized = declaredDefault;
        ThrowIfMessage(result.Message);
        if (hasType)
            CheckFinalType(key, normalized, type);
        _data[key] = normalized;
    }

    private object ValidateField(string key, object value, Type expected)
    {
        if (_converters.TryGetValue(key, out var converter))
            value = converter(value);
        // Only explicitly declared null defaults, object, and Nullable<T>
        // may accept null. Validation hooks still run for those values.
        // Nullish-default compatibility in C#: explicit null assignment is type-compatible
        // when the field explicitly declares a null default.
        _defaults.TryGetValue(key, out var defaultValue);
        var declaredNullDefault = value == null
            && defaultValue == null
            && _defaults.ContainsKey(key);
        object converted;
        try
        {
            if (declaredNullDefault)
                converted = defaultValue;
            else
                converted = DJson.ConvertBaseDataValue(expected, value);
        }
        catch (InvalidCastException ex)
        {
            throw new InvalidCastException($"invalid_type:{key}", ex);
        }
        var result = DoValidation(key, converted);
        ThrowIfMessage(result.Message);
        CheckFinalType(key, result.Value, expected);
        if (result.Value == null && _defaults.TryGetValue(key, out var declaredDefaultAfterValidation) && declaredDefaultAfterValidation == null)
            return declaredDefaultAfterValidation;
        return result.Value;
    }

    private void CheckFinalType(string key, object value, Type expected)
    {
        // Preserve explicit null when contract default is explicitly null.
        if (value == null && _defaults.TryGetValue(key, out var defaultValue) && defaultValue == null)
            return;
        try
        {
            if (!DJson.MatchesBaseDataType(expected, value))
                throw new InvalidCastException("invalid_type");
        }
        catch (InvalidCastException ex)
        {
            throw new InvalidCastException($"invalid_type:{key}", ex);
        }
    }

    private static void ThrowIfMessage(string message)
    {
        if (!string.IsNullOrEmpty(message))
            throw new ArgumentException(message);
    }

    private bool TryFieldType(string key, out Type type)
    {
        if (_required.TryGetValue(key, out type)) return true;
        if (_optional.TryGetValue(key, out type)) return true;
        type = null;
        return false;
    }

    private bool TryWildcard(out Type type)
    {
        if (_required.TryGetValue("*", out type)) return true;
        if (_optional.TryGetValue("*", out type)) return true;
        type = null;
        return false;
    }

    /// <summary>Recursive <<NAME>> expansion over dictionaries and lists.
    /// Existing BaseData instances remain intact, as in Python.</summary>
    public object DoEnvMapping(object input)
    {
        if (input is BaseData) return input;
        if (input is IDictionary<string, object> dictionary)
        {
            var result = new Dictionary<string, object>();
            foreach (var pair in dictionary)
                result.Add(pair.Key, DoEnvMapping(pair.Value));
            return result;
        }
        if (input is IDictionary otherDictionary)
        {
            var result = new Dictionary<string, object>();
            foreach (DictionaryEntry pair in otherDictionary)
            {
                if (pair.Key is not string key)
                    throw new InvalidCastException("invalid_type");
                result.Add(key, DoEnvMapping(pair.Value));
            }
            return result;
        }
        if (input is IEnumerable items && input is not string)
        {
            var result = new List<object>();
            foreach (var item in items)
                result.Add(DoEnvMapping(item));
            return result;
        }
        if (input is string text)
        {
            return EnvPattern.Replace(text, match =>
            {
                var name = match.Groups[1].Value;
                var value = Environment.GetEnvironmentVariable(name);
                if (value == null)
                    throw new InvalidOperationException(
                        $"Missing required environment variable: '{name}' for '{GetType().Name}'");
                return value;
            });
        }
        return input;
    }

    private static Dictionary<string, Dictionary<string, object>> CloneStore(
        Dictionary<string, Dictionary<string, object>> source)
    {
        var clone = new Dictionary<string, Dictionary<string, object>>();
        foreach (var pair in source)
            clone[pair.Key] = new Dictionary<string, object>(pair.Value);
        return clone;
    }

    public static bool DiagnosticPrint(
        string title,
        string index,
        object start_id,
        object end_id,
        string description_of_location,
        object interesting_conditionals,
        double? repeat_delay)
    {
        if (repeat_delay.HasValue && repeat_delay.Value < 0 && repeat_delay.Value != -1)
            throw new ArgumentOutOfRangeException(nameof(repeat_delay), "invalid_repeat_delay");

        var key = index ?? string.Empty;
        var nowTs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;

        lock (DiagnosticLock)
        {
            Diagnostics.TryGetValue(key, out var existing);
            var allowed = false;
            if (!repeat_delay.HasValue || repeat_delay.Value == 0)
                allowed = true;
            else if (repeat_delay.Value == -1)
                allowed = existing == null;
            else if (existing == null)
                allowed = true;
            else
                allowed = (nowTs - Convert.ToDouble(existing["timestamp"])) >= repeat_delay.Value;

            if (!allowed)
                return false;

            var record = new Dictionary<string, object>
            {
                ["title"] = title,
                ["index"] = index,
                ["start_id"] = start_id,
                ["end_id"] = end_id,
                ["description_of_location"] = description_of_location,
                ["interesting_conditionals"] = interesting_conditionals,
                ["timestamp"] = nowTs,
            };

            Console.WriteLine(JsonSerializer.Serialize(record));
            Diagnostics[key] = record;
            if (start_id != null)
                DiagnosticStarts[key] = new Dictionary<string, object>(record);
            if (end_id != null)
                DiagnosticEnds[key] = new Dictionary<string, object>(record);
            return true;
        }
    }

    public static Dictionary<string, object> DiagnosticDump()
    {
        lock (DiagnosticLock)
        {
            return new Dictionary<string, object>
            {
                ["diagnostics"] = CloneStore(Diagnostics),
                ["starts"] = CloneStore(DiagnosticStarts),
                ["ends"] = CloneStore(DiagnosticEnds),
            };
        }
    }

    // IDictionary: the instance IS the payload, not a projected copy.
    public ICollection<string> Keys => _data.Keys;
    public ICollection<object> Values => _data.Values;
    public int Count => _data.Count;
    public bool IsReadOnly => false;
    public void Add(string key, object value)
    {
        if (_data.ContainsKey(key))
            throw new ArgumentException($"duplicate_key:{key}");
        SetValidated(key, value);
    }
    public bool ContainsKey(string key) => _data.ContainsKey(key);
    public bool Remove(string key)
    {
        if (_required.ContainsKey(key) && key != "*")
            throw new InvalidOperationException($"cannot_remove_required_key:{key}");
        return _data.Remove(key);
    }
    public bool TryGetValue(string key, out object value)
    {
        if (_data.ContainsKey(key))
        {
            value = this[key];
            return true;
        }
        value = null;
        return false;
    }
    public void Add(KeyValuePair<string, object> item) => Add(item.Key, item.Value);
    public void Clear()
    {
        if (_required.Keys.Any(key => key != "*"))
            throw new InvalidOperationException("cannot_clear_required_keys");
        _data.Clear();
    }
    public bool Contains(KeyValuePair<string, object> item) =>
        ((ICollection<KeyValuePair<string, object>>)_data).Contains(item);
    public void CopyTo(KeyValuePair<string, object>[] array, int arrayIndex) =>
        ((ICollection<KeyValuePair<string, object>>)_data).CopyTo(array, arrayIndex);
    public bool Remove(KeyValuePair<string, object> item)
    {
        if (!Contains(item)) return false;
        return Remove(item.Key);
    }
    public IEnumerator<KeyValuePair<string, object>> GetEnumerator() => _data.GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
