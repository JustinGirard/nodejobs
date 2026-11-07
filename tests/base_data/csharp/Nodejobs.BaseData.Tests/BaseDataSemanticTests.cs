using System;
using System.Collections.Generic;
using System.Threading;
using Nodejobs.Client;
using Xunit;

namespace Nodejobs.Client.Tests;

/// <summary>
/// C# semantic parity suite aligned to canonical Python BaseData tests.
/// Focus: runtime behavior + compact developer contract surface.
/// </summary>
public class BaseDataSemanticTests
{
    [Fact]
    public void RequiredFieldsAndTypeValidation()
    {
        var item = new RequiredSpec(new Dictionary<string, object>
        {
            ["name"] = "alpha",
            ["count"] = 2,
        });

        Assert.Equal("alpha", item["name"]);
        Assert.Equal(2, item["count"]);

        var missing = Assert.ThrowsAny<Exception>(() => new RequiredSpec(
            new Dictionary<string, object> { ["name"] = "alpha" }));
        Assert.Contains("missing_required_key:count", missing.Message);

        var wrongType = Assert.ThrowsAny<Exception>(() => new RequiredSpec(
            new Dictionary<string, object>
            {
                ["name"] = "alpha",
                ["count"] = "2",
            }));
        Assert.Contains("invalid_type:count", wrongType.Message);
    }

    public sealed class RequiredSpec : BaseData
    {
        public RequiredSpec(IDictionary<string, object> input) : base(input) { }

        protected override Dictionary<string, FieldSpec> Fields() => new()
        {
            ["name"] = Required<string>(),
            ["count"] = Required<int>(),
        };
    }

    [Fact]
    public void DoPreProcessAndDoValidation()
    {
        var cleaned = new NormalizedTag(new Dictionary<string, object>
        {
            ["tag"] = "  hello  ",
        });
        Assert.Equal("hello", cleaned["tag"]);

        var invalid = Assert.ThrowsAny<ArgumentException>(() => new NormalizedTag(
            new Dictionary<string, object> { ["tag"] = "   " }));
        Assert.Contains("tag_required", invalid.Message);
    }

    public sealed class NormalizedTag : BaseData
    {
        public NormalizedTag(IDictionary<string, object> input) : base(input) { }

        protected override Dictionary<string, FieldSpec> Fields() => new()
        {
            ["tag"] = Required<string>(),
        };

        protected override Dictionary<string, object> DoPreProcess(Dictionary<string, object> input)
        {
            var output = new Dictionary<string, object>(input);
            if (output.ContainsKey("tag") && output["tag"] is string text)
                output["tag"] = text.Trim();
            return output;
        }

        protected override (object Value, string Message) DoValidation(string key, object value)
        {
            if (key == "tag" && ((string)value).Length == 0)
                throw new ArgumentException("tag_required");
            return (value, "");
        }
    }

    [Fact]
    public void PreservesUndeclaredInputKeys()
    {
        var payload = new Dictionary<string, object>
        {
            ["title"] = "doc",
            ["extra_flag"] = true,
            ["metadata"] = new Dictionary<string, object> { ["author"] = "test" },
        };

        var item = new DeclaredOnly(payload);
        Assert.Equal("doc", item["title"]);
        Assert.Equal(true, item["extra_flag"]);
        Assert.Equal("test", ((IDictionary<string, object>)item["metadata"])["author"]);
    }

    public sealed class DeclaredOnly : BaseData
    {
        public DeclaredOnly(IDictionary<string, object> input) : base(input) { }

        protected override Dictionary<string, FieldSpec> Fields() => new()
        {
            ["title"] = Required<string>(),
        };
    }

    [Fact]
    public void CombinatorialContractsShareFullInput()
    {
        var payload = new Dictionary<string, object>
        {
            ["a"] = "one",
            ["b"] = "two",
            ["c"] = "three",
            ["undeclared"] = "kept",
        };

        var data1 = new Data1(payload);
        var data2 = new Data2(payload);

        Assert.Equal("one", data1["a"]);
        Assert.Equal("two", data1["b"]);
        Assert.Equal("two", data2["b"]);
        Assert.Equal("three", data2["c"]);

        Assert.Equal("three", data1["c"]);
        Assert.Equal("one", data2["a"]);
        Assert.Equal("kept", data1["undeclared"]);
        Assert.Equal("kept", data2["undeclared"]);
    }

    public sealed class Data1 : BaseData
    {
        public Data1(IDictionary<string, object> input) : base(input) { }

        protected override Dictionary<string, FieldSpec> Fields() => new()
        {
            ["a"] = Required<string>(),
            ["b"] = Required<string>(),
        };
    }

    public sealed class Data2 : BaseData
    {
        public Data2(IDictionary<string, object> input) : base(input) { }

        protected override Dictionary<string, FieldSpec> Fields() => new()
        {
            ["b"] = Required<string>(),
            ["c"] = Required<string>(),
        };
    }

    [Fact]
    public void ContractDefinedDefaults()
    {
        var defaults = new DefaultsSpec(new Dictionary<string, object>
        {
            ["name"] = "job",
        });

        Assert.Equal(1.5, defaults["timeout_s"]);
        Assert.Equal(3, defaults["retries"]);

        var overridden = new DefaultsSpec(new Dictionary<string, object>
        {
            ["name"] = "job",
            ["retries"] = 9,
        });

        Assert.Equal(1.5, overridden["timeout_s"]);
        Assert.Equal(9, overridden["retries"]);
    }

    public sealed class DefaultsSpec : BaseData
    {
        public DefaultsSpec(IDictionary<string, object> input) : base(input) { }

        protected override Dictionary<string, FieldSpec> Fields() => new()
        {
            ["name"] = Required<string>(),
            ["timeout_s"] = Defaulted<double>(1.5),
            ["retries"] = Defaulted<int>(3),
        };
    }

    [Fact]
    public void StatefulDictionaryMutation()
    {
        var item = new MutableSpec(new Dictionary<string, object>
        {
            ["label"] = "seed",
            ["note"] = "keep",
        });

        item["qty"] = 5;
        item["new_blob"] = new Dictionary<string, object>
        {
            ["steps"] = new List<object> { 1, 2, 3 },
        };

        Assert.Equal("seed", item["label"]);
        Assert.Equal(5, item["qty"]);
        Assert.Equal("keep", item["note"]);
        var steps = Assert.IsAssignableFrom<IList<object>>(
            ((IDictionary<string, object>)item["new_blob"])["steps"]);
        Assert.Equal(new List<object> { 1, 2, 3 }, steps);

        var wrongType = Assert.ThrowsAny<Exception>(() => item["qty"] = "bad");
        Assert.Contains("invalid_type:qty", wrongType.Message);
    }

    public sealed class MutableSpec : BaseData
    {
        public MutableSpec(IDictionary<string, object> input) : base(input) { }

        protected override Dictionary<string, FieldSpec> Fields() => new()
        {
            ["label"] = Required<string>(),
            ["qty"] = Defaulted<int>(0),
        };
    }

    [Fact]
    public void RecursiveNestedBaseDataValidationAndErrorPropagation()
    {
        var parent = new Parent(new Dictionary<string, object>
        {
            ["child"] = new Dictionary<string, object> { ["value"] = 1 },
            ["children"] = new List<object>
            {
                new Dictionary<string, object> { ["value"] = 2 },
                new Dictionary<string, object> { ["value"] = 3 },
            },
            ["extra"] = "preserved",
        });

        var child = Assert.IsType<Child>(parent["child"]);
        var children = Assert.IsAssignableFrom<IList<Child>>(parent["children"]);
        Assert.IsType<Child>(children[0]);
        Assert.Equal(1, child["value"]);
        Assert.Equal(3, children[1]["value"]);
        Assert.Equal("preserved", parent["extra"]);

        var invalid = Assert.ThrowsAny<ArgumentException>(() => new Parent(new Dictionary<string, object>
        {
            ["child"] = new Dictionary<string, object> { ["value"] = -1 },
            ["children"] = new List<object>(),
        }));
        Assert.Contains("value_must_be_non_negative", invalid.Message);
    }

    public sealed class Child : BaseData
    {
        public Child(IDictionary<string, object> input) : base(input) { }

        protected override Dictionary<string, FieldSpec> Fields() => new()
        {
            ["value"] = Required<int>(),
        };

        protected override (object Value, string Message) DoValidation(string key, object value)
        {
            if (key == "value" && (int)value < 0)
                throw new ArgumentException("value_must_be_non_negative");
            return (value, "");
        }
    }

    public sealed class Parent : BaseData
    {
        public Parent(IDictionary<string, object> input) : base(input) { }

        protected override Dictionary<string, FieldSpec> Fields() => new()
        {
            ["child"] = Required<Child>(),
            ["children"] = Required<List<Child>>(),
        };
    }

    [Fact]
    public void OptionalFieldsShorthandAndOverrideForms()
    {
        ShortOptionalSpec.ValidationCalls = 0;
        LongOptionalSpec.ValidationCalls = 0;

        var shortMissing = new ShortOptionalSpec(new Dictionary<string, object>());
        Assert.Null(shortMissing.tag);
        Assert.False(shortMissing.ContainsKey("tag"));
        Assert.Equal(0, ShortOptionalSpec.ValidationCalls);

        var shortPresent = new ShortOptionalSpec(new Dictionary<string, object> { ["tag"] = "ok" });
        Assert.Equal("ok", shortPresent["tag"]);
        Assert.Equal(1, ShortOptionalSpec.ValidationCalls);

        var shortInvalid = Assert.ThrowsAny<ArgumentException>(() => new ShortOptionalSpec(
            new Dictionary<string, object> { ["tag"] = "bad" }));
        Assert.Contains("tag_invalid", shortInvalid.Message);

        var longMissing = new LongOptionalSpec(new Dictionary<string, object>());
        Assert.False(longMissing.ContainsKey("tag"));
        Assert.Equal(0, LongOptionalSpec.ValidationCalls);

        var longPresent = new LongOptionalSpec(new Dictionary<string, object> { ["tag"] = "ok" });
        Assert.Equal("ok", longPresent["tag"]);
        Assert.Equal(1, LongOptionalSpec.ValidationCalls);

        var longInvalid = Assert.ThrowsAny<ArgumentException>(() => new LongOptionalSpec(
            new Dictionary<string, object> { ["tag"] = "bad" }));
        Assert.Contains("tag_invalid", longInvalid.Message);
    }

    [Fact]
    public void RequiredFieldsShorthandAndOverrideForms()
    {
        ShortRequiredSpec.ValidationCalls = 0;
        LongRequiredSpec.ValidationCalls = 0;

        var shortOk = new ShortRequiredSpec(new Dictionary<string, object> { ["name"] = "alpha" });
        Assert.Equal("alpha", shortOk["name"]);
        Assert.Equal(1, ShortRequiredSpec.ValidationCalls);

        var shortMissing = Assert.ThrowsAny<Exception>(() => new ShortRequiredSpec(new Dictionary<string, object>()));
        Assert.Contains("missing_required_key:name", shortMissing.Message);

        var shortInvalid = Assert.ThrowsAny<ArgumentException>(() => new ShortRequiredSpec(
            new Dictionary<string, object> { ["name"] = "" }));
        Assert.Contains("name_required", shortInvalid.Message);

        var longOk = new LongRequiredSpec(new Dictionary<string, object> { ["name"] = "alpha" });
        Assert.Equal("alpha", longOk["name"]);
        Assert.Equal(1, LongRequiredSpec.ValidationCalls);

        var longMissing = Assert.ThrowsAny<Exception>(() => new LongRequiredSpec(new Dictionary<string, object>()));
        Assert.Contains("missing_required_key:name", longMissing.Message);

        var longInvalid = Assert.ThrowsAny<ArgumentException>(() => new LongRequiredSpec(
            new Dictionary<string, object> { ["name"] = "" }));
        Assert.Contains("name_required", longInvalid.Message);
    }

    [Fact]
    public void DefaultsShorthandAndOverrideForms()
    {
        ShortDefaultsSpec.ValidationCalls = 0;
        LongDefaultsSpec.ValidationCalls = 0;

        var shortDefaulted = new ShortDefaultsSpec(new Dictionary<string, object>());
        Assert.Equal(3, shortDefaulted["retries"]);
        Assert.Equal(0, ShortDefaultsSpec.ValidationCalls);

        var shortOverridden = new ShortDefaultsSpec(new Dictionary<string, object> { ["retries"] = 7 });
        Assert.Equal(7, shortOverridden["retries"]);
        Assert.Equal(1, ShortDefaultsSpec.ValidationCalls);

        var shortInvalid = Assert.ThrowsAny<ArgumentException>(() => new ShortDefaultsSpec(
            new Dictionary<string, object> { ["retries"] = 0 }));
        Assert.Contains("retries_must_be_positive", shortInvalid.Message);

        var longDefaulted = new LongDefaultsSpec(new Dictionary<string, object>());
        Assert.Equal(3, longDefaulted["retries"]);
        Assert.Equal(0, LongDefaultsSpec.ValidationCalls);

        var longOverridden = new LongDefaultsSpec(new Dictionary<string, object> { ["retries"] = 7 });
        Assert.Equal(7, longOverridden["retries"]);
        Assert.Equal(1, LongDefaultsSpec.ValidationCalls);

        var longInvalid = Assert.ThrowsAny<ArgumentException>(() => new LongDefaultsSpec(
            new Dictionary<string, object> { ["retries"] = 0 }));
        Assert.Contains("retries_must_be_positive", longInvalid.Message);
    }

    [Fact]
    public void NullDefaultAcceptsExplicitNullAssignment()
    {
        var explicitNull = new NullDefaultSpec(new Dictionary<string, object>
        {
            ["note"] = null,
        });
        Assert.Null(explicitNull.note);
        Assert.True(explicitNull.ContainsKey("note"));
        Assert.Null(explicitNull["note"]);

        var missing = new NullDefaultSpec(new Dictionary<string, object>());
        Assert.Null(missing.note);
        Assert.False(missing.ContainsKey("note"));

        missing.note = null;
        Assert.Null(missing.note);
        Assert.True(missing.ContainsKey("note"));
        Assert.Null(missing["note"]);

        Assert.True(missing.Remove("note"));
        Assert.False(missing.ContainsKey("note"));
        Assert.Null(missing.note);
    }

    [Fact]
    public void StaticDiagnosticPrintAndDumpContract()
    {
        var unique = Guid.NewGuid().ToString("N");

        var idx = $"diag_cs_delay_{unique}";
        Assert.True(BaseData.DiagnosticPrint("t", idx, null, null, "loc", new Dictionary<string, object> { ["a"] = 1 }, 0.01));
        var dumpA = BaseData.DiagnosticDump();
        var diagnosticsA = Assert.IsAssignableFrom<IDictionary<string, Dictionary<string, object>>>(dumpA["diagnostics"]);
        var tsA = Convert.ToDouble(diagnosticsA[idx]["timestamp"]);

        Assert.False(BaseData.DiagnosticPrint("t", idx, null, null, "loc", new Dictionary<string, object> { ["a"] = 2 }, 0.01));
        var dumpB = BaseData.DiagnosticDump();
        var diagnosticsB = Assert.IsAssignableFrom<IDictionary<string, Dictionary<string, object>>>(dumpB["diagnostics"]);
        var tsB = Convert.ToDouble(diagnosticsB[idx]["timestamp"]);
        Assert.Equal(tsA, tsB);

        Thread.Sleep(25);
        Assert.True(BaseData.DiagnosticPrint("t", idx, null, null, "loc", new Dictionary<string, object> { ["a"] = 3 }, 0.01));

        var onceIdx = $"diag_cs_once_{unique}";
        Assert.True(BaseData.DiagnosticPrint("t", onceIdx, null, null, "loc", new Dictionary<string, object>(), -1));
        Assert.False(BaseData.DiagnosticPrint("t", onceIdx, null, null, "loc", new Dictionary<string, object>(), -1));

        var freeIdx = $"diag_cs_free_{unique}";
        Assert.True(BaseData.DiagnosticPrint("t", freeIdx, null, null, "loc", new Dictionary<string, object>(), null));
        Assert.True(BaseData.DiagnosticPrint("t", freeIdx, null, null, "loc", new Dictionary<string, object>(), 0));

        var startIdx = $"diag_cs_start_{unique}";
        var endIdx = $"diag_cs_end_{unique}";
        Assert.True(BaseData.DiagnosticPrint("t", startIdx, "s1", null, "loc", new Dictionary<string, object>(), 0));
        Assert.True(BaseData.DiagnosticPrint("t", endIdx, null, "e1", "loc", new Dictionary<string, object>(), 0));
        var dumpC = BaseData.DiagnosticDump();
        var starts = Assert.IsAssignableFrom<IDictionary<string, Dictionary<string, object>>>(dumpC["starts"]);
        var ends = Assert.IsAssignableFrom<IDictionary<string, Dictionary<string, object>>>(dumpC["ends"]);
        Assert.True(starts.ContainsKey(startIdx));
        Assert.True(ends.ContainsKey(endIdx));

        var beforeCount = Assert.IsAssignableFrom<IDictionary<string, Dictionary<string, object>>>(BaseData.DiagnosticDump()["diagnostics"]).Count;
        var boundedIdx = $"diag_cs_bounded_{unique}";
        Assert.True(BaseData.DiagnosticPrint("t", boundedIdx, null, null, "loc", new Dictionary<string, object>(), 0));
        var midCount = Assert.IsAssignableFrom<IDictionary<string, Dictionary<string, object>>>(BaseData.DiagnosticDump()["diagnostics"]).Count;
        Assert.True(BaseData.DiagnosticPrint("t", boundedIdx, null, null, "loc", new Dictionary<string, object>(), 0));
        var afterCount = Assert.IsAssignableFrom<IDictionary<string, Dictionary<string, object>>>(BaseData.DiagnosticDump()["diagnostics"]).Count;
        Assert.Equal(beforeCount + 1, midCount);
        Assert.Equal(midCount, afterCount);

        var invalid = Assert.Throws<ArgumentOutOfRangeException>(() =>
            BaseData.DiagnosticPrint("t", "bad_delay", null, null, "loc", new Dictionary<string, object>(), -2));
        Assert.Contains("invalid_repeat_delay", invalid.Message);
    }

    public sealed class ShortOptionalSpec : BaseData
    {
        public static int ValidationCalls;
        public ShortOptionalSpec(IDictionary<string, object> input) : base(input) { }

        protected override Dictionary<string, FieldSpec> Fields() => new()
        {
            ["tag"] = Defaulted<string>(null),
        };

        protected override (object Value, string Message) DoValidation(string key, object value)
        {
            if (key == "tag")
            {
                ValidationCalls += 1;
                if ((string)value == "bad")
                    throw new ArgumentException("tag_invalid");
            }
            return (value, "");
        }

        public object tag
        {
            get => GetOptional<object>(nameof(tag));
            set => this[nameof(tag)] = value;
        }
    }

    public sealed class LongOptionalSpec : BaseData
    {
        public static int ValidationCalls;
        public LongOptionalSpec(IDictionary<string, object> input) : base(input) { }

        protected override Dictionary<string, Type> OptionalKeys() => new()
        {
            ["tag"] = typeof(string),
        };

        protected override (object Value, string Message) DoValidation(string key, object value)
        {
            if (key == "tag")
            {
                ValidationCalls += 1;
                if ((string)value == "bad")
                    throw new ArgumentException("tag_invalid");
            }
            return (value, "");
        }
    }

    public sealed class ShortRequiredSpec : BaseData
    {
        public static int ValidationCalls;
        public ShortRequiredSpec(IDictionary<string, object> input) : base(input) { }

        protected override Dictionary<string, FieldSpec> Fields() => new()
        {
            ["name"] = Required<string>(),
        };

        protected override (object Value, string Message) DoValidation(string key, object value)
        {
            if (key == "name")
            {
                ValidationCalls += 1;
                if (((string)value).Length == 0)
                    throw new ArgumentException("name_required");
            }
            return (value, "");
        }
    }

    public sealed class LongRequiredSpec : BaseData
    {
        public static int ValidationCalls;
        public LongRequiredSpec(IDictionary<string, object> input) : base(input) { }

        protected override Dictionary<string, Type> RequiredKeys() => new()
        {
            ["name"] = typeof(string),
        };

        protected override (object Value, string Message) DoValidation(string key, object value)
        {
            if (key == "name")
            {
                ValidationCalls += 1;
                if (((string)value).Length == 0)
                    throw new ArgumentException("name_required");
            }
            return (value, "");
        }
    }

    public sealed class ShortDefaultsSpec : BaseData
    {
        public static int ValidationCalls;
        public ShortDefaultsSpec(IDictionary<string, object> input) : base(input) { }

        protected override Dictionary<string, FieldSpec> Fields() => new()
        {
            ["retries"] = Defaulted<int>(3),
        };

        protected override (object Value, string Message) DoValidation(string key, object value)
        {
            if (key == "retries")
            {
                ValidationCalls += 1;
                if ((int)value <= 0)
                    throw new ArgumentException("retries_must_be_positive");
            }
            return (value, "");
        }
    }

    public sealed class LongDefaultsSpec : BaseData
    {
        public static int ValidationCalls;
        public LongDefaultsSpec(IDictionary<string, object> input) : base(input) { }

        protected override Dictionary<string, Type> OptionalKeys() => new()
        {
            ["retries"] = typeof(int),
        };

        protected override Dictionary<string, object> Defaults() => new()
        {
            ["retries"] = 3,
        };

        protected override (object Value, string Message) DoValidation(string key, object value)
        {
            if (key == "retries")
            {
                ValidationCalls += 1;
                if ((int)value <= 0)
                    throw new ArgumentException("retries_must_be_positive");
            }
            return (value, "");
        }
    }

    public sealed class NullDefaultSpec : BaseData
    {
        public NullDefaultSpec(IDictionary<string, object> input) : base(input) { }

        protected override Dictionary<string, FieldSpec> Fields() => new()
        {
            ["note"] = Defaulted<string>(null),
        };

        public object note
        {
            get => GetOptional<object>(nameof(note));
            set => this[nameof(note)] = value;
        }
    }
}
