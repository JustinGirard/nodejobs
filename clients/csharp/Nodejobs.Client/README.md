# Nodejobs.Client (C#)

This C# client includes `DJson`, a reusable JSON/object-tree utility replicated from the UI project so JSON behavior stays consistent across projects.

## DJson Location

- `clients/csharp/Nodejobs.Client/DJson.cs`

## What DJson Is Good For

- JSON stringify for request payloads and debug/error dumps.
- JSON parse into `Dictionary<string, object>` when top-level JSON is an object.
- Generic parse when payload may be object or list.
- Nested key read/write helpers for dictionary trees.
- Lightweight tree merge helpers (`Append` / `Replace`).

## Core Methods

- `DJson.Stringify(object obj, int indentLevel = 0, bool debug = false)`
  - Serializes dictionaries/lists/primitives for transport or logs.

- `DJson.Parse(string json)`
  - Parses top-level JSON object into `Dictionary<string, object>`.

- `DJson.ParseGeneric(string json)`
  - Parses top-level JSON object/list into `object`.

- `DJson.TryGetKeyAt(IDictionary<string, object> root, IList<string> keys, object null_substitute = null)`
  - Reads nested values by path.

- `DJson.TrySetKeyAt(IDictionary<string, object> root, IList<string> keys, object value, bool overwrite = true)`
  - Writes nested values by path.

- `DJson.MergeGeneric(object baseTree, object overrideTree, DJson.MergeMode mode)`
  - Merges object trees (`Append` or `Replace` semantics).

- `DJson.ValidateJsonSchema(Dictionary<string, object> json, Dictionary<string, object> schema)`
  - Minimal schema validation helper.

## Usage Examples

### 1) Build HTTP JSON body

```csharp
var json = DJson.Stringify(validatedData);
var content = new StringContent(json, Encoding.UTF8, "application/json");
```

### 2) Parse MCP envelope

```csharp
var env = DJson.Parse(mcpStdout);
var data = DJson.TryGetKeyAt(env, new List<string> { "data" }, null);
if (data == null) throw new Exception("mcp_response_missing_data");
```

### 3) Structured error dump

```csharp
var err = new Dictionary<string, object>
{
    ["error"] = "invoke_failed",
    ["stdout"] = stdout,
    ["stderr"] = stderr
};
throw new Exception(DJson.Stringify(err));
```

### 4) Nested assignment

```csharp
var root = new Dictionary<string, object>();
if (!DJson.TrySetKeyAt(root, new List<string> { "transport", "mcp", "tool" }, "now", overwrite: true))
    throw new Exception("set_nested_key_failed");
```

## Notes

- `DJson.Parse(...)` expects a top-level JSON object.
- `DJson.ParseGeneric(...)` supports object/list trees and returns `object`.
- Keep exception behavior explicit; do not silently fill missing required data.

## Recommended Adoption in ClientService

Use `DJson` first for:

- request serialization (`command_json`, HTTP body, MCP `data-json`)
- response parsing (`mcp.StdOut`, HTTP response body)
- error payload dumps (`stderr`/`stdout`/context)

This keeps C# client JSON handling consistent with the UI-side utilities.
