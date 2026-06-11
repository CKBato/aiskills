---
name: microsoft-code-reference
description: Find working code samples, verify API signatures, and fix Microsoft SDK errors using official docs. Use whenever the user is writing, debugging, or reviewing code that touches any Microsoft SDK, .NET library, Azure client library, or Microsoft API—even if they don't ask for a "reference."
---

# Microsoft Code Reference

## Tools

| Need | Tool | Example |
|------|------|---------|
| API method/class lookup | `microsoft_docs_search` | `"BlobClient UploadAsync Azure.Storage.Blobs"` |
| Working code sample | `microsoft_code_sample_search` | `query: "upload blob managed identity", language: "python"` |
| Full API reference | `microsoft_docs_fetch` | Fetch URL from search results |

## Validation Workflow

Before generating code using Microsoft SDKs:

1. Confirm method or package exists — `microsoft_docs_search`
2. Fetch full details when needed — `microsoft_docs_fetch`
3. Find working sample — `microsoft_code_sample_search`

## Error Troubleshooting

| Error Type | Query |
|------------|-------|
| Method not found | `"[ClassName] methods [Namespace]"` |
| Type not found | `"[TypeName] NuGet package namespace"` |
| Wrong signature | `"[ClassName] [MethodName] overloads"` |
| Deprecated warning | `"[OldType] migration v12"` |
| Auth failure | `"DefaultAzureCredential troubleshooting"` |
