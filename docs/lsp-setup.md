# LSP Setup for Claude Code

DataHub uses three language servers to give Claude Code accurate code navigation
(`goToDefinition`, `findReferences`, `workspaceSymbol`) across the Java, Python, and TypeScript
layers.

## Java — jdtls (Eclipse JDT Language Server)

Enabled per-project in `.claude/settings.json`:

```json
{
  "enabledPlugins": {
    "jdtls-lsp@claude-plugins-official": true
  }
}
```

No additional installation needed — the plugin is bundled with Claude Code.

## Python — Pyright

Enabled globally in `~/.claude/settings.json`:

```json
{
  "enabledPlugins": {
    "pyright-lsp@claude-plugins-official": true
  }
}
```

Pyright requires `python` to be on your `PATH`. The repo's `mise.toml` provides Python 3.11:

```bash
mise install   # installs python 3.11 and other tools declared in mise.toml
```

## TypeScript — typescript-language-server

Not yet configured. To enable it, add to `~/.claude/settings.json`:

```json
{
  "enabledPlugins": {
    "typescript-language-server-lsp@claude-plugins-official": true
  }
}
```

Requires Node.js (provided by `mise.toml`).

## Verifying

Once enabled, LSP tools are available in Claude Code tool calls:

- `goToDefinition` — jump to symbol definition
- `findReferences` — find all usages
- `workspaceSymbol` — search symbols by name
- `hover` — type info and docs
