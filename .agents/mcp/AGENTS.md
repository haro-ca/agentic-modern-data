# MCP Configurations

Each subdirectory contains the MCP server config for a specific agent tool. They all connect
to the same Databricks Genie endpoint but use different file formats and key names, which is
why we need one per tool.

## Why multiple files?

There is no shared MCP config standard (unlike `.agents/skills/` which all tools recognize).
Each tool has its own format and destination path:

| Tool | Config file | Copy to |
|---|---|---|
| **VS Code / Copilot** | `vscode/mcp.json` | `~/Library/Application Support/Code/User/mcp.json` (global) or `.vscode/mcp.json` (workspace) |
| **Copilot CLI** | `copilot-cli/mcp-config.json` | `~/.copilot/mcp-config.json` |
| **opencode** | `opencode/opencode.json` | merge `"mcp"` key into `opencode.json` at project or `~/.config/opencode/opencode.json` globally |
| **Windsurf Cascade** | `windsurf/mcp_config.json` | `~/.codeium/windsurf/mcp_config.json` |

## Setup

1. Replace `<WORKSPACE-HOSTNAME>` with your Databricks workspace hostname (e.g. `dbc-xxxx.cloud.databricks.com`)
2. Replace `<PAT>` with a Databricks Personal Access Token (Settings -> Developer -> Access Tokens)
3. Copy the file to the destination path for each tool you use

## Adding servers to existing configs

Do NOT use `cp` blindly - it will overwrite any other MCP servers you have configured.
Instead, copy only the server entry from the template and merge it into your existing
config file manually.

**VS Code** - `"$HOME/Library/Application Support/Code/User/mcp.json"`
Add the entry inside the existing `"servers": { ... }` object.

**Copilot CLI** - `~/.copilot/mcp-config.json`
Add the entry inside the existing `"mcpServers": { ... }` object.

**Windsurf Cascade** - `~/.codeium/windsurf/mcp_config.json`
Add the entry inside the existing `"mcpServers": { ... }` object.

**opencode** - `~/.config/opencode/opencode.json` (global) or `opencode.json` (project)
Add the entry inside the existing `"mcp": { ... }` object.

Only use `cp` if the destination file does not exist yet:

```bash
cp vscode/mcp.json "$HOME/Library/Application Support/Code/User/mcp.json"
cp copilot-cli/mcp-config.json ~/.copilot/mcp-config.json
cp windsurf/mcp_config.json ~/.codeium/windsurf/mcp_config.json
```
