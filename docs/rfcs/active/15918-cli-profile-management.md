- Start Date: 2026-01-18
- RFC PR: [#15918](https://github.com/datahub-project/datahub/pull/15918)
- Discussion Issue: (if any)
- Implementation PR(s): (leave this empty)

# DataHub CLI Profile Management

## Summary

Add multi-profile configuration support to the DataHub CLI, enabling users to manage connections to multiple DataHub instances (dev, staging, production) and easily switch between them. This replaces the single-profile `~/.datahubenv` file with a flexible `~/.datahub/config.yaml` system that supports ancestor-walk config resolution — any directory in the ancestor chain can provide a `.datahub/config.yaml` (full config or profile pointer), enabling concurrent agent and git-worktree workflows while maintaining full backward compatibility.

## Basic example

**Setting up multiple profiles:**

```bash
# Add a local development profile
datahub profile add dev \
  --server http://localhost:8080 \
  --token-env DATAHUB_DEV_TOKEN \
  --description "Local development"

# Add a production profile with safety confirmation
datahub profile add prod \
  --server https://datahub.company.com \
  --token-env DATAHUB_PROD_TOKEN \
  --require-confirmation \
  --description "Production - USE WITH CAUTION"

# Pin this repository/worktree to the dev profile
datahub profile use dev

# Set the global default profile (used when no local pointer exists)
datahub profile set-default dev
```

**Using profiles:**

```bash
# Use the current/default profile
datahub get --urn "urn:li:dataset:..."

# Override with specific profile
datahub get --profile prod --urn "urn:li:dataset:..."

# Or via environment variable
export DATAHUB_PROFILE=staging
datahub get --urn "urn:li:dataset:..."
```

**Profile management:**

```bash
# List all profiles
datahub profile list

# Show current profile details
datahub profile current

# Test connection
datahub profile test prod

# Validate configuration file
datahub profile validate
```

**Configuration file (`~/.datahub/config.yaml`):**

```yaml
version: "1.0"
current_profile: dev

profiles:
  dev:
    server: http://localhost:8080
    token: ${DATAHUB_DEV_TOKEN}
    description: "Local development"

  staging:
    server: https://datahub-staging.company.com
    token: ${DATAHUB_STAGING_TOKEN}
    timeout_sec: 60
    description: "Staging environment"

  prod:
    server: https://datahub.company.com
    token: ${DATAHUB_PROD_TOKEN}
    timeout_sec: 120
    require_confirmation: true
    description: "Production - USE WITH CAUTION"
```

**Worktree / agent isolation:**

```bash
# In worktree A: pin to staging
cd ~/worktrees/feature-a
datahub profile use staging
# creates .datahub/config.yaml with "profile: staging"

# In worktree B: pin to prod
cd ~/worktrees/hotfix-b
datahub profile use prod
# creates .datahub/config.yaml with "profile: prod"

# Both agents run concurrently — no conflicts
# CLI walks up from CWD to find nearest .datahub/config.yaml
# Agent A auto-routes to staging, Agent B auto-routes to prod
```

**Local config file (`.datahub/config.yaml` — two forms):**

```yaml
# Form 1: Profile pointer (simple — references a named profile from ~/.datahub/config.yaml)
# Safe to commit — contains no secrets.
profile: staging
```

```yaml
# Form 2: Full config (self-contained — e.g. for CI or custom instances)
version: "1.0"
current_profile: ci
profiles:
  ci:
    server: https://staging.datahub.com
    token: ${DATAHUB_STAGING_TOKEN}   # use env var refs to keep secrets out of files
    description: "CI environment"
```

The CLI walks up from the current working directory looking for the nearest `.datahub/config.yaml`. The first one found wins — `~/.datahub/config.yaml` is the ultimate fallback since `~` is an ancestor of every directory.

## Motivation

### Current Pain Points

1. **Single Environment Limitation**: The current `~/.datahubenv` file only supports one DataHub connection at a time. Users working across multiple environments (local dev, staging, production, customer instances) must manually edit the configuration file or use environment variables each time they switch contexts.

2. **Context Switching Friction**: Common workflows require frequent environment switching:

   - Testing ingestion changes locally before deploying to production
   - Comparing metadata between environments
   - Working with customer DataHub instances for support
   - Managing different DataHub instances for different projects

3. **Credential Management Risks**:

   - Users often store tokens directly in the config file, creating security risks
   - No built-in guidance for secure credential management
   - Difficult to use different credential storage strategies per environment

4. **Production Safety Concerns**:

   - No safety rails to prevent accidentally running destructive operations against production
   - Easy to forget which environment you're connected to
   - No confirmation prompts for dangerous operations

5. **Poor Developer Experience**:
   - Constant manual editing of config files
   - Reliance on shell aliases or wrapper scripts
   - No visibility into which profile is currently active
   - Difficult to share standard configurations across teams

6. **Agent and Worktree Isolation**: When multiple AI agents (Claude Code, Cursor, Codex, Devin) or developers use separate git worktrees to work on the same repository concurrently, they often need to target different DataHub instances (e.g., one agent ingests to staging while another validates in prod). The current single-profile model forces all concurrent processes to share one configuration, leading to race conditions or requiring fragile per-process environment variable management.

### Use Cases This Supports

1. **Multi-Environment Development**: Engineers can maintain separate profiles for dev, staging, and production environments, switching between them with a single command.

2. **Customer Support**: Support engineers can manage profiles for multiple customer instances without losing their primary configuration.

3. **Team Onboarding**: New team members can import standard profile configurations, reducing setup time and configuration errors.

4. **Safe Production Operations**: Production profiles can require explicit confirmation before destructive operations, reducing the risk of accidental data deletion.

5. **Automated Workflows**: CI/CD pipelines can use profile selection to target specific environments without complex environment variable management.

6. **Cross-Project Work**: Developers working on multiple projects with different DataHub instances can maintain isolated profiles per project.

7. **Concurrent Agent Workflows**: AI coding agents operating in separate git worktrees can each target a different DataHub instance without environment variable coordination. Each worktree carries a local pointer file that auto-routes commands to the correct profile.

### Expected Outcomes

- **Reduced context-switching overhead**: Switching environments becomes a one-line command
- **Improved security posture**: Built-in support for environment variable references encourages secure credential management
- **Fewer production incidents**: Confirmation prompts on production profiles prevent accidental destructive operations
- **Better developer experience**: Clear visibility into active profile, standardized configuration management
- **Team productivity gains**: Shareable profile templates reduce onboarding time and configuration errors
- **Backward compatibility**: Existing users continue working without any changes to their workflows
- **Agent-safe concurrency**: Multiple processes in different worktrees resolve profiles independently with no shared mutable state

## Requirements

### Functional Requirements

1. **Multi-Profile Configuration**

   - Support multiple named profiles in a single configuration file
   - Each profile contains connection details (server, token, timeout, etc.)
   - Profiles are stored in `~/.datahub/config.yaml` in human-readable YAML format

2. **Profile Selection with Clear Precedence**

   - Command-line flag: `--profile <name>` (highest priority)
   - Environment variable: `DATAHUB_PROFILE=<name>`
   - Nearest ancestor `.datahub/config.yaml` (walk from CWD toward `~`):
     - If file has `profiles:` key → full config, use directly
     - If file has `profile: <name>` key → pointer, resolve named profile from `~/.datahub/config.yaml`
   - `~/.datahub/config.yaml` fallback chain: `current_profile` → profile named `"default"`
   - Legacy `~/.datahubenv` file (backward compatibility)

   Note: The ancestor walk naturally reaches `~/.datahub/config.yaml` as the ultimate fallback since `~` is an ancestor of every directory. Steps 3 and 4 are listed separately because the home config has additional fallback behavior (`current_profile`, `"default"` profile).

   Note: `DATAHUB_GMS_URL` and `DATAHUB_GMS_TOKEN` environment variables override the *connection details* of whichever profile is resolved, but do not participate in profile *selection*.

3. **Profile Management Commands**

   - `datahub profile list` - List all profiles
   - `datahub profile current` - Show active profile
   - `datahub profile show <name>` - Display profile details
   - `datahub profile use <name>` - Pin current directory to a named profile (writes `.datahub/config.yaml` in CWD)
   - `datahub profile set-default <name>` - Set the global default profile (`current_profile` in `~/.datahub/config.yaml`)
   - `datahub profile add <name>` - Add/update profile
   - `datahub profile remove <name>` - Delete profile
   - `datahub profile test <name>` - Test connection
   - `datahub profile validate` - Validate configuration syntax
   - `datahub profile export <name>` - Export profile (secrets redacted)

4. **Environment Variable Interpolation**

   - Support `${VAR_NAME}` syntax for variable substitution
   - Support `${VAR_NAME:-default}` for default values
   - Support `${VAR_NAME:?error}` for required variables
   - Interpolation happens at config load time

5. **Security Features**

   - Token redaction in CLI output
   - Guidance toward environment variable storage for tokens
   - Warning when storing tokens directly in config
   - `require_confirmation` flag per profile for destructive operations

6. **Production Safety**

   - Confirmation prompts for destructive operations on flagged profiles
   - User must type profile name (uppercase) to confirm
   - Clear warnings showing profile, server, and operation details
   - `--force` flag to bypass for automation

7. **Backward Compatibility**
   - Automatic detection of legacy `~/.datahubenv` file
   - Seamless fallback to legacy config if new config doesn't exist
   - All existing environment variables (`DATAHUB_GMS_URL`, etc.) continue to work as overrides
   - No breaking changes to existing CLI commands
   - Migration path from legacy to new config format

8. **Ancestor-Walk Config Resolution**
   - The CLI walks up from the current working directory looking for `.datahub/config.yaml`
   - First match wins (closest ancestor); `~/.datahub/config.yaml` is the ultimate fallback
   - The local file can be a **full config** (same schema as global, with `profiles:`) or a **profile pointer** (`profile: <name>` referencing the home config)
   - Detection is unambiguous: `profiles:` key = full config, `profile:` key = pointer
   - No git dependency — works in any directory (git repos, worktrees, bare directories, CI containers)
   - No merging across ancestor levels — closest config wins completely
   - Warn (don't block) when a non-home config contains literal token values; nudge toward `${VAR}` interpolation

### Non-Functional Requirements

9. **Performance**

   - Profile loading adds negligible overhead to CLI commands (< 50ms)
   - Configuration file parsing is efficient for typical sizes (< 100 profiles)

10. **Usability**

    - Clear, actionable error messages with suggestions
    - Consistent command-line interface patterns
    - Visual indicators for current profile
    - Color-coded output for different environments (dev=green, staging=yellow, prod=red)

11. **Testability**

    - Comprehensive unit tests for all profile operations
    - Integration tests for end-to-end workflows
    - Tests for precedence ordering (including local pointer)
    - Tests for edge cases (missing files, invalid YAML, etc.)

12. **Documentation**
    - Updated CLI documentation with profile examples
    - Migration guide from legacy configuration
    - Best practices for credential management
    - Troubleshooting guide for common issues

### Extensibility

13. **Future-Proof Design**

    - Profile model uses Pydantic, allowing easy addition of new fields
    - Configuration format version field (`version: "1.0"`) enables future migrations
    - Plugin architecture for additional profile validation rules
    - Extensible confirmation prompt system for other operation types

14. **SDK Integration**
    - Python SDK can use profile configuration: `DataHubClient.from_env(profile="staging")`
    - Profile system accessible via public API for custom tools
    - Configuration models exported for external use

## Non-Requirements

The following are explicitly **out of scope** for this initial RFC:

1. **Profile Encryption**: Native encryption of the config file or token storage. Users should leverage OS-level encryption (e.g., macOS Keychain, Linux secret-service) via environment variables.

2. **Remote Profile Storage**: Syncing profiles across machines, storing profiles in remote services, or team-level profile sharing via a central repository. Users can manually share config files via version control.

3. **Role-Based Access Control**: Profile-level permissions or role restrictions. Access control should be managed at the DataHub server level.

4. **Profile Groups/Hierarchies**: Inheritance between profiles, profile templates, or profile composition. Each profile is independent.

5. **GUI Configuration**: A web-based or desktop GUI for profile management. This RFC focuses solely on CLI tooling.

6. **Automatic Merge of Ancestor Configs**: The ancestor walk finds the nearest `.datahub/config.yaml` and uses it as the complete truth. It does NOT merge configs from multiple ancestor levels. Closest wins completely, like `.editorconfig` or `.nvmrc`. If you need settings from a parent config, copy or reference them explicitly.

7. **Profile Analytics**: Tracking which profiles are used, usage statistics, or telemetry. This could be added later if needed.

8. **Other CLI Tools**: Integration with tools outside the DataHub ecosystem (Airflow CLI, dbt CLI, etc.). Each tool manages its own configuration.

## Detailed design

### Architecture Overview

The profile system adds a new configuration layer to the DataHub CLI without disrupting existing functionality. The core components are:

```
┌─────────────────────────────────────────────────────────────┐
│                     CLI Command Layer                        │
│  (entrypoints.py, *_cli.py files)                          │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ├──> Profile parameter extraction from:
                        │    • --profile flag (click option)
                        │    • Context object (ctx.obj["profile"])
                        │
┌───────────────────────▼─────────────────────────────────────┐
│                  Config Discovery Layer                      │
│  (config_utils.py)                                          │
│                                                              │
│  find_nearest_config() → Optional[Tuple[Path, dict]]       │
│  Walks CWD → ~ looking for .datahub/config.yaml            │
│  First match wins (closest ancestor)                        │
│                                                              │
│  Precedence: --profile flag > DATAHUB_PROFILE env var >    │
│    nearest .datahub/config.yaml > current_profile > default │
└───────────────────────┬─────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────┐
│                  Profile Loading Layer                       │
│  (config_utils.py)                                          │
│                                                              │
│  load_profile_config(profile_name) → DatahubClientConfig   │
│  • Full config (profiles: key) → use directly              │
│  • Pointer (profile: key) → resolve from ~/.datahub/       │
│  • Applies DATAHUB_GMS_URL / DATAHUB_GMS_TOKEN overrides   │
│  • Warns on literal tokens in non-home configs             │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ├──> Reads from:
                        │
┌───────────────────────▼─────────────────────────────────────┐
│                Configuration Storage                         │
│                                                              │
│  Ancestor walk (CWD → ~):                                   │
│    ./datahub/config.yaml           (CWD)                   │
│    ../.datahub/config.yaml         (parent)                │
│    ...                              (ancestors)             │
│    ~/.datahub/config.yaml          (home — ultimate)       │
│                                                              │
│  Each can be:                                               │
│  • Full config (profiles + creds, env var interpolation)   │
│  • Profile pointer (profile: <name>)                       │
│                                                              │
│  ~/.datahubenv                     (legacy fallback)        │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ├──> Parsed into:
                        │
┌───────────────────────▼─────────────────────────────────────┐
│              Configuration Models (Pydantic)                 │
│  (configuration/config.py)                                  │
│                                                              │
│  DataHubConfig                                              │
│  ├── version: str                                           │
│  ├── current_profile: Optional[str]                        │
│  └── profiles: Dict[str, ProfileConfig]                    │
│                                                              │
│  ProfileConfig (extends DatahubClientConfig)               │
│  ├── server: str                                            │
│  ├── token: Optional[str]                                   │
│  ├── timeout_sec: int                                       │
│  ├── description: Optional[str]                            │
│  └── require_confirmation: bool                            │
│                                                              │
│  LocalProfilePointer                                        │
│  └── profile: str                                           │
└─────────────────────────────────────────────────────────────┘
```

### File Structure

```
src/datahub/
├── configuration/
│   ├── config.py                    # NEW: Pydantic models (ProfileConfig, DataHubConfig, LocalProfilePointer)
│   └── env_vars.py                  # MODIFIED: Add DATAHUB_PROFILE env var
├── cli/
│   ├── profile_cli.py               # NEW: Profile management commands (incl. use, set-default)
│   ├── config_utils.py              # MODIFIED: Profile resolution + loading, find_nearest_config()
│   ├── entrypoints.py               # MODIFIED: Global --profile flag
│   ├── delete_cli.py                # MODIFIED: Add confirmation prompts
│   ├── get_cli.py                   # MODIFIED: Profile parameter support
│   ├── put_cli.py                   # MODIFIED: Profile parameter support
│   └── ... (other CLI commands)     # MODIFIED: Profile parameter support
└── ingestion/graph/
    └── client.py                    # MODIFIED: get_default_graph(profile=...)

tests/unit/
├── test_config.py                   # NEW: Test config models
└── cli/
    ├── test_config_utils_profiles.py # NEW: Test profile functions (incl. local pointer resolution)
    └── test_profile_cli.py           # NEW: Test CLI commands
```

Ancestor-walk config resolution (any level):

```
~/.datahub/config.yaml                        # User-level (ultimate fallback)
~/work/.datahub/config.yaml                   # Company-wide defaults (optional)
~/work/project/.datahub/config.yaml           # Repo-level config or pointer
~/work/project/worktree/.datahub/config.yaml  # Worktree-level (rare)
```

Each `.datahub/config.yaml` can be a full config (with `profiles:`) or a pointer (`profile: <name>`).

### Core Design Details

#### Profile Configuration Model

Each profile contains connection details for a DataHub instance:

| Field | Required | Description |
|-------|----------|-------------|
| `server` | Yes | DataHub GMS server URL |
| `token` | No | Authentication token (supports `${VAR}` interpolation) |
| `timeout_sec` | No | Connection timeout in seconds |
| `description` | No | Human-readable description |
| `require_confirmation` | No | If `true`, prompt before destructive operations |
| `extra_headers` | No | Additional HTTP headers |
| `ca_certificate_path` | No | Path to CA certificate |
| `disable_ssl_verification` | No | Disable SSL verification |

Profile fields extend the existing `DatahubClientConfig`, so all current connection options are supported.

#### Config File Detection

When the CLI finds a `.datahub/config.yaml`, it determines the file type by key presence:

- **`profiles:` key present** → Full config (same schema as `~/.datahub/config.yaml`). Use directly.
- **`profile:` key present** → Pointer. Resolve the named profile from `~/.datahub/config.yaml`.
- **Neither key** → Error with a message explaining the expected format.
- **Both keys** → Error. A file should be one or the other.

#### Config Resolution Behavior

The resolution process follows these steps:

1. If `--profile <name>` flag is provided, use that profile name.
2. Else if `DATAHUB_PROFILE` environment variable is set, use that profile name.
3. Else walk up the directory tree from CWD toward `~`, looking for `.datahub/config.yaml`:
   - If a **full config** is found, select a profile from it using its `current_profile` or `"default"`.
   - If a **pointer** is found, use the named profile from `~/.datahub/config.yaml`.
4. If no config was found in the walk (including at `~`), fall back to legacy `~/.datahubenv`.
5. If nothing found, show an error suggesting `datahub init` or `datahub profile add`.

After profile selection, `DATAHUB_GMS_URL` and `DATAHUB_GMS_TOKEN` environment variables override the resolved profile's connection details (maintaining backward compatibility).

#### Literal Token Warnings

When a `.datahub/config.yaml` outside of `~` contains a literal token value (not a `${VAR}` reference), the CLI emits a warning on stderr:

```
WARNING: Profile 'ci' in /path/to/repo/.datahub/config.yaml has a literal token.
Use ${VAR} syntax to avoid committing secrets.
```

This is a warning, not an error — it does not block execution. The check operates on the raw YAML before environment variable interpolation, so `${VAR}` references do not trigger it.

#### Profile Management Commands

All `datahub profile` commands follow consistent patterns:

- Validate inputs before making changes
- Provide clear success/error messages with context (server URL, description)
- Redact tokens in output (show last 4 characters only)

**`datahub profile use <name>`** — Creates `.datahub/config.yaml` in the current working directory with `profile: <name>`. Validates the profile exists in `~/.datahub/config.yaml` first.

**`datahub profile set-default <name>`** — Sets `current_profile` in `~/.datahub/config.yaml`. Used when no local config or flags are present.

**`datahub profile show <name>`** — Displays profile details with token redaction. Indicates whether a token comes from an environment variable or is stored directly.

**`datahub profile current`** — Shows the active profile and how it was resolved (which file, which precedence layer).

#### Production Safety

When a profile has `require_confirmation: true`, destructive operations (delete, etc.) trigger a confirmation prompt:

```
⚠️  DESTRUCTIVE OPERATION WARNING
Profile: prod
Server: https://datahub.company.com
Operation: delete entities by filter

This operation cannot be undone. Type 'PROD' to confirm:
```

Key safety features:

- **Profile-specific**: Only applies to profiles with `require_confirmation: true`
- **Clear context**: Shows exactly what will be affected (profile, server, operation)
- **Explicit confirmation**: User must type the profile name in uppercase (not just "yes")
- **Bypass for automation**: `--force` flag skips the prompt for CI/CD pipelines

#### Global `--profile` Flag

The `--profile` flag is available on the top-level `datahub` command and applies to all subcommands. It also reads from the `DATAHUB_PROFILE` environment variable. This flag takes the highest precedence in the resolution chain.

### Environment Variable Interpolation

Config values support standard shell-like variable substitution, resolved at config load time:

| Syntax | Behavior |
|--------|----------|
| `${VAR}` | Replace with value of `VAR`, or empty string if unset |
| `${VAR:-default}` | Replace with value of `VAR`, or `default` if unset |
| `${VAR:?error message}` | Replace with value of `VAR`, or raise an error with the message if unset |

Example:

```yaml
profiles:
  prod:
    server: https://datahub.company.com
    token: ${DATAHUB_PROD_TOKEN:?Set DATAHUB_PROD_TOKEN before using the prod profile}
```

### Configuration File Format

The YAML configuration file has a simple, human-readable structure:

```yaml
version: "1.0"
current_profile: dev

profiles:
  dev:
    server: http://localhost:8080
    token: ${DATAHUB_DEV_TOKEN}
    timeout_sec: 30
    description: "Local development"

  staging:
    server: https://datahub-staging.company.com
    token: ${DATAHUB_STAGING_TOKEN}
    timeout_sec: 60
    extra_headers:
      X-Environment: staging
    description: "Staging environment"

  prod:
    server: https://datahub.company.com
    token: ${DATAHUB_PROD_TOKEN}
    timeout_sec: 120
    require_confirmation: true
    extra_headers:
      X-Environment: production
    description: "Production - USE WITH CAUTION"
```

**Design decisions:**

- **YAML format**: Human-readable, supports comments, widely understood
- **Flat structure**: No unnecessary nesting
- **Optional fields**: Only `server` is required per profile
- **Version field**: Enables future format migrations
- **Location**: `~/.datahub/config.yaml` (standard for CLI tools)

### Local Config File Format

Any `.datahub/config.yaml` in the ancestor chain can take one of two forms:

**Form 1: Profile pointer** (simple — references a named profile from `~/.datahub/config.yaml`):

```yaml
# Safe to commit — contains no secrets.
profile: staging
```

**Form 2: Full config** (self-contained — same schema as the home config):

```yaml
version: "1.0"
current_profile: ci
profiles:
  ci:
    server: https://staging.datahub.com
    token: ${DATAHUB_STAGING_TOKEN}   # use env var refs to keep secrets out
    description: "CI environment"
```

**Detection heuristic**: If the file has a `profiles:` key, it's a full config. If it has a `profile:` key, it's a pointer. The keys don't overlap, so detection is unambiguous.

**Literal token warning**: When loading a non-home config that contains a literal token (not a `${VAR}` reference), the CLI emits a warning:

```
WARNING: Profile 'ci' in /path/to/repo/.datahub/config.yaml has a literal token.
Use ${VAR} syntax to avoid committing secrets.
```

**Design decisions:**

- **Same filename everywhere**: All levels use `.datahub/config.yaml`. The ancestor walk naturally resolves the right one based on proximity.
- **Two modes, one file**: Rather than requiring separate file formats for different levels, the same file can be a full config or a lightweight pointer. This reduces cognitive overhead.
- **Warn, don't block**: Literal tokens in non-home configs are warned about, not rejected. This supports valid use cases (local dev, air-gapped environments, temporary debugging) while nudging toward best practices.
- **Future extensibility**: The `.datahub/` directory at any level could later hold other config (ingestion recipes, custom assertions, etc.), but that is out of scope for this RFC.

### Migration from Legacy Configuration

Users can migrate from `~/.datahubenv` manually or through automatic detection:

**Automatic migration** happens when running `datahub init` and detects a legacy config:

1. If `~/.datahub/config.yaml` already exists, migration is skipped
2. If `~/.datahubenv` exists, its contents are converted to a `"default"` profile in `~/.datahub/config.yaml`
3. The legacy file is left intact — the user is informed and can delete it manually

```
✓ Migrated configuration to new format
  Old: ~/.datahubenv
  New: ~/.datahub/config.yaml

You can safely delete the old config file.
```

**Key migration features:**

- Non-destructive: Legacy file remains intact
- Automatic detection: Happens transparently on first use
- Clear communication: User informed about what happened
- "default" profile: Preserves existing behavior exactly

### Error Handling and User Feedback

The implementation provides clear, actionable error messages:

**Profile not found:**

```
Error: Profile 'staging' not found.
Available profiles: dev, prod

Run 'datahub profile list' to see all profiles.
Run 'datahub profile add staging' to create a new profile.
```

**Missing environment variable:**

```
Error: Required environment variable not set: DATAHUB_PROD_TOKEN
Profile 'prod' requires this variable to be set.

Set it with:
  export DATAHUB_PROD_TOKEN=<your-token>
```

**Invalid YAML syntax:**

```
Error: Failed to parse configuration file: ~/.datahub/config.yaml
Line 5: mapping values are not allowed here

Run 'datahub profile validate' to check syntax.
See: https://yaml.org/spec/ for YAML syntax help.
```

### Testing Strategy

The implementation includes comprehensive tests:

1. **Unit Tests** (`tests/unit/`)

   - Configuration model validation (25 tests)
   - Profile loading with precedence, including ancestor walk
   - CLI command functionality (28 tests)
   - Environment variable interpolation
   - Error handling and edge cases
   - Ancestor walk: finds nearest config, stops at `~`, handles missing files
   - Full config mode (`profiles:` key) vs pointer mode (`profile:` key)
   - Invalid config file (neither key) → clear error
   - Literal token warning for non-home configs
   - `datahub profile use` writes pointer to CWD
   - `datahub profile set-default` always modifies home config
   - Concurrent resolution from different working directories (simulating worktrees)

2. **Integration Tests** (future work)

   - End-to-end workflow tests
   - Tests with real DataHub instance
   - Migration scenarios
   - Multi-command workflows
   - Multi-worktree profile isolation

3. **Test Coverage**
   - Target: >90% coverage for new code
   - All edge cases covered
   - Backward compatibility verified

## How we teach this

### Audience Impact

This feature impacts multiple audiences:

1. **CLI Users** (Primary): All users of `datahub` CLI commands
2. **Python SDK Users** (Secondary): Can leverage profiles via `DataHubClient.from_env(profile="...")`
3. **CI/CD Engineers**: Can use profiles in automated pipelines
4. **Support Engineers**: Can maintain multiple customer profile configurations
5. **New Users**: Get clearer onboarding with `datahub init` and profile setup
6. **AI Agent Operators**: Teams running multiple AI agents in parallel git worktrees can isolate each agent's DataHub target without environment variable coordination

### Naming and Terminology

The feature uses familiar terminology from other CLI tools:

- **Profile**: A named configuration set (inspired by AWS CLI, kubectl, etc.)
- **Current/Active Profile**: The profile currently in use
- **Default Profile**: Fallback profile when none specified

These terms are intuitive and align with industry standards.

### Documentation Changes

1. **Getting Started Guide**: Add profile setup to initial onboarding

   ```
   # Quick Start
   1. Install DataHub CLI: pip install acryl-datahub
   2. Set up your first profile: datahub profile add dev --server http://localhost:8080
   3. Test connection: datahub profile test dev
   4. Start using DataHub: datahub get --urn "..."
   ```

2. **CLI Reference**: Update command documentation

   - Add global `--profile` flag to all commands
   - Document `datahub profile` command group
   - Include profile selection precedence

3. **Configuration Guide**: New section on profile management

   - Configuration file structure
   - Profile setup best practices
   - Environment variable interpolation
   - Security recommendations

4. **Migration Guide**: Help existing users upgrade

   - Why profiles are useful
   - Step-by-step migration from `~/.datahubenv`
   - Common patterns (dev/staging/prod setup)

5. **Troubleshooting**: Common issues and solutions
   - "Profile not found" errors
   - Environment variable issues
   - Configuration syntax errors

### Teaching to New Users

New users are introduced to profiles during initial setup:

```bash
$ datahub init

Welcome to DataHub! Let's set up your first profile.

Profile name [default]: dev
DataHub server URL: http://localhost:8080

How would you like to store your access token?
  1. Environment variable (recommended) ← default
  2. Store in config file

Choice [1]: 1

Great! Set your token with:
  export DATAHUB_TOKEN=<your-token>

✓ Profile 'dev' created successfully!

Next steps:
  • Test connection: datahub profile test dev
  • List entities: datahub get --urn "..."
  • Add more profiles: datahub profile add staging
```

This guides users toward secure practices (environment variables) while making the initial setup simple.

### Teaching to Existing Users

Existing users experience a seamless transition:

1. **Backward Compatibility**: Existing `.datahubenv` file continues to work with zero changes
2. **Opt-in Migration**: Users can migrate when ready via `datahub profile migrate`
3. **Release Notes**: Clear explanation of new features and benefits
4. **Documentation**: Side-by-side examples showing old vs. new approach

Example documentation:

Example upgrade guide:

> **Do I Need to Change Anything?** No! Your existing configuration continues to work. The CLI automatically detects your `~/.datahubenv` file.
>
> **How to Upgrade:**
> 1. Migrate your existing config: `datahub profile migrate`
> 2. Add additional profiles: `datahub profile add staging --server https://staging.datahub.com`
> 3. Switch between profiles: `datahub profile use staging`
>
> Your old config file remains unchanged as a backup.

### Teaching the Worktree / Agent Workflow

For teams using git worktrees or AI agents that operate in parallel:

```bash
# One-time setup: create profiles in the global config
datahub profile add staging --server https://staging.datahub.com --token-env DATAHUB_STAGING_TOKEN
datahub profile add prod --server https://prod.datahub.com --token-env DATAHUB_PROD_TOKEN --require-confirmation

# In each worktree, pin a profile
cd ~/worktrees/feature-branch
datahub profile use staging
# Creates .datahub/config.yaml with: profile: staging

cd ~/worktrees/hotfix-branch
datahub profile use prod
# Creates .datahub/config.yaml with: profile: prod

# Now any datahub command in each worktree auto-targets the right instance
# No environment variables needed, no --profile flag needed
cd ~/worktrees/feature-branch
datahub get --urn "urn:li:dataset:..."   # -> hits staging

cd ~/worktrees/hotfix-branch
datahub get --urn "urn:li:dataset:..."   # -> hits prod
```

**Key points to teach:**
- The CLI uses ancestor-walk discovery — it walks up from CWD to `~` to find the nearest `.datahub/config.yaml`
- A profile pointer (`profile: staging`) is safe to commit (no credentials). A full local config should use `${VAR}` references for tokens.
- Each worktree resolves its config independently — no global state mutation
- The `--profile` flag and `DATAHUB_PROFILE` env var still take precedence if set
- Users not using worktrees can ignore this feature entirely — `current_profile` and `set-default` work as before
- Intermediate-level configs are also supported (e.g., `~/work/.datahub/config.yaml` for company-wide defaults)

## Drawbacks

### 1. Increased Complexity

**Concern**: Adding profiles increases the conceptual surface area of the CLI.

**Mitigation**:
- Backward compatibility ensures existing users aren't forced to learn new concepts
- Clear documentation and defaults guide new users
- The feature is intuitive to anyone familiar with other CLI tools (AWS CLI, kubectl, gcloud)
- Profile management is entirely optional

**Impact**: Low - The benefits significantly outweigh the added complexity.

### 2. Configuration File Location Change

**Concern**: Moving from `~/.datahubenv` to `~/.datahub/config.yaml` could confuse users.

**Mitigation**:
- Automatic detection of legacy config
- Clear migration path with helpful messages
- Both files can coexist (profile system checks legacy as fallback)
- Documentation clearly explains the change

**Impact**: Low - Transition is smooth and well-communicated.

### 3. Potential for Profile Proliferation

**Concern**: Users might create many profiles and lose track of which is which.

**Mitigation**:
- `datahub profile list` shows all profiles with descriptions
- `datahub profile current` shows active profile at any time
- Profile descriptions encourage documentation
- Best practices guide recommends standard naming (dev/staging/prod)

**Impact**: Low - Standard tooling prevents this from becoming a real issue.

### 4. Security Considerations

**Concern**: Config file might accidentally be committed to version control with secrets.

**Mitigation**:
- Strong guidance toward environment variable usage for tokens
- CLI warns when storing tokens directly in config
- Default behavior encourages `--token-env` flag
- Documentation emphasizes best practices
- `.gitignore` recommendations in docs

**Impact**: Medium - Requires good documentation and user education.

### 5. Testing Overhead

**Concern**: Testing profile precedence and all combinations adds complexity.

**Mitigation**:
- Comprehensive unit test suite (97 tests) already implemented
- Clear precedence rules make testing straightforward
- Test fixtures simplify profile configuration in tests

**Impact**: Low - Testing is well-structured and maintainable.

### 6. Maintenance Burden

**Concern**: Maintaining two configuration systems (legacy + profiles) increases maintenance.

**Mitigation**:
- Legacy support is minimal (simple file parsing)
- Clear deprecation path: legacy can be deprecated in future major version
- Shared configuration models reduce duplication
- Well-tested implementation reduces bug surface

**Impact**: Low - Short-term increase, but profile system simplifies long-term maintenance.

### 7. Ancestor Walk Complexity

**Concern**: The ancestor-walk discovery means a `.datahub/config.yaml` anywhere in the directory tree could affect CLI behavior, potentially in surprising ways (e.g., a forgotten config in a parent directory).

**Mitigation**:
- `datahub profile current` shows the full resolution chain — which file was found, at what path, and whether it's a full config or pointer
- The walk stops at `~` (never goes above the home directory)
- Closest wins with no merging — the mental model is simple
- This is an established pattern used by `.editorconfig`, `.nvmrc`, Dagster's `dg.toml`, etc.

**Impact**: Low - The pattern is well-understood by developers and the diagnostic tooling makes it easy to debug.

## Alternatives

### Alternative 1: Environment Variables Only

**Description**: Instead of profiles, rely entirely on environment variables for configuration.

**Example**:
```bash
# Dev
export DATAHUB_GMS_URL=http://localhost:8080
export DATAHUB_GMS_TOKEN=$DEV_TOKEN

# Prod
export DATAHUB_GMS_URL=https://prod.datahub.com
export DATAHUB_GMS_TOKEN=$PROD_TOKEN
```

**Pros**:

- Simple implementation
- No new configuration files
- Works with existing patterns

**Cons**:

- No persistence - must set variables in each shell session
- No named profiles - rely on user memory or scripts
- No built-in safety features
- Poor discoverability
- Difficult to share configurations

**Why Not Chosen**: Poor user experience for multi-environment workflows. Users would need to build their own profile system via shell scripts or aliases, which is error-prone and not portable.

### Alternative 2: Multiple Configuration Files

**Description**: Use separate files for each environment: `~/.datahubenv.dev`, `~/.datahubenv.prod`, etc.

**Example**:

```bash
# Switch environments by symlinking
ln -sf ~/.datahubenv.dev ~/.datahubenv
```

**Pros**:

- Simple to understand
- Each environment isolated in separate file
- Easy to version control per-environment

**Cons**:

- No built-in profile switching
- Manual symlinking is error-prone
- No "current profile" visibility
- No safety features
- Doesn't scale well (many files clutter home directory)
- No profile listing/discovery

**Why Not Chosen**: Requires manual file management, lacks safety features, and provides poor user experience.

### Alternative 3: Directory-Based Configuration

**Description**: Similar to `.git/config`, use directory-local configuration files.

**Example**:

```bash
# Each project has .datahub/config
project1/.datahub/config  # Points to dev
project2/.datahub/config  # Points to staging
```

**Pros**:

- Automatic profile selection based on current directory
- Works well for project-specific configurations
- Can be committed to version control

**Cons**:

- Secrets in project directories are dangerous
- Full inline configuration per-directory is complex to maintain
- No cross-project profile sharing if each directory has its own config

**Why Partially Adopted**: The original RFC rejected full directory-based configuration, but the agent/worktree use case demonstrated a strong need for directory-scoped config. The adopted design uses ancestor-walk discovery (like `.editorconfig`, Dagster's `dg.toml`) where any directory can provide a `.datahub/config.yaml` — either a lightweight profile pointer or a full config. To mitigate credential risks, the CLI warns when non-home configs contain literal tokens and nudges toward `${VAR}` interpolation.

### Alternative 4: Config File with Environment Selection

**Description**: Single config file with all environments, use environment variable to select.

**Example**:

```yaml
# ~/.datahub/config.yaml
environments:
  dev: { server: ..., token: ... }
  prod: { server: ..., token: ... }

# Usage
export DATAHUB_ENV=prod
datahub get --urn "..."
```

**Pros**:

- Single source of truth
- No profile precedence complexity

**Cons**:

- No command-line flag for profile selection
- No "current environment" setting
- Still requires environment variable for switching
- No per-command overrides
- Terminology confusion (environment vs. profile)

**Why Not Chosen**: Less flexible than profile system, still requires environment variables for switching, and doesn't provide command-line convenience.

### Alternative 5: External Profile Management Tool

**Description**: Build a separate tool (`datahub-profile`) to manage profiles.

**Example**:

```bash
datahub-profile set dev
datahub get --urn "..."
```

**Pros**:

- Separation of concerns
- Could be used by multiple tools

**Cons**:

- Additional tool to install and maintain
- Complex integration with CLI
- Poor user experience (two tools instead of one)
- Configuration spread across multiple places
- Harder to discover and learn

**Why Not Chosen**: Adds unnecessary complexity and friction. Profile management should be integrated into the main CLI for better UX.

### Why the Proposed Solution is Best

The proposed profile system strikes the right balance:

1. **Excellent UX**: Simple commands (`datahub profile use dev`) that are intuitive
2. **Flexible**: Supports command-line flags, environment variables, and persistent settings
3. **Safe**: Built-in production safety features
4. **Discoverable**: `datahub profile list` makes profiles visible
5. **Industry Standard**: Follows patterns from AWS CLI, kubectl, gcloud, etc.
6. **Backward Compatible**: Doesn't break existing workflows
7. **Agent/Worktree Safe**: Ancestor-walk config resolution enables concurrent processes without conflicts
8. **Future-Proof**: Extensible configuration model

## Rollout / Adoption Strategy

### Phase 1: Internal Testing and Documentation (Week 1-2)

1. **Code Review**: RFC approved and implementation reviewed by core team
2. **Documentation**: Complete all documentation (CLI reference, guides, migration docs)
3. **Internal Dogfooding**: DataHub team tests profiles in daily work
4. **Worktree Testing**: Test local pointer resolution across git worktrees to verify concurrent isolation
5. **Fix Issues**: Address any bugs or UX issues discovered

### Phase 2: Beta Release (Week 3-4)

1. **Beta Announcement**: Announce in Slack #announcements channel
2. **Feature Flag**: Initially behind feature flag (if needed for safety)
3. **Early Adopters**: Recruit volunteers from community to test
4. **Gather Feedback**: Collect feedback via Slack and GitHub issues
5. **Iterate**: Make improvements based on early feedback

### Phase 3: General Availability (Week 5-6)

1. **Release**: Include in next minor version (e.g., v0.13.0)
2. **Release Notes**: Highlight profile support with examples
3. **Blog Post**: Write detailed blog post with use cases and tutorials
4. **Slack Announcement**: Announce in #general and #contribute-code
5. **Demo Video**: Create short video tutorial (2-3 minutes)

### Phase 4: Promotion and Education (Ongoing)

1. **Documentation**: Keep docs updated with user feedback
2. **Stack Overflow**: Answer questions and create canonical answers
3. **Conference Talks**: Present profile feature at DataHub meetups
4. **Webinars**: Include profiles in DataHub onboarding webinars
5. **Monitor Adoption**: Track usage via telemetry (if available)

### Backward Compatibility

**This is a non-breaking change.** The rollout strategy ensures smooth adoption:

1. **Automatic Detection**: CLI automatically detects and uses legacy `~/.datahubenv`
2. **No Action Required**: Existing users can keep using current setup indefinitely
3. **Opt-in Migration**: Users migrate when ready via `datahub profile migrate`
4. **Environment Variable Overrides**: All existing env vars (`DATAHUB_GMS_URL`, etc.) continue to work
5. **Deprecation Timeline**: Legacy support maintained for at least 2 major versions (minimum 1 year)
6. **No Worktree Required**: Ancestor-walk config resolution is entirely opt-in. Users who don't place `.datahub/config.yaml` files in their directories are unaffected — `current_profile` and `set-default` in `~/.datahub/config.yaml` provide the same single-profile switching experience as before.

### Migration Tools

Users have multiple migration paths:

**Automatic Migration** (recommended):

```bash
# Detects legacy config and offers to migrate
datahub init
```

**Manual Migration**:

```bash
# Explicit migration command
datahub profile migrate

# Or create new profile manually
datahub profile add default \
  --server $(grep server ~/.datahubenv | cut -d= -f2) \
  --token-env DATAHUB_TOKEN
```

**Hybrid Approach** (during transition):

```bash
# Keep legacy config as backup
# Add new profiles alongside it
datahub profile add staging --server https://staging.datahub.com
datahub profile add prod --server https://prod.datahub.com

# Switch between them
datahub profile use staging
```

### Telemetry and Monitoring

To measure adoption (opt-in telemetry only):

1. **Profile Usage**: Track how many users use profiles vs. legacy config
2. **Profile Count**: Track average number of profiles per user
3. **Command Usage**: Track most-used profile commands (`use`, `list`, `add`)
4. **Migration Rate**: Track migration from legacy to profiles
5. **Error Rates**: Monitor profile-related errors

This data helps identify areas for improvement and validates the feature's value.

### Support Strategy

1. **Troubleshooting Guide**: Comprehensive guide for common issues
2. **Slack Support**: Dedicated support in #datahub-cli channel
3. **GitHub Issues**: Template for profile-related bug reports
4. **Stack Overflow Tags**: `datahub-cli-profiles` tag for community questions
5. **Office Hours**: Include profiles in regular DataHub office hours

### Deprecation Path (Future)

While legacy config will be supported long-term, eventual deprecation could follow this path:

1. **Version 0.13.0** (2026 Q1): Profiles introduced, legacy fully supported
2. **Version 0.15.0** (2026 Q3): Warning message when using legacy config (suggest migration)
3. **Version 1.0.0** (2027): Legacy support deprecated, still works with warning
4. **Version 2.0.0** (2028): Legacy support removed (breaking change)

This provides ample time (2+ years) for users to migrate.

## Future Work

The profile system provides a foundation for several future enhancements:

### 1. Profile Templates and Sharing

**Use Case**: Teams want to share standard profile configurations.

**Possible Implementation**:

```bash
# Export profile template (with secrets redacted)
datahub profile export prod --template > prod-template.yaml

# Import profile from template
datahub profile import --template prod-template.yaml --name prod

# Share templates in version control
git add .datahub/templates/prod.yaml
```

**Benefits**: Faster onboarding, consistent configurations across team.

### 2. Profile Validation Rules

**Use Case**: Enforce organization policies on profile configurations.

**Possible Implementation**:

```yaml
# ~/.datahub/config.yaml
version: "1.0"

validation_rules:
  - require_confirmation_for_prod: true
  - require_env_vars_for_tokens: true
  - allowed_servers:
      - "*.company.com"
      - "localhost"

profiles:
  prod:
    server: https://datahub.company.com
    # ...
```

**Benefits**: Prevent misconfigurations, enforce security policies.

### 3. Profile Groups

**Use Case**: Manage related profiles together (e.g., all dev environments).

**Possible Implementation**:

```bash
# Create profile group
datahub profile group create dev-group --profiles dev1,dev2,dev3

# Run command against all profiles in group
datahub profile group run dev-group -- get --urn "..."
```

**Benefits**: Bulk operations, easier management of many profiles.

### 4. Profile Encryption

**Use Case**: Encrypt tokens stored in config file.

**Possible Implementation**:

```bash
# Enable encryption for profile
datahub profile encrypt prod --key ~/.datahub/keys/prod.key

# Automatically decrypt when used
datahub --profile prod get --urn "..."
```

**Benefits**: Defense-in-depth security for stored credentials.

**Note**: Environment variables are still recommended approach.

### 5. Profile Inheritance

**Use Case**: Share common settings across profiles (e.g., timeout, headers).

**Possible Implementation**:

```yaml
profiles:
  base:
    timeout_sec: 120
    extra_headers:
      X-Client: datahub-cli

  prod:
    extends: base
    server: https://datahub.company.com
    token: ${PROD_TOKEN}
```

**Benefits**: DRY configuration, easier maintenance.

### 6. Remote Profile Storage

**Use Case**: Sync profiles across machines, share team profiles centrally.

**Possible Implementation**:

```bash
# Pull profiles from remote
datahub profile pull --remote https://config.company.com/datahub/profiles

# Push local profiles to remote
datahub profile push --remote https://config.company.com/datahub/profiles
```

**Benefits**: Multi-machine consistency, centralized team management.

### 7. Profile Context in Output

**Use Case**: Always know which profile is being used.

**Possible Implementation**:

```bash
# Show profile in command output
$ datahub --profile prod get --urn "..."
[Profile: prod | Server: https://datahub.company.com]
{
  "urn": "...",
  # ...
}

# Or via environment variable
export DATAHUB_SHOW_PROFILE=true
```

**Benefits**: Prevents confusion, clearer audit trail.

### 8. SDK Profile Integration

**Use Case**: Python SDK users want profile support.

**Already Supported**:

```python
from datahub.sdk import DataHubClient

# Use profile in SDK
client = DataHubClient.from_env(profile="staging")
```

**Future Enhancement**:

```python
# List available profiles
profiles = DataHubClient.list_profiles()

# Switch profiles on existing client
client.switch_profile("prod")
```

**Benefits**: Unified experience across CLI and SDK.

### 9. Profile-Scoped Configuration

**Use Case**: Store profile-specific settings beyond connection details.

**Possible Implementation**:

```yaml
profiles:
  dev:
    server: http://localhost:8080
    token: ${DEV_TOKEN}
    settings:
      default_domain: "urn:li:domain:engineering"
      prefer_graphql: true
      log_level: debug
```

**Benefits**: Richer profile customization, better UX per environment.

### 10. Profile Analytics Dashboard

**Use Case**: Understand profile usage patterns within organization.

**Possible Implementation**:

- Web dashboard showing profile usage across team
- Most common profiles, operations per profile
- Error rates by profile
- Help identify optimization opportunities

**Benefits**: Better understanding of usage patterns, identify issues proactively.

### 11. Per-Repository DataHub Configuration

**Use Case**: Repositories may want to store additional DataHub configuration beyond the profile pointer — for example, default ingestion recipes, custom assertion rules, or metadata model extensions.

**Foundation**: The ancestor-walk model naturally supports this — `.datahub/` directories at any level can hold additional config. Future work could add:
- `.datahub/recipes/` for ingestion recipes
- `.datahub/assertions.yaml` for custom data quality rules
- `.datahub/metadata/` for local metadata definitions

Intermediate-level `.datahub/` directories (e.g., `~/work/.datahub/`) could hold company-wide defaults for recipes and assertions, just as they can hold company-wide profile configs today.

**Note**: This is explicitly out of scope for this RFC. The `.datahub/` directory currently contains only `config.yaml`.

**Benefits**: Standardized per-repo DataHub configuration, better CI/CD integration, shareable team configurations.

## Unresolved questions

### 1. Profile Name Validation

**Question**: Should we restrict profile names to certain characters or patterns?

**Current State**: No restrictions implemented yet.

**Options**:

- **Option A**: Allow any string (maximum flexibility)
- **Option B**: Restrict to alphanumeric + hyphens/underscores (DNS-like)
- **Option C**: Restrict to alphanumeric only (simplest)

**Recommendation**: Option B - Allows readable names like "prod-west-1" while preventing special characters that could cause shell issues.

**Decision Needed**: Before GA release.

---

### 2. Profile Storage Format

**Question**: Should we support formats other than YAML (JSON, TOML, etc.)?

**Current State**: YAML only.

**Pros of Additional Formats**:

- JSON: Machine-friendly, already used by some tools
- TOML: Gaining popularity, clear syntax

**Cons**:

- Increased complexity
- Multiple formats to document
- Format detection logic needed

**Recommendation**: Start with YAML only, add other formats if users request them. YAML is widely understood and supports comments.

**Decision Needed**: Can be deferred post-launch.

---

### 3. Profile Sharing Mechanism

**Question**: How should teams share profile templates?

**Current State**: Manual YAML file sharing (users can copy config.yaml).

**Options**:

- **Option A**: File-based sharing (current approach)
- **Option B**: Built-in import/export commands
- **Option C**: Central profile repository
- **Option D**: Git-based profile management

**Recommendation**: Option B for near-term, Option C/D as future work. Import/export commands provide good UX without infrastructure overhead.

**Decision Needed**: Can be added post-launch in future version.

---

### 4. Confirmation Prompt Customization

**Question**: Should users be able to customize confirmation prompts?

**Current State**: Fixed prompt format (type profile name in uppercase).

**Options**:

- **Option A**: Fixed format (current)
- **Option B**: Configurable prompt format per profile
- **Option C**: Custom confirmation messages
- **Option D**: Multiple confirmation levels (standard/strict)

**Recommendation**: Option A for launch, gather feedback. Fixed format is predictable and secure. Can add customization if users need it.

**Decision Needed**: Post-launch based on user feedback.

---

### 5. Legacy Config Deprecation Timeline

**Question**: When should we deprecate `~/.datahubenv` support?

**Current State**: Fully supported with no deprecation date.

**Considerations**:

- Need time for users to adopt profiles
- Breaking change requires major version bump
- Large enterprises may need years to migrate

**Recommendation**:

- Maintain support for at least 2 major versions (minimum 1-2 years)
- Start showing migration prompts in version 0.15.0 (6 months after launch)
- Re-evaluate deprecation timeline based on adoption metrics

**Decision Needed**: Can be decided 6-12 months post-launch.

---

### 6. Profile-Level Cache Settings

**Question**: Should each profile have independent cache settings?

**Current State**: `get_default_graph` uses `@lru_cache` without considering profile in cache key.

**Issue**: Switching profiles doesn't invalidate cache, could return stale config.

**Options**:

- **Option A**: Include profile name in cache key
- **Option B**: Clear cache on profile switch
- **Option C**: Per-profile cache instances
- **Option D**: No caching (simplest, possible performance impact)

**Recommendation**: Option A - Include profile name in cache key. Most transparent and correct behavior.

**Decision Needed**: Before GA release.

---

### 7. Multi-Profile Operations

**Question**: Should we support operations across multiple profiles?

**Example**: `datahub profile run-all -- get --urn "..."` runs command against all profiles.

**Current State**: Not supported.

**Use Cases**:

- Compare metadata across environments
- Bulk validation across all profiles
- Cross-environment reporting

**Recommendation**: Not in initial release. Add as future enhancement if users request it.

**Decision Needed**: Post-launch based on demand.

---

### 8. Profile Discovery and Autocomplete

**Question**: Should we support shell autocomplete for profile names?

**Current State**: No autocomplete support.

**Options**:

- **Option A**: Generate shell completion scripts (bash/zsh/fish)
- **Option B**: Manual autocomplete setup in docs
- **Option C**: No autocomplete (current)

**Recommendation**: Option A - Click supports autocomplete generation. Improves UX significantly.

**Decision Needed**: Nice-to-have for GA, can be added post-launch.

---

### 9. Profile Configuration Validation Hooks

**Question**: Should we allow custom validation logic for profiles?

**Example**: Organization-specific rules (e.g., "prod profiles must use corp.com domain").

**Current State**: Basic Pydantic validation only.

**Options**:

- **Option A**: Plugin system for custom validators
- **Option B**: Configuration file with validation rules
- **Option C**: No custom validation (current)

**Recommendation**: Option C for launch, Option B as future enhancement. Keep it simple initially.

**Decision Needed**: Post-launch based on enterprise feedback.

---

### 10. Environment Variable Naming Convention

**Question**: Should we recommend specific env var naming patterns?

**Current State**: No enforced convention, users choose their own names.

**Options**:

- **Option A**: Recommend pattern (e.g., `DATAHUB_<PROFILE>_TOKEN`)
- **Option B**: No recommendations (current)
- **Option C**: Auto-generate env var names

**Recommendation**: Option A - Document recommended patterns in best practices guide, but don't enforce.

**Decision Needed**: For documentation phase.

---

### 11. Should `datahub profile current` Show Resolution Chain?

**Question**: When displaying the active profile, should the command also show *why* it was selected (which precedence layer resolved it)?

**Example output**:

```
Active profile: staging
Resolved via: ancestor walk (/Users/me/worktree-a/.datahub/config.yaml)
Config type: profile pointer
Server: https://staging.datahub.com
```

**Recommendation**: Yes — this is invaluable for debugging, especially in agent/worktree workflows where the user did not explicitly pass `--profile`.

**Decision Needed**: Before GA release.

---

### 12. Where Should `datahub profile use` Write?

**Question**: Should `datahub profile use <name>` write `.datahub/config.yaml` to CWD, or try to find the nearest git root first?

**Options**:

- **Option A**: Always write to CWD (simplest, works everywhere)
- **Option B**: Write to git root if in a repo, CWD otherwise (more predictable for repos)

**Recommendation**: Option A — CWD is the simplest behavior and the ancestor walk naturally finds it. Users who want it at the repo root can `cd` there first. This keeps the implementation git-agnostic.

**Decision Needed**: Before implementation.

---

### 13. Should the Ancestor Walk Support a Stop Marker?

**Question**: Should a `.datahub/config.yaml` be able to declare itself as a "root" to prevent the walk from continuing above it?

**Example**:

```yaml
# Stop the ancestor walk here — don't look in parent directories
root: true
profiles:
  dev:
    server: http://localhost:8080
```

**Recommendation**: Not for v1. The walk already stops at `~`, and the "closest wins" semantic is simple enough. Add a stop marker if users request it.

**Decision Needed**: Post-launch based on user feedback.

---

These questions can be resolved during implementation, beta testing, or post-launch based on user feedback. None are blockers for the initial release.
