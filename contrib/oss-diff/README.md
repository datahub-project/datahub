# OSS Diff Tool

The oss-diff tool is used for ensuring that the SaaS codebase is kept reasonably in sync with OSS, which ensures that we do OSS-first development where possible and avoid merge conflicts.

## Installation

1. Install `uv`

   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Run the tool

   ```bash
   ❯ ./oss-diff.py --help

   Usage: oss-diff.py [OPTIONS] COMMAND [ARGS]...

   ╭─ Options ───────────────────────────────────────────────────────────────────────────────╮
   │ --install-completion          Install completion for the current shell.                 │
   │ --show-completion             Show completion for the current shell, to copy it or      │
   │                               customize the installation.                               │
   │ --help                        Show this message and exit.                               │
   ╰─────────────────────────────────────────────────────────────────────────────────────────╯
   ╭─ Commands ──────────────────────────────────────────────────────────────────────────────╮
   │ check                                                                                   │
   │ show-diff                                                                               │
   │ restore-from-oss                                                                        │
   │ ui                                                                                      │
   ╰─────────────────────────────────────────────────────────────────────────────────────────╯
   ```

We're using `uv` and "inline script metadata" to declare the dependencies for the script inline, which means that you don't need any extra setup / deps / venvs / etc.

## How it works

There are a bunch of rules in `oss-diff-rules.yml` that define what areas of the codebase are allowed to diverge between OSS and SaaS, and by how much. These rules are manually curated.

Check conformance with the rules:

```bash
./oss-diff.py check
```

For code that violates these rules, the `oss-diff-exceptions.json` file defines exceptions to the defined rules. This file can be automatically updated using the `--loosen` and `--tighten` flags.

```bash
./oss-diff.py check --loosen
./oss-diff.py check --tighten
```

## UI

The tool also has a UI that can be used to peruse the files/diffs that violate the rules.

```bash
./oss-diff.py ui
```

## Demo

<div>
  <a href="https://www.loom.com/share/d5c6856110854b8e886147a59d25eee1">
    <p>OSS Diff Tool Overview 🚀 - Watch Loom Demo</p>
  </a>
  <a href="https://www.loom.com/share/d5c6856110854b8e886147a59d25eee1">
    <img style="max-width:300px;" src="https://cdn.loom.com/sessions/thumbnails/d5c6856110854b8e886147a59d25eee1-bc4b7fca00a3a379-full-play.gif">
  </a>
</div>
