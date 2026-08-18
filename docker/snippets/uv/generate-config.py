#!/usr/bin/env python3
"""Generate uv.toml for the 'custom' (from-scratch) profile, or layer extra
indexes onto an existing profile. Used by the Python 3.11-based images
(datahub-actions) where tomllib is available.

Env vars (URLs are creds-free; credentials live in ~/.netrc):
  UV_PROFILE        'custom' (from-scratch) or an existing profile name.
  BASE_PROFILE      Alias of UV_PROFILE (passed by the Dockerfile).
  DEFAULT_INDEX_URL Default index URL (custom only; ignored for existing).
  EXTRA_INDEX_URLS  Space-separated extra index URLs (both cases).
  PROFILES_DIR      Directory holding <name>.toml base profiles.

Writes uv.toml to stdout; caller redirects to $HOME/.config/uv/uv.toml.
"""
import os
import sys
import tomllib

INDEX_STRATEGY = "unsafe-best-match"


def emit(indexes, strategy):
    out = []
    if strategy:
        out.append(f'index-strategy = "{strategy}"')
    for ix in indexes:
        out.append("")
        out.append("[[index]]")
        out.append(f'name = "{ix["name"]}"')
        out.append(f'url = "{ix["url"]}"')
        if ix.get("default"):
            out.append("default = true")
        if ix.get("authenticate"):
            out.append(f'authenticate = "{ix["authenticate"]}"')
    return "\n".join(out) + "\n"


def main():
    uv_profile = os.environ.get("UV_PROFILE") or os.environ.get("BASE_PROFILE") or ""
    profiles_dir = os.environ.get("PROFILES_DIR", "")
    default_url = os.environ.get("DEFAULT_INDEX_URL", "").strip()
    extras = [u for u in os.environ.get("EXTRA_INDEX_URLS", "").split() if u]

    base_file = os.path.join(profiles_dir, f"{uv_profile}.toml") if profiles_dir else ""
    # "custom" is an EXPLICIT choice — a missing profile file is NOT inferred as
    # custom. An unknown profile (not "custom" and no file) is rejected so typos
    # surface here instead of silently becoming a from-scratch build. This
    # mirrors gradle's resolveUvBuildArgs, which the Dockerfiles bypass when
    # they invoke this script directly (e.g. the Depot/direct-docker path).
    if uv_profile == "custom":
        is_custom = True
    elif base_file and os.path.isfile(base_file):
        is_custom = False
    else:
        reason = "PROFILES_DIR is unset" if not base_file else f"no profile file at {base_file}"
        sys.exit(
            f"generate-config.py: unknown UV_PROFILE '{uv_profile}' ({reason}). "
            "Set UV_PROFILE to 'custom' or an existing profile (default/chainguard/chainguard-ci)."
        )

    if is_custom:
        if not default_url:
            sys.exit("generate-config.py: DEFAULT_INDEX_URL is required for the custom profile")
        indexes = [{"name": "custom-default", "url": default_url, "default": True}]
        strategy = INDEX_STRATEGY
    else:
        with open(base_file, "rb") as fh:
            data = tomllib.load(fh)
        indexes = list(data.get("index", []))
        strategy = data.get("index-strategy") or INDEX_STRATEGY

    existing_urls = {ix.get("url") for ix in indexes}
    for i, url in enumerate(extras, 1):
        if url in existing_urls:
            continue
        indexes.append({"name": f"extra-{i}", "url": url})
        existing_urls.add(url)

    sys.stdout.write(emit(indexes, strategy))


if __name__ == "__main__":
    main()
