#!/usr/bin/env python3
"""Fail the frontend preview build if any source map exceeds Cloudflare Pages' per-file limit.

Cloudflare Pages rejects any single file over 25 MiB (26,214,400 bytes — marketed as "26.2 MB"
using decimal MB), which silently breaks the preview deploy after merge. That preview is the
environment Meticulous records against, so a rejected *.js.map means no source coverage. This
guards on the PR instead: hard-fail if any map reaches the limit, warn earlier so a chunk can be
split (see manualChunks in datahub-web-react/vite.config.ts) before it ever hard-fails.

Comparisons are in BYTES against Cloudflare's exact ceiling — do NOT use 26.2 * 1024 * 1024, which
is ~1.26 MB too generous and would pass files Cloudflare then rejects.
"""

import glob
import os
import sys
from typing import List

HARD_LIMIT = 26_214_400  # Cloudflare Pages per-file hard limit (25 MiB), in bytes
WARN_LIMIT = int(HARD_LIMIT * 0.9)  # early-warning threshold (~90% of the hard limit)
MIB = 1024 * 1024

DEFAULT_MAPS_GLOB = "datahub-web-react/dist/assets/*.map"


def resolve_maps(args: List[str]) -> List[str]:
    """Resolve the map files from CLI args.

    Accepts either a single glob pattern (when the caller quotes it) or a list of already
    shell-expanded file paths (when the caller leaves the glob unquoted). Every arg is run through
    glob.glob so a literal path resolves to itself and an unmatched pattern resolves to nothing;
    this makes the guard behave identically regardless of how the workflow quotes the argument.
    """
    matched: List[str] = []
    for arg in args:
        matched.extend(glob.glob(arg))
    # De-dupe while sorting largest-first, so the summary lists the biggest map at the top.
    return sorted(set(matched), key=os.path.getsize, reverse=True)


def main(args: List[str]) -> int:
    maps = resolve_maps(args or [DEFAULT_MAPS_GLOB])
    if not maps:
        # The build ran with -Psourcemap, so maps must exist here. None means generation silently
        # broke (e.g. a stale build cache) — fail loudly rather than skip, since a mapless deploy is
        # exactly the Meticulous-coverage regression this guards against.
        print(
            "::error title=No source maps generated::The -Psourcemap build produced no "
            f"*.js.map matching {' '.join(args) or DEFAULT_MAPS_GLOB}. "
            "Source-map generation is broken."
        )
        print("FAIL: expected source maps after a -Psourcemap build, found none.")
        return 1

    over, warn = [], []
    for m in maps:
        size = os.path.getsize(m)
        name = os.path.basename(m)
        print(f"  {size / MIB:6.2f} MiB  {name}")
        if size >= HARD_LIMIT:
            over.append((name, size))
        elif size >= WARN_LIMIT:
            warn.append((name, size))

    # GitHub Actions annotations surface in the PR "Files changed" / checks UI.
    for name, size in warn:
        print(
            "::warning title=Sourcemap approaching Cloudflare limit::"
            f"{name} is {size / MIB:.2f} MiB (warn >= {WARN_LIMIT / MIB:.2f} MiB, hard limit "
            f"{HARD_LIMIT / MIB:.2f} MiB). Split a chunk in vite.config.ts manualChunks soon."
        )

    if over:
        for name, size in over:
            print(
                "::error title=Sourcemap over Cloudflare limit::"
                f"{name} is {size / MIB:.2f} MiB ({size} bytes), exceeding the Cloudflare Pages "
                f"{HARD_LIMIT / MIB:.2f} MiB ({HARD_LIMIT} bytes) per-file limit. Split this chunk "
                "in vite.config.ts manualChunks."
            )
        print(
            f"\nFAIL: {len(over)} source map(s) exceed the {HARD_LIMIT / MIB:.2f} MiB "
            "Cloudflare Pages limit."
        )
        return 1

    biggest = os.path.getsize(maps[0])
    print(
        f"\nOK: all {len(maps)} source maps under {HARD_LIMIT / MIB:.2f} MiB "
        f"(largest {biggest / MIB:.2f} MiB, {(HARD_LIMIT - biggest) / MIB:.2f} MiB headroom)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
