#!/bin/sh
# Remove pip/uv packages and launchers from Python venvs (locked / hardened image builds).
# Shared snippet: use from Dockerfiles via COPY, then `sh /path/strip_pip_uv_from_venvs.sh`.
# Kept as a .sh file so RUN does not treat $v / $sp as empty Docker env vars.

strip_venv() {
    v="$1"
    if [ ! -d "$v" ] || [ ! -x "$v/bin/python" ]; then
        return 0
    fi
    ("$v/bin/python" -m pip uninstall -y pip) 2>/dev/null || true
    for b in "$v/bin/uv" "$v/bin/uvx" "$v/bin/pip" "$v/bin/pip3"; do
        [ -e "$b" ] && rm -f "$b"
    done
    find "$v/bin" -maxdepth 1 -name 'pip3.*' -delete 2>/dev/null || true
    for sp in "$v"/lib/python*/site-packages; do
        [ -d "$sp" ] || continue
        find "$sp" -mindepth 1 -maxdepth 1 -name 'pip' -exec rm -rf {} + 2>/dev/null || true
        find "$sp" -mindepth 1 -maxdepth 1 -name 'uv' -exec rm -rf {} + 2>/dev/null || true
        find "$sp" -mindepth 1 -maxdepth 1 -name 'pip-*.dist-info' -exec rm -rf {} + 2>/dev/null || true
        find "$sp" -mindepth 1 -maxdepth 1 -name 'uv-*.dist-info' -exec rm -rf {} + 2>/dev/null || true
        find "$sp" -mindepth 1 -maxdepth 1 -name 'pip-*.data' -exec rm -rf {} + 2>/dev/null || true
    done
    rm -rf "$v/.cache/pip" "$v/.cache/uv" 2>/dev/null || true
}

strip_venv /home/datahub/.venv
if [ -d /opt/datahub/venvs ]; then
    for d in /opt/datahub/venvs/*; do
        [ -d "$d" ] && strip_venv "$d" || true
    done
fi

rm -rf /home/datahub/.cache/pip /home/datahub/.cache/uv \
    /home/datahub/.local/share/uv /home/datahub/.local/pip \
    /home/datahub/.config/pip /home/datahub/.config/uv 2>/dev/null || true

if [ -d /home/datahub/.local ]; then
    find /home/datahub/.local -type d -name 'site-packages' 2>/dev/null | while IFS= read -r sproot; do
        [ -d "$sproot" ] || continue
        find "$sproot" -mindepth 1 -maxdepth 1 -name 'pip' -exec rm -rf {} + 2>/dev/null || true
        find "$sproot" -mindepth 1 -maxdepth 1 -name 'uv' -exec rm -rf {} + 2>/dev/null || true
        find "$sproot" -mindepth 1 -maxdepth 1 -name 'pip-*.dist-info' -exec rm -rf {} + 2>/dev/null || true
        find "$sproot" -mindepth 1 -maxdepth 1 -name 'uv-*.dist-info' -exec rm -rf {} + 2>/dev/null || true
        find "$sproot" -mindepth 1 -maxdepth 1 -name 'pip-*.data' -exec rm -rf {} + 2>/dev/null || true
    done
fi
