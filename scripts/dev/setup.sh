#!/usr/bin/env bash
# Sets up a new development environment for DataHub.
#
# Can be run BEFORE cloning the repository — does not depend on mise.toml.
#
# Installs (via mise):  Java 21, Python 3.11, Node 22, Yarn 1.22.22
# Installs separately:  uv (required by datahub-dev.sh), Docker + Docker Compose
#
# Usage: curl -fsSL <url>/scripts/dev/setup.sh | bash
#        scripts/dev/setup.sh [--no-docker]
#
# Supports: Ubuntu/Debian, macOS (Homebrew)
set -euo pipefail

# Tool versions — kept in sync with mise.toml
JAVA_VERSION="21"
PYTHON_VERSION="3.11"
NODE_VERSION="22"
YARN_VERSION="1.22.22"

NO_DOCKER=false

for arg in "$@"; do
    case "$arg" in
        --no-docker) NO_DOCKER=true ;;
        -h|--help)
            echo "Usage: setup.sh [--no-docker]"
            echo ""
            echo "Options:"
            echo "  --no-docker   Skip Docker installation"
            exit 0
            ;;
        *) echo "Unknown argument: $arg" >&2; exit 1 ;;
    esac
done

# ── helpers ──────────────────────────────────────────────────────────────────

info()    { echo "[setup] $*"; }
success() { echo "[setup] ✓ $*"; }
warn()    { echo "[setup] ⚠ $*" >&2; }
die()     { echo "[setup] ✗ $*" >&2; exit 1; }

os_type() {
    case "$(uname -s)" in
        Linux*)  echo linux ;;
        Darwin*) echo macos ;;
        *)       die "Unsupported OS: $(uname -s)" ;;
    esac
}

is_linux() { [[ "$(os_type)" == linux ]]; }
is_macos() { [[ "$(os_type)" == macos ]]; }

command_exists() { command -v "$1" &>/dev/null; }

# ── package manager bootstrap ─────────────────────────────────────────────────

ensure_homebrew() {
    if command_exists brew; then
        success "Homebrew already installed"
        return
    fi
    info "Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    # Add brew to PATH for the rest of this script
    if [[ -x /opt/homebrew/bin/brew ]]; then
        eval "$(/opt/homebrew/bin/brew shellenv)"
    fi
    success "Homebrew installed"
}

ensure_apt_deps() {
    info "Installing system dependencies via apt..."
    sudo apt-get update -qq
    sudo apt-get install -y -qq \
        curl wget git build-essential \
        libssl-dev zlib1g-dev libbz2-dev \
        libreadline-dev libsqlite3-dev \
        libncursesw5-dev xz-utils libxml2-dev \
        libxmlsec1-dev libffi-dev liblzma-dev \
        ca-certificates gnupg
    success "System dependencies installed"
}

# ── mise ──────────────────────────────────────────────────────────────────────

install_mise() {
    if command_exists mise; then
        success "mise already installed ($(mise --version))"
        return
    fi
    info "Installing mise..."
    curl -fsSL https://mise.run | sh
    # mise installs to ~/.local/bin/mise — add to PATH for this session
    export PATH="$HOME/.local/bin:$PATH"
    success "mise installed ($(mise --version))"
}

install_mise_tools() {
    info "Installing tools via mise: Java $JAVA_VERSION, Python $PYTHON_VERSION, Node $NODE_VERSION, Yarn $YARN_VERSION..."
    # --global installs into ~/.local/share/mise — no repo checkout needed
    mise use --global "java@$JAVA_VERSION"
    mise use --global "python@$PYTHON_VERSION"
    mise use --global "node@$NODE_VERSION"
    mise use --global "yarn@$YARN_VERSION"
    success "mise tools installed"
    mise ls --current
}

# ── uv ───────────────────────────────────────────────────────────────────────

install_uv() {
    if command_exists uv; then
        success "uv already installed ($(uv --version))"
        return
    fi
    info "Installing uv..."
    curl -fsSL https://astral.sh/uv/install.sh | sh
    # uv installs to ~/.local/bin/uv
    export PATH="$HOME/.local/bin:$PATH"
    success "uv installed ($(uv --version))"
}

# ── Docker ───────────────────────────────────────────────────────────────────

install_docker_linux() {
    if command_exists docker; then
        success "Docker already installed ($(docker --version))"
        return
    fi
    info "Installing Docker Engine..."

    # Official Docker install script (supports Ubuntu, Debian, Fedora, RHEL, etc.)
    curl -fsSL https://get.docker.com | sh

    # Add current user to the docker group so sudo isn't needed
    if ! groups | grep -q docker; then
        sudo usermod -aG docker "$USER"
        warn "Added $USER to the 'docker' group. Log out and back in (or run 'newgrp docker') for this to take effect."
    fi
    success "Docker Engine installed"
}

install_docker_compose_linux() {
    # Docker Engine ships with the Compose plugin (docker compose); check for it.
    if docker compose version &>/dev/null 2>&1; then
        success "Docker Compose already installed ($(docker compose version))"
        return
    fi
    info "Installing Docker Compose plugin..."
    COMPOSE_VERSION="$(curl -fsSL https://api.github.com/repos/docker/compose/releases/latest | grep '"tag_name"' | sed 's/.*"v\([^"]*\)".*/\1/')"
    sudo mkdir -p /usr/local/lib/docker/cli-plugins
    sudo curl -fsSL "https://github.com/docker/compose/releases/download/v${COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" \
        -o /usr/local/lib/docker/cli-plugins/docker-compose
    sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
    success "Docker Compose installed ($(docker compose version))"
}

install_docker_macos() {
    if command_exists docker; then
        success "Docker already installed"
        return
    fi
    info "Installing Docker Desktop via Homebrew..."
    brew install --cask docker
    warn "Open Docker Desktop at least once to complete installation before using 'docker' commands."
    success "Docker Desktop installed"
}

install_docker() {
    if $NO_DOCKER; then
        warn "Skipping Docker installation (--no-docker)"
        return
    fi
    if is_linux; then
        install_docker_linux
        install_docker_compose_linux
    else
        install_docker_macos
    fi
}

# ── shell profile hints ───────────────────────────────────────────────────────

print_shell_setup() {
    echo ""
    echo "──────────────────────────────────────────────────────────"
    echo " Post-install steps"
    echo "──────────────────────────────────────────────────────────"
    echo ""
    echo "1. Add these lines to your shell profile (~/.bashrc, ~/.zshrc, etc.)"
    echo "   if they aren't already present:"
    echo ""
    echo '   # mise'
    echo '   export PATH="$HOME/.local/bin:$PATH"'
    echo '   eval "$(mise activate bash)"   # or: mise activate zsh'
    echo ""
    echo '   # uv'
    echo '   export PATH="$HOME/.local/bin:$PATH"'
    echo ""
    echo "2. Reload your shell:"
    echo "   source ~/.bashrc   # or ~/.zshrc"
    echo ""
    if is_linux && ! groups | grep -q docker; then
        echo "3. Apply the docker group change without logging out:"
        echo "   newgrp docker"
        echo ""
    fi
    echo "Then verify the setup:"
    echo "   java --version"
    echo "   python3 --version"
    echo "   node --version"
    echo "   yarn --version"
    echo "   uv --version"
    echo "   docker --version"
    echo ""
    echo "Clone and start DataHub:"
    echo "   git clone https://github.com/datahub-project/datahub.git && cd datahub"
    echo "   scripts/dev/datahub-dev.sh start"
    echo "──────────────────────────────────────────────────────────"
}

# ── main ──────────────────────────────────────────────────────────────────────

main() {
    info "Setting up DataHub development environment on $(os_type)..."
    echo ""

    if is_linux; then
        ensure_apt_deps
    else
        ensure_homebrew
    fi

    install_mise
    install_mise_tools
    install_uv
    install_docker

    echo ""
    success "All tools installed successfully!"
    print_shell_setup
}

main
