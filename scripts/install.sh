#!/bin/sh
set -eu

repo=KirillShakhov/HopMux
case "$(uname -s):$(uname -m)" in
    Linux:x86_64|Linux:amd64) asset=mssh-linux-x86_64 ;;
    *) echo "mssh: this installer supports Linux x86_64" >&2; exit 1 ;;
esac

: "${HOME:?HOME must be set}"
install_dir=${MSSH_INSTALL_DIR:-"$HOME/.local/bin"}
config_dir=${MSSH_CONFIG_DIR:-"${XDG_CONFIG_HOME:-$HOME/.config}/mssh"}
release_url=${MSSH_RELEASE_URL:-"https://github.com/$repo/releases/latest/download/$asset"}
example_url=${MSSH_EXAMPLE_URL:-"https://raw.githubusercontent.com/$repo/main/examples/config.toml"}

umask 077
mkdir -p "$install_dir" "$config_dir"
binary_temp=$(mktemp "$install_dir/.mssh-download.XXXXXX")
config_temp=
trap 'rm -f "$binary_temp" ${config_temp:+"$config_temp"}' EXIT

if [ ! -e "$config_dir/config.toml" ]; then
    config_temp=$(mktemp "$config_dir/.config-download.XXXXXX")
    curl -fL --retry 3 --connect-timeout 10 "$example_url" -o "$config_temp"
    test -s "$config_temp" || { echo "mssh: downloaded example config is empty" >&2; exit 1; }
    if ln "$config_temp" "$config_dir/config.toml" 2>/dev/null; then
        echo "Created $config_dir/config.toml from the main branch example"
    fi
fi

curl -fL --retry 3 --connect-timeout 10 "$release_url" -o "$binary_temp"
test -s "$binary_temp" || { echo "mssh: downloaded binary is empty" >&2; exit 1; }
chmod 755 "$binary_temp"
"$binary_temp" --version >/dev/null
mv -f "$binary_temp" "$install_dir/mssh"
echo "Installed $("$install_dir/mssh" --version) to $install_dir/mssh"

if [ "$install_dir" = "$HOME/.local/bin" ]; then
    path_line='case ":$PATH:" in *":$HOME/.local/bin:"*) ;; *) export PATH="$HOME/.local/bin:$PATH";; esac'
    case "${SHELL:-}" in
        */zsh) shell_profile="$HOME/.zshrc" ;;
        *) shell_profile="$HOME/.bashrc" ;;
    esac
    for profile in "$HOME/.profile" "$shell_profile"; do
        if ! grep -Fq '# mssh user bin' "$profile" 2>/dev/null; then
            printf '\n# mssh user bin\n%s\n' "$path_line" >> "$profile"
        fi
    done
    case ":$PATH:" in
        *":$install_dir:"*) ;;
        *)
            echo 'Open a new shell, or run: export PATH="$HOME/.local/bin:$PATH"'
            ;;
    esac
else
    case ":$PATH:" in
        *":$install_dir:"*) ;;
        *) echo "Add $install_dir to PATH to run mssh from any directory" ;;
    esac
fi
