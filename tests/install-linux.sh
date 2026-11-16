#!/bin/sh
set -eu

test_home=$(mktemp -d)
trap 'rm -rf "$test_home"' EXIT
export HOME="$test_home"
unset XDG_CONFIG_HOME
export MSSH_RELEASE_URL="file://$(pwd)/${MSSH_TEST_BINARY:-target/release/mssh}"
export MSSH_EXAMPLE_URL="file://$(pwd)/examples/config.toml"

sh scripts/install.sh
test -x "$HOME/.local/bin/mssh"
test -f "$HOME/.config/mssh/config.toml"
"$HOME/.local/bin/mssh" --version

printf 'custom config\n' > "$HOME/.config/mssh/config.toml"
sh scripts/install.sh
test "$(cat "$HOME/.config/mssh/config.toml")" = 'custom config'
echo 'Linux installer test passed'
