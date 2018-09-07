#!/usr/bin/env nix-shell
#!nix-shell -i bash -p clang-tools

set -eu
for i in *.cc *.h; do
  echo "$i"
  clang-format "$i" > "$i.tmp"
  mv "$i.tmp" "$i"
done
