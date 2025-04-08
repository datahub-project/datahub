#!/usr/bin/env bash

prefix="datahub-web-react/"

trimmed_paths=()
for path in "$@"; do
  trimmed_paths+=("${path#"$prefix"}")
done

cd "$(dirname "$0")" || exit 1
yarn run eslint-base --fix "${trimmed_paths[@]}" && yarn run format-base "${trimmed_paths[@]}"
