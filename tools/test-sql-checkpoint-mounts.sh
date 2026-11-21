#!/usr/bin/env bash
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

# Execute the supplied test command with two bind mounts in a private namespace.
set -euo pipefail

if [[ "$(uname -s)" != Linux ]]; then
    echo "SQL checkpoint mount qualification requires Linux" >&2
    exit 1
fi
for tool in unshare mount umount findmnt stat setpriv mktemp realpath; do
    command -v "$tool" >/dev/null || { echo "Missing prerequisite: $tool" >&2; exit 1; }
done

if [[ "${1:-}" != --in-namespace ]]; then
    if (( $# == 0 )); then
        echo "Usage: bash tools/test-sql-checkpoint-mounts.sh <test-command> [args...]" >&2
        exit 1
    fi
    script=$(realpath "$0")
    fixture=$(mktemp -d /tmp/otel-arrow-sql-mounts.XXXXXXXX)
    trap 'rm -rf -- "$fixture"' EXIT
    mkdir "$fixture/backing" "$fixture/primary" "$fixture/alias"
    privilege=()
    if (( EUID != 0 )); then
        command -v sudo >/dev/null || { echo "Missing prerequisite: sudo" >&2; exit 1; }
        privilege=(sudo -n)
    fi
    "${privilege[@]}" unshare --mount --propagation private \
        bash "$script" --in-namespace "$(id -u)" "$(id -g)" "$fixture" "$HOME" "$PATH" "$@"
    exit
fi

shift
uid=$1
gid=$2
fixture=$3
user_home=$4
user_path=$5
shift 5
primary="$fixture/primary"
alias="$fixture/alias"
primary_mounted=false
alias_mounted=false
cleanup() {
    status=$?
    if "$alias_mounted"; then umount -- "$alias" || status=1; fi
    if "$primary_mounted"; then umount -- "$primary" || status=1; fi
    exit "$status"
}
trap cleanup EXIT

mount --bind "$fixture/backing" "$primary"
primary_mounted=true
mount --bind "$fixture/backing" "$alias"
alias_mounted=true
primary_id=$(findmnt --noheadings --output ID --mountpoint "$primary")
alias_id=$(findmnt --noheadings --output ID --mountpoint "$alias")
[[ "$primary_id" != "$alias_id" ]] || { echo "Expected two distinct mount IDs" >&2; exit 1; }
[[ "$(stat -c '%d:%i' "$primary")" == "$(stat -c '%d:%i' "$alias")" ]] || {
    echo "Mounts do not share a backing directory" >&2
    exit 1
}
echo "SQL checkpoint mount qualification: primary ID=$primary_id, alias ID=$alias_id"
findmnt --noheadings --output ID,TARGET,SOURCE,FSROOT,PROPAGATION --mountpoint "$primary"
findmnt --noheadings --output ID,TARGET,SOURCE,FSROOT,PROPAGATION --mountpoint "$alias"
stat -c 'backing identity %d:%i %n' "$fixture/backing" "$primary" "$alias"

# Only mount setup runs privileged; preserve the caller's user and tool paths.
setpriv --reuid "$uid" --regid "$gid" --init-groups \
    env HOME="$user_home" PATH="$user_path" \
    OTAP_SCRAPER_TEST_STATE_ROOT="$primary" OTAP_SCRAPER_TEST_STATE_ALIAS="$alias" \
    "$@"
