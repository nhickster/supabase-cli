#!/bin/sh
set -eu

# migra doesn't shutdown gracefully, so kill it ourselves
trap 'kill -9 %1' TERM

run_migra() {
    # additional flags for diffing extensions
    [ "$schema" = "extensions" ] && set -- --create-extensions-only --ignore-extension-versions "$@"
    
    # For user schemas (especially 'public'), don't use --with-privileges to avoid spurious REVOKE statements
    # See: https://github.com/supabase/cli/issues/4068
    # Managed schemas (auth, storage, etc.) still get privilege checking via TypeScript migra
    if [ "$schema" = "public" ]; then
        echo "[DEBUG migra.sh] Running migra for schema: $schema (WITHOUT --with-privileges)" >&2
        echo "[DEBUG migra.sh] SOURCE: $SOURCE" >&2
        echo "[DEBUG migra.sh] TARGET: $TARGET" >&2
        migra --unsafe --schema="$schema" "$@"
    else
        echo "[DEBUG migra.sh] Running migra for schema: $schema (with --with-privileges)" >&2
        echo "[DEBUG migra.sh] SOURCE: $SOURCE" >&2
        echo "[DEBUG migra.sh] TARGET: $TARGET" >&2
        migra --with-privileges --unsafe --schema="$schema" "$@"
    fi
}

# accepts command line args as a list of schema to generate
for schema in "$@"; do
    # migra exits 2 when differences are found
    run_migra "$SOURCE" "$TARGET" || status=$?
    if [ ${status:-2} -ne 2 ]; then
        exit $status
    fi
done
