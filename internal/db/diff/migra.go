package diff

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"os"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/go-errors/errors"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/spf13/viper"
	"github.com/supabase/cli/internal/gen/types"
	"github.com/supabase/cli/internal/utils"
	"github.com/supabase/cli/pkg/config"
	"github.com/supabase/cli/pkg/migration"
)

var (
	//go:embed templates/migra.sh
	diffSchemaScript string
	//go:embed templates/migra.ts
	diffSchemaTypeScript string

	managedSchemas = []string{
		// Local development
		"_analytics",
		"_realtime",
		"_supavisor",
		// Owned by extensions
		"cron",
		"graphql",
		"graphql_public",
		"net",
		"pgroonga",
		"pgtle",
		"repack",
		"tiger_data",
		"vault",
		// Deprecated extensions
		"pgsodium",
		"pgsodium_masks",
		"timescaledb_experimental",
		"timescaledb_information",
		"_timescaledb_cache",
		"_timescaledb_catalog",
		"_timescaledb_config",
		"_timescaledb_debug",
		"_timescaledb_functions",
		"_timescaledb_internal",
		// Managed by Supabase
		"pgbouncer",
		"supabase_functions",
		"supabase_migrations",
	}
)

// filterCosmeticFunctionChanges removes cosmetic-only function CREATE OR REPLACE statements
// while preserving legitimate schema changes. This handles both pure function-only migrations
// and mixed migrations containing both function changes and real schema alterations.
func filterCosmeticFunctionChanges(migration string) (filtered string, hadCosmetic bool) {
	if migration == "" {
		return "", false
	}

	lines := strings.Split(migration, "\n")
	var resultLines []string
	inFunction := false
	skipNextSemicolon := false
	var currentFunction []string
	cosmeticCount := 0
	realStatementCount := 0

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		trimmed := strings.TrimSpace(line)
		upperLine := strings.ToUpper(trimmed)

		// Skip the semicolon after a function end marker
		if skipNextSemicolon && trimmed == ";" {
			skipNextSemicolon = false
			continue
		}

		// Track if we're entering a function definition
		if !inFunction && (strings.HasPrefix(upperLine, "CREATE OR REPLACE FUNCTION") ||
			strings.HasPrefix(upperLine, "CREATE FUNCTION")) {
			inFunction = true
			currentFunction = []string{line}
			continue
		}

		// If we're in a function, accumulate lines until we find the end
		if inFunction {
			currentFunction = append(currentFunction, line)

			// Check if this is the end marker (these appear on their own line)
			if trimmed == "$function$" || trimmed == "$$" || trimmed == "$BODY$" {
				// Next line should be a semicolon - skip that too
				inFunction = false
				skipNextSemicolon = true
				currentFunction = nil
				cosmeticCount++
				continue
			}
			continue
		}

		// Not in a function - check if this is a real schema change
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			// Preserve empty lines and comments
			resultLines = append(resultLines, line)
			continue
		}

		// Check for real DDL statements (not functions)
		if strings.HasPrefix(upperLine, "ALTER TABLE") ||
			strings.HasPrefix(upperLine, "CREATE TABLE") ||
			strings.HasPrefix(upperLine, "DROP TABLE") ||
			strings.HasPrefix(upperLine, "CREATE INDEX") ||
			strings.HasPrefix(upperLine, "DROP INDEX") ||
			strings.HasPrefix(upperLine, "ALTER INDEX") ||
			strings.HasPrefix(upperLine, "CREATE VIEW") ||
			strings.HasPrefix(upperLine, "DROP VIEW") ||
			strings.HasPrefix(upperLine, "ALTER VIEW") ||
			strings.HasPrefix(upperLine, "CREATE TRIGGER") ||
			strings.HasPrefix(upperLine, "DROP TRIGGER") ||
			strings.HasPrefix(upperLine, "CREATE POLICY") ||
			strings.HasPrefix(upperLine, "DROP POLICY") ||
			strings.HasPrefix(upperLine, "ALTER POLICY") ||
			strings.HasPrefix(upperLine, "GRANT") ||
			strings.HasPrefix(upperLine, "REVOKE") ||
			strings.HasPrefix(upperLine, "CREATE TYPE") ||
			strings.HasPrefix(upperLine, "DROP TYPE") ||
			strings.HasPrefix(upperLine, "ALTER TYPE") ||
			strings.HasPrefix(upperLine, "CREATE SEQUENCE") ||
			strings.HasPrefix(upperLine, "DROP SEQUENCE") ||
			strings.HasPrefix(upperLine, "ALTER SEQUENCE") ||
			strings.HasPrefix(upperLine, "CREATE SCHEMA") ||
			strings.HasPrefix(upperLine, "DROP SCHEMA") ||
			strings.HasPrefix(upperLine, "ALTER SCHEMA") {
			realStatementCount++
			resultLines = append(resultLines, line)
			continue
		}

		// Preserve any other non-empty lines (might be part of multi-line statements)
		resultLines = append(resultLines, line)
	}

	filtered = strings.Join(resultLines, "\n")
	filtered = strings.TrimSpace(filtered)

	return filtered, cosmeticCount > 0
}

// Diffs local database schema against shadow, dumps output to stdout.
func DiffSchemaMigraBash(ctx context.Context, source, target pgconn.Config, schema []string, options ...func(*pgx.ConnConfig)) (string, error) {
	// Load all user defined schemas
	if len(schema) == 0 {
		var err error
		if schema, err = loadSchema(ctx, target, options...); err != nil {
			return "", err
		}
	}
	sourceURL := utils.ToPostgresURL(source)
	targetURL := utils.ToPostgresURL(target)

	// Get search_path from TARGET (remote) database to ensure consistent pg_get_expr() normalization
	// We want both databases to be queried with the remote's search_path
	searchPath := ""
	if targetConn, err := utils.ConnectByConfig(ctx, target, options...); err == nil {
		targetConn.QueryRow(ctx, "SHOW search_path").Scan(&searchPath)
		targetConn.Close(ctx)
	}

	env := []string{
		"SOURCE=" + sourceURL,
		"TARGET=" + targetURL,
	}

	// Set PGOPTIONS to force search_path for psycopg2 connections (migra)
	// This ensures pg_get_expr() normalizes column defaults consistently
	if searchPath != "" {
		// Remove quotes from PostgreSQL's formatted output ("$user", public -> $user, public)
		searchPath = strings.ReplaceAll(searchPath, "\"", "")
		// Remove spaces after commas - PostgreSQL search_path requires comma-separated without spaces
		searchPath = strings.ReplaceAll(searchPath, ", ", ",")
		pgoptions := fmt.Sprintf("-c search_path=%s", searchPath)
		env = append(env, "PGOPTIONS="+pgoptions)
		fmt.Fprintf(os.Stderr, "[DEBUG migra.go] Setting PGOPTIONS=%s for migra\n", pgoptions)
	}
	// Passing in script string means command line args must be set manually, ie. "$@"
	args := "set -- " + strings.Join(schema, " ") + ";"
	cmd := []string{"/bin/sh", "-c", args + diffSchemaScript}
	var out, stderr bytes.Buffer
	if err := utils.DockerRunOnceWithConfig(
		ctx,
		container.Config{
			Image: config.Images.Migra,
			Env:   env,
			Cmd:   cmd,
		},
		container.HostConfig{
			NetworkMode: network.NetworkHost,
		},
		network.NetworkingConfig{},
		"",
		&out,
		&stderr,
	); err != nil {
		return "", errors.Errorf("error diffing schema: %w:\n%s", err, stderr.String())
	}

	// Post-process to filter out cosmetic function changes
	output := out.String()
	filtered, hadCosmetic := filterCosmeticFunctionChanges(output)

	if hadCosmetic {
		// Check if filtered output only contains the preamble
		trimmedFiltered := strings.TrimSpace(filtered)
		if trimmedFiltered == "" || trimmedFiltered == "set check_function_bodies = off;" {
			// Migration contained ONLY cosmetic function changes
			fmt.Fprintln(os.Stderr, "[INFO] Detected cosmetic-only function changes, skipping migration generation")
			return "", nil
		}
		// Mixed migration: had cosmetic functions + real changes
		fmt.Fprintf(os.Stderr, "[INFO] Filtered out cosmetic function changes, keeping real schema changes\n")
		return filtered, nil
	}

	return output, nil
}

func loadSchema(ctx context.Context, config pgconn.Config, options ...func(*pgx.ConnConfig)) ([]string, error) {
	conn, err := utils.ConnectByConfig(ctx, config, options...)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	// RLS policies in auth and storage schemas can be included with -s flag
	return migration.ListUserSchemas(ctx, conn)
}

func DiffSchemaMigra(ctx context.Context, source, target pgconn.Config, schema []string, options ...func(*pgx.ConnConfig)) (string, error) {
	env := []string{
		"SOURCE=" + utils.ToPostgresURL(source),
		"TARGET=" + utils.ToPostgresURL(target),
	}
	if ca, err := types.GetRootCA(ctx, utils.ToPostgresURL(target), options...); err != nil {
		return "", err
	} else if len(ca) > 0 {
		env = append(env, "SSL_CA="+ca)
	}
	if len(schema) > 0 {
		env = append(env, "INCLUDED_SCHEMAS="+strings.Join(schema, ","))
	} else {
		env = append(env, "EXCLUDED_SCHEMAS="+strings.Join(managedSchemas, ","))
	}
	cmd := []string{"edge-runtime", "start", "--main-service=."}
	if viper.GetBool("DEBUG") {
		cmd = append(cmd, "--verbose")
	}
	cmdString := strings.Join(cmd, " ")
	entrypoint := []string{"sh", "-c", `cat <<'EOF' > index.ts && ` + cmdString + `
` + diffSchemaTypeScript + `
EOF
`}
	var out, stderr bytes.Buffer
	if err := utils.DockerRunOnceWithConfig(
		ctx,
		container.Config{
			Image:      utils.Config.EdgeRuntime.Image,
			Env:        env,
			Entrypoint: entrypoint,
		},
		container.HostConfig{
			Binds:       []string{utils.EdgeRuntimeId + ":/root/.cache/deno:rw"},
			NetworkMode: network.NetworkHost,
		},
		network.NetworkingConfig{},
		"",
		&out,
		&stderr,
	); err != nil && !strings.HasPrefix(stderr.String(), "main worker has been destroyed") {
		return "", errors.Errorf("error diffing schema: %w:\n%s", err, stderr.String())
	}
	return out.String(), nil
}
