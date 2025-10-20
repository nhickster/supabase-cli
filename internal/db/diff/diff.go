package diff

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/go-connections/nat"
	"github.com/go-errors/errors"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/spf13/afero"
	"github.com/supabase/cli/internal/db/start"
	"github.com/supabase/cli/internal/gen/keys"
	"github.com/supabase/cli/internal/utils"
	"github.com/supabase/cli/pkg/migration"
	"github.com/supabase/cli/pkg/parser"
)

type DiffFunc func(context.Context, pgconn.Config, pgconn.Config, []string, ...func(*pgx.ConnConfig)) (string, error)

func Run(ctx context.Context, schema []string, file string, config pgconn.Config, differ DiffFunc, fsys afero.Fs, options ...func(*pgx.ConnConfig)) (err error) {
	out, err := DiffDatabase(ctx, schema, config, os.Stderr, fsys, differ, options...)
	if err != nil {
		return err
	}
	branch := keys.GetGitBranch(fsys)
	fmt.Fprintln(os.Stderr, "Finished "+utils.Aqua("supabase db diff")+" on branch "+utils.Aqua(branch)+".\n")
	if err := SaveDiff(out, file, fsys); err != nil {
		return err
	}
	drops := findDropStatements(out)
	if len(drops) > 0 {
		fmt.Fprintln(os.Stderr, "Found drop statements in schema diff. Please double check if these are expected:")
		fmt.Fprintln(os.Stderr, utils.Yellow(strings.Join(drops, "\n")))
	}
	return nil
}

func loadDeclaredSchemas(fsys afero.Fs) ([]string, error) {
	if schemas := utils.Config.Db.Migrations.SchemaPaths; len(schemas) > 0 {
		return schemas.Files(afero.NewIOFS(fsys))
	}
	if exists, err := afero.DirExists(fsys, utils.SchemasDir); err != nil {
		return nil, errors.Errorf("failed to check schemas: %w", err)
	} else if !exists {
		return nil, nil
	}
	var declared []string
	if err := afero.Walk(fsys, utils.SchemasDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() && filepath.Ext(info.Name()) == ".sql" {
			declared = append(declared, path)
		}
		return nil
	}); err != nil {
		return nil, errors.Errorf("failed to walk dir: %w", err)
	}
	return declared, nil
}

// https://github.com/djrobstep/migra/blob/master/migra/statements.py#L6
var dropStatementPattern = regexp.MustCompile(`(?i)drop\s+`)

func findDropStatements(out string) []string {
	lines, err := parser.SplitAndTrim(strings.NewReader(out))
	if err != nil {
		return nil
	}
	var drops []string
	for _, line := range lines {
		if dropStatementPattern.MatchString(line) {
			drops = append(drops, line)
		}
	}
	return drops
}

// GetSearchPath queries the target database to get its current search_path setting
// This ensures the shadow database can be configured with the same search_path
func GetSearchPath(ctx context.Context, config pgconn.Config, options ...func(*pgx.ConnConfig)) (string, error) {
	conn, err := utils.ConnectByConfig(ctx, config, options...)
	if err != nil {
		return "", errors.Errorf("failed to connect to target database: %w", err)
	}
	defer conn.Close(context.Background())

	var searchPath string
	// Query the current search_path setting
	err = conn.QueryRow(ctx, "SHOW search_path").Scan(&searchPath)
	if err != nil {
		return "", errors.Errorf("failed to query search_path: %w", err)
	}

	// PostgreSQL's SHOW search_path returns quoted identifiers like "$user", public
	// We need to normalize this for use in the -c flag
	// Strip the outer quotes if present and normalize
	searchPath = strings.TrimSpace(searchPath)

	// PostgreSQL uses double quotes for identifiers, but for the search_path config
	// we need to remove them to avoid double-quoting issues
	// Example: "$user", public -> $user, public
	searchPath = strings.ReplaceAll(searchPath, "\"", "")

	return searchPath, nil
}

func CreateShadowDatabase(ctx context.Context, port uint16) (string, error) {
	return CreateShadowDatabaseWithSearchPath(ctx, port, "")
}

func CreateShadowDatabaseWithSearchPath(ctx context.Context, port uint16, searchPath string) (string, error) {
	// Disable background workers in shadow database
	args := []string{"-c", "max_worker_processes=0"}

	// CRITICAL FIX: The remote database has search_path='$user,public' (WITHOUT extensions)
	// But NewContainerConfig adds search_path='$user,public,extensions' for PostgreSQL 14+
	// This causes PostgreSQL to normalize "extensions"."uuid_generate_v4"() differently:
	// - With extensions in path: stored as uuid_generate_v4() (unqualified)
	// - Without extensions in path: stored as extensions.uuid_generate_v4() (qualified)
	// We MUST match the remote's search_path exactly

	if searchPath == "" {
		// Default to excluding extensions to match typical Supabase remote setup
		searchPath = "$user, public"
	}

	// Override the default search_path by adding it as an argument
	// PostgreSQL uses the LAST -c search_path value
	searchPathArg := fmt.Sprintf("search_path='%s'", searchPath)
	args = append(args, "-c", searchPathArg)

	fmt.Fprintf(os.Stderr, "[DEBUG diff.go] Target search_path for shadow: %s\n", searchPath)
	fmt.Fprintf(os.Stderr, "[DEBUG diff.go] Args BEFORE NewContainerConfig: %v\n", args)

	config := start.NewContainerConfig(args...)

	fmt.Fprintf(os.Stderr, "[DEBUG diff.go] Docker CMD after NewContainerConfig: %v\n", config.Cmd)
	fmt.Fprintf(os.Stderr, "[DEBUG diff.go] Docker Entrypoint: %v\n", config.Entrypoint)

	hostPort := strconv.FormatUint(uint64(port), 10)
	hostConfig := container.HostConfig{
		PortBindings: nat.PortMap{"5432/tcp": []nat.PortBinding{{HostPort: hostPort}}},
		AutoRemove:   true,
	}
	networkingConfig := network.NetworkingConfig{}
	if utils.Config.Db.MajorVersion <= 14 {
		hostConfig.Tmpfs = map[string]string{"/docker-entrypoint-initdb.d": ""}
	}
	return utils.DockerStart(ctx, config, hostConfig, networkingConfig, "")
}

func ConnectShadowDatabase(ctx context.Context, timeout time.Duration, options ...func(*pgx.ConnConfig)) (conn *pgx.Conn, err error) {
	// Retry until connected, cancelled, or timeout
	policy := start.NewBackoffPolicy(ctx, timeout)
	config := pgconn.Config{Port: utils.Config.Db.ShadowPort}
	connect := func() (*pgx.Conn, error) {
		return utils.ConnectLocalPostgres(ctx, config, options...)
	}
	return backoff.RetryWithData(connect, policy)
}

// Required to bypass pg_cron check: https://github.com/citusdata/pg_cron/blob/main/pg_cron.sql#L3
const CREATE_TEMPLATE = "CREATE DATABASE contrib_regression TEMPLATE postgres"

func MigrateShadowDatabase(ctx context.Context, container string, fsys afero.Fs, options ...func(*pgx.ConnConfig)) error {
	migrations, err := migration.ListLocalMigrations(utils.MigrationsDir, afero.NewIOFS(fsys))
	if err != nil {
		return err
	}
	conn, err := ConnectShadowDatabase(ctx, 10*time.Second, options...)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if err := start.SetupDatabase(ctx, conn, container[:12], os.Stderr, fsys); err != nil {
		return err
	}

	if _, err := conn.Exec(ctx, CREATE_TEMPLATE); err != nil {
		return errors.Errorf("failed to create template database: %w", err)
	}

	fmt.Fprintf(os.Stderr, "[DEBUG] Applying %d local migrations to shadow database...\n", len(migrations))

	// DIAGNOSTIC: Check shadow DB search_path before migrations
	var searchPathBefore string
	if err := conn.QueryRow(ctx, "SHOW search_path").Scan(&searchPathBefore); err != nil {
		fmt.Fprintf(os.Stderr, "[DEBUG] Could not query search_path before migrations: %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "[DEBUG] Shadow search_path BEFORE migrations: %s\n", searchPathBefore)
	}

	if err := migration.ApplyMigrations(ctx, migrations, conn, afero.NewIOFS(fsys)); err != nil {
		return err
	}

	// DIAGNOSTIC: Check shadow DB search_path after migrations
	var searchPathAfter string
	if err := conn.QueryRow(ctx, "SHOW search_path").Scan(&searchPathAfter); err != nil {
		fmt.Fprintf(os.Stderr, "[DEBUG] Could not query search_path after migrations: %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "[DEBUG] Shadow search_path AFTER migrations: %s\n", searchPathAfter)
	}

	// DIAGNOSTIC: Check actual column defaults stored in shadow DB
	fmt.Fprintf(os.Stderr, "[DEBUG] Querying column defaults from shadow database...\n")
	rows, err := conn.Query(ctx, `
		SELECT table_name, column_name, column_default
		FROM information_schema.columns
		WHERE table_schema = 'public' 
		  AND column_default LIKE '%uuid_generate_v4%'
		ORDER BY table_name
		LIMIT 5
	`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[DEBUG] Could not query column defaults: %v\n", err)
	} else {
		defer rows.Close()
		fmt.Fprintf(os.Stderr, "[DEBUG] Column defaults in SHADOW database:\n")
		for rows.Next() {
			var tableName, columnName, columnDefault string
			if err := rows.Scan(&tableName, &columnName, &columnDefault); err == nil {
				fmt.Fprintf(os.Stderr, "[DEBUG]   %s.%s: %s\n", tableName, columnName, columnDefault)
			}
		}
	}

	return nil
}

func DiffDatabase(ctx context.Context, schema []string, config pgconn.Config, w io.Writer, fsys afero.Fs, differ DiffFunc, options ...func(*pgx.ConnConfig)) (string, error) {
	// DIAGNOSTIC: Check target database column defaults BEFORE creating shadow
	fmt.Fprintln(w, "Connecting to remote database...")
	targetConn, err := utils.ConnectByConfig(ctx, config, options...)
	if err != nil {
		return "", errors.Errorf("failed to connect to target database: %w", err)
	}

	fmt.Fprintf(w, "[DEBUG] Querying column defaults from TARGET (remote/local) database...\n")
	targetRows, err := targetConn.Query(ctx, `
		SELECT table_name, column_name, column_default
		FROM information_schema.columns
		WHERE table_schema = 'public' 
		  AND column_name = 'id'
		  AND column_default LIKE '%uuid_generate_v4%'
		ORDER BY table_name
		LIMIT 5
	`)
	if err != nil {
		fmt.Fprintf(w, "[DEBUG] Could not query target column defaults: %v\n", err)
	} else {
		defer targetRows.Close()
		fmt.Fprintf(w, "[DEBUG] Column defaults in TARGET database:\n")
		for targetRows.Next() {
			var tableName, columnName, columnDefault string
			if err := targetRows.Scan(&tableName, &columnName, &columnDefault); err == nil {
				fmt.Fprintf(w, "[DEBUG]   %s.%s: %s\n", tableName, columnName, columnDefault)
			}
		}
	}
	targetConn.Close(ctx)

	// Query the target database's search_path before creating shadow
	// This ensures shadow DB will be configured with the same search_path
	targetSearchPath, err := GetSearchPath(ctx, config, options...)
	if err != nil {
		fmt.Fprintf(w, "Warning: Could not determine target search_path, using default: %v\n", err)
		targetSearchPath = "" // Fall back to default ($user, public without extensions)
	}

	fmt.Fprintln(w, "Creating shadow database...")
	shadow, err := CreateShadowDatabaseWithSearchPath(ctx, utils.Config.Db.ShadowPort, targetSearchPath)
	if err != nil {
		return "", err
	}
	defer utils.DockerRemove(shadow)
	if err := start.WaitForHealthyService(ctx, start.HealthTimeout, shadow); err != nil {
		return "", err
	}
	if err := MigrateShadowDatabase(ctx, shadow, fsys, options...); err != nil {
		return "", err
	}
	shadowConfig := pgconn.Config{
		Host:     utils.Config.Hostname,
		Port:     utils.Config.Db.ShadowPort,
		User:     "postgres",
		Password: utils.Config.Db.Password,
		Database: "postgres",
	}
	if utils.IsLocalDatabase(config) {
		if declared, err := loadDeclaredSchemas(fsys); err != nil {
			return "", err
		} else if len(declared) > 0 {
			config = shadowConfig
			config.Database = "contrib_regression"
			if err := migrateBaseDatabase(ctx, config, declared, fsys, options...); err != nil {
				return "", err
			}
		}
	}
	// Load all user defined schemas
	if len(schema) > 0 {
		fmt.Fprintln(w, "Diffing schemas:", strings.Join(schema, ","))
	} else {
		fmt.Fprintln(w, "Diffing schemas...")
	}
	return differ(ctx, shadowConfig, config, schema, options...)
}

func migrateBaseDatabase(ctx context.Context, config pgconn.Config, migrations []string, fsys afero.Fs, options ...func(*pgx.ConnConfig)) error {
	fmt.Fprintln(os.Stderr, "Creating local database from declarative schemas:")
	msg := make([]string, len(migrations))
	for i, m := range migrations {
		msg[i] = fmt.Sprintf(" • %s", utils.Bold(m))
	}
	fmt.Fprintln(os.Stderr, strings.Join(msg, "\n"))
	conn, err := utils.ConnectLocalPostgres(ctx, config, options...)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	return migration.SeedGlobals(ctx, migrations, conn, afero.NewIOFS(fsys))
}
