package migration

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v4"
)

var (
	ErrMissingRemote = errors.New("Found local migration files to be inserted before the last migration on remote database.")
	ErrMissingLocal  = errors.New("Remote migration versions not found in local migrations directory.")
)

// Find unapplied local migrations older than the latest migration on
// remote, and remote migrations that are missing from local.
func FindPendingMigrations(localMigrations, remoteMigrations []string) ([]string, error) {
	var unapplied, missing []string
	i, j := 0, 0
	for i < len(remoteMigrations) && j < len(localMigrations) {
		remote := remoteMigrations[i]
		filename := filepath.Base(localMigrations[j])
		// Check if migration has been applied before, LoadLocalMigrations guarantees a match
		local := migrateFilePattern.FindStringSubmatch(filename)[1]
		if remote == local {
			j++
			i++
		} else if remote < local {
			missing = append(missing, remote)
			i++
		} else {
			// Include out-of-order local migrations
			unapplied = append(unapplied, localMigrations[j])
			j++
		}
	}
	// Ensure all remote versions exist on local
	if j == len(localMigrations) {
		missing = append(missing, remoteMigrations[i:]...)
	}
	if len(missing) > 0 {
		return missing, errors.New(ErrMissingLocal)
	}
	// Enforce migrations are applied in chronological order by default
	if len(unapplied) > 0 {
		return unapplied, errors.New(ErrMissingRemote)
	}
	pending := localMigrations[len(remoteMigrations):]
	return pending, nil
}

func ApplyMigrations(ctx context.Context, pending []string, conn *pgx.Conn, fsys fs.FS) error {
	if len(pending) > 0 {
		if err := CreateMigrationTable(ctx, conn); err != nil {
			return err
		}
	}
	for _, path := range pending {
		filename := filepath.Base(path)
		fmt.Fprintf(os.Stderr, "Applying migration %s...\n", filename)
		
		// DIAGNOSTIC: Check connection's search_path before applying migration
		var searchPathBefore string
		if err := conn.QueryRow(ctx, "SHOW search_path").Scan(&searchPathBefore); err == nil {
			fmt.Fprintf(os.Stderr, "[DEBUG] Connection search_path BEFORE %s: %s\n", filename, searchPathBefore)
		}
		
		// Reset all connection settings that might have been modified by another statement on the same connection
		// eg: `SELECT pg_catalog.set_config('search_path', '', false);`
		if _, err := conn.Exec(ctx, "RESET ALL"); err != nil {
			return errors.Errorf("failed to reset connection state: %v", err)
		}
		
		// DIAGNOSTIC: Check search_path after RESET ALL
		var searchPathAfterReset string
		if err := conn.QueryRow(ctx, "SHOW search_path").Scan(&searchPathAfterReset); err == nil {
			fmt.Fprintf(os.Stderr, "[DEBUG] Connection search_path AFTER RESET ALL: %s\n", searchPathAfterReset)
		}
		
		if migration, err := NewMigrationFromFile(path, fsys); err != nil {
			return err
		} else {
			// Let migration run as-is, including any search_path modifications
			// The transaction-scoped set_config('search_path', '', false) will execute
			// and properly set search_path for CREATE TABLE statements within that transaction
			if err := migration.ExecBatch(ctx, conn); err != nil {
				return err
			}
			
			// DIAGNOSTIC: Check search_path after migration execution
			var searchPathAfterMigration string
			if err := conn.QueryRow(ctx, "SHOW search_path").Scan(&searchPathAfterMigration); err == nil {
				fmt.Fprintf(os.Stderr, "[DEBUG] Connection search_path AFTER %s: %s\n", filename, searchPathAfterMigration)
			}
		}
	}
	return nil
}

// filterSearchPathStatements removes search_path modification statements from migrations
// This prevents pg_dump's security measure from causing schema differences
func filterSearchPathStatements(statements []string) []string {
	filtered := make([]string, 0, len(statements))
	for _, stmt := range statements {
		// Skip the pg_dump search_path setting statement
		// "SELECT pg_catalog.set_config('search_path', '', false);"
		if strings.Contains(strings.ToLower(stmt), "set_config") && 
		   strings.Contains(strings.ToLower(stmt), "search_path") {
			fmt.Fprintf(os.Stderr, "[DEBUG] Skipping search_path modification statement: %s\n", 
				strings.TrimSpace(stmt)[:min(len(stmt), 80)])
			continue
		}
		filtered = append(filtered, stmt)
	}
	return filtered
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
