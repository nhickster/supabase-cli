package migration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/go-errors/errors"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/spf13/viper"
	"github.com/supabase/cli/pkg/parser"
)

type MigrationFile struct {
	Version    string
	Name       string
	Statements []string
}

var migrateFilePattern = regexp.MustCompile(`^([0-9]+)_(.*)\.sql$`)

func NewMigrationFromFile(path string, fsys fs.FS) (*MigrationFile, error) {
	lines, err := parseFile(path, fsys)
	if err != nil {
		return nil, err
	}
	file := MigrationFile{Statements: lines}
	// Parse version from file name
	filename := filepath.Base(path)
	matches := migrateFilePattern.FindStringSubmatch(filename)
	if len(matches) > 2 {
		file.Version = matches[1]
		file.Name = matches[2]
	}
	return &file, nil
}

func parseFile(path string, fsys fs.FS) ([]string, error) {
	sql, err := fsys.Open(path)
	if err != nil {
		return nil, errors.Errorf("failed to open migration file: %w", err)
	}
	defer sql.Close()
	// Unless explicitly specified, Use file length as max buffer size
	if !viper.IsSet("SCANNER_BUFFER_SIZE") {
		if fi, err := sql.Stat(); err == nil {
			if size := int(fi.Size()); size > parser.MaxScannerCapacity {
				parser.MaxScannerCapacity = size
			}
		}
	}
	return parser.SplitAndTrim(sql)
}

func NewMigrationFromReader(sql io.Reader) (*MigrationFile, error) {
	lines, err := parser.SplitAndTrim(sql)
	if err != nil {
		return nil, err
	}
	return &MigrationFile{Statements: lines}, nil
}

func (m *MigrationFile) ExecBatch(ctx context.Context, conn *pgx.Conn) error {
	// DIAGNOSTIC: Log statements being batched
	fmt.Fprintf(os.Stderr, "[DEBUG] ExecBatch: Starting v11 - session-level SET before batch\n")
	fmt.Fprintf(os.Stderr, "[DEBUG] ExecBatch: Processing %d statements\n", len(m.Statements))
	
	// Look for set_config search_path statement
	setConfigIdx := -1
	for i, stmt := range m.Statements {
		stmtLower := strings.ToLower(stmt)
		if strings.Contains(stmtLower, "set_config") && strings.Contains(stmtLower, "search_path") {
			setConfigIdx = i
			fmt.Fprintf(os.Stderr, "[DEBUG] ExecBatch: Found set_config at statement %d\n", i)
			break
		}
	}
	
	// THE REAL FIX: Set search_path at SESSION level OUTSIDE any transaction
	// This way it persists through the implicit transaction created by ExecBatch
	if setConfigIdx >= 0 {
		fmt.Fprintf(os.Stderr, "[DEBUG] ExecBatch: Setting SESSION search_path = pg_catalog (no extensions!)\n")
		if _, err := conn.Exec(ctx, "SET search_path = pg_catalog"); err != nil {
			fmt.Fprintf(os.Stderr, "[DEBUG] ExecBatch: ERROR setting search_path: %v\n", err)
			return errors.Errorf("failed to set search_path: %w", err)
		}
		
		// Verify it worked
		var newSearchPath string
		if err := conn.QueryRow(ctx, "SHOW search_path").Scan(&newSearchPath); err == nil {
			fmt.Fprintf(os.Stderr, "[DEBUG] ExecBatch: Session search_path NOW: %s\n", newSearchPath)
		}
	}
	
	// Batch migration commands, without using statement cache
	batch := &pgconn.Batch{}
	for i, line := range m.Statements {
		// Skip the original set_config statement - we handled it above
		if i == setConfigIdx {
			fmt.Fprintf(os.Stderr, "[DEBUG] ExecBatch: Skipping original set_config statement at position %d\n", i)
			continue
		}
		batch.ExecParams(line, nil, nil, nil, nil)
	}
	
	// Insert into migration history
	if len(m.Version) > 0 {
		if err := m.insertVersionSQL(conn, batch); err != nil {
			return err
		}
	}
	
	fmt.Fprintf(os.Stderr, "[DEBUG] ExecBatch: Executing batch with search_path=%s...\n", "pg_catalog")
	
	// ExecBatch is implicitly transactional
	result, err := conn.PgConn().ExecBatch(ctx, batch).ReadAll()
	if err != nil {
		// Defaults to printing the last statement on error
		stat := INSERT_MIGRATION_VERSION
		i := len(result)
		// Account for skipped set_config statement
		if setConfigIdx >= 0 && i >= setConfigIdx {
			i++
		}
		if i < len(m.Statements) {
			stat = m.Statements[i]
		}
		var msg []string
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			stat = markError(stat, int(pgErr.Position))
			if len(pgErr.Detail) > 0 {
				msg = append(msg, pgErr.Detail)
			}
		}
		msg = append(msg, fmt.Sprintf("At statement: %d", i), stat)
		return errors.Errorf("%w\n%s", err, strings.Join(msg, "\n"))
	}
	
	fmt.Fprintf(os.Stderr, "[DEBUG] ExecBatch: Batch executed successfully (%d results)\n", len(result))
	
	// DIAGNOSTIC: Check column defaults that were created
	// Query with search_path = pg_catalog to see if normalization changes
	rows, err := conn.Query(ctx, `
		SELECT c.table_name, c.column_name, c.column_default
		FROM information_schema.columns c
		WHERE c.table_schema = 'public' 
		  AND c.column_name = 'id'
		  AND c.column_default LIKE '%uuid_generate_v4%'
		ORDER BY c.table_name
		LIMIT 5
	`)
	if err == nil {
		defer rows.Close()
		fmt.Fprintf(os.Stderr, "[DEBUG] ExecBatch: Column defaults WITH search_path=pg_catalog:\n")
		for rows.Next() {
			var tableName, columnName, columnDefault string
			if err := rows.Scan(&tableName, &columnName, &columnDefault); err == nil {
				fmt.Fprintf(os.Stderr, "[DEBUG]   %s.%s: %s\n", tableName, columnName, columnDefault)
			}
		}
	}
	
	// Reset search_path to default after migration completes
	if setConfigIdx >= 0 {
		fmt.Fprintf(os.Stderr, "[DEBUG] ExecBatch: Resetting search_path to default\n")
		conn.Exec(ctx, "RESET search_path")
		
		// Now query AGAIN with the default search_path to see if it changes
		rows2, err := conn.Query(ctx, `
			SELECT c.table_name, c.column_name, c.column_default
			FROM information_schema.columns c
			WHERE c.table_schema = 'public' 
			  AND c.column_name = 'id'
			  AND c.column_default LIKE '%uuid_generate_v4%'
			ORDER BY c.table_name
			LIMIT 5
		`)
		if err == nil {
			defer rows2.Close()
			fmt.Fprintf(os.Stderr, "[DEBUG] ExecBatch: Column defaults AFTER RESET (search_path has extensions):\n")
			for rows2.Next() {
				var tableName, columnName, columnDefault string
				if err := rows2.Scan(&tableName, &columnName, &columnDefault); err == nil {
					fmt.Fprintf(os.Stderr, "[DEBUG]   %s.%s: %s\n", tableName, columnName, columnDefault)
				}
			}
		}
		
		// Also check the RAW pg_attrdef to see what's ACTUALLY stored
		fmt.Fprintf(os.Stderr, "[DEBUG] ExecBatch: Checking RAW pg_attrdef.adbin (what's ACTUALLY stored):\n")
		rows3, err := conn.Query(ctx, `
			SELECT 
				c.relname AS table_name,
				a.attname AS column_name,
				pg_get_expr(d.adbin, d.adrelid) AS column_default
			FROM pg_catalog.pg_attrdef d
			JOIN pg_catalog.pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
			JOIN pg_catalog.pg_class c ON c.oid = d.adrelid
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'public'
			  AND a.attname = 'id'
			  AND pg_get_expr(d.adbin, d.adrelid) LIKE '%uuid_generate_v4%'
			ORDER BY c.relname
			LIMIT 5
		`)
		if err == nil {
			defer rows3.Close()
			for rows3.Next() {
				var tableName, columnName, columnDefault string
				if err := rows3.Scan(&tableName, &columnName, &columnDefault); err == nil {
					fmt.Fprintf(os.Stderr, "[DEBUG]   %s.%s: %s\n", tableName, columnName, columnDefault)
				}
			}
		}
	}
	
	return nil
}

func markError(stat string, pos int) string {
	lines := strings.Split(stat, "\n")
	for j, r := range lines {
		if c := len(r); pos > c {
			pos -= c + 1
			continue
		}
		// Show a caret below the error position
		if pos > 0 {
			caret := append(bytes.Repeat([]byte{' '}, pos-1), '^')
			lines = append(lines[:j+1], string(caret))
		}
		break
	}
	return strings.Join(lines, "\n")
}

func (m *MigrationFile) insertVersionSQL(conn *pgx.Conn, batch *pgconn.Batch) error {
	value := pgtype.TextArray{}
	if err := value.Set(m.Statements); err != nil {
		return errors.Errorf("failed to set text array: %w", err)
	}
	ci := conn.ConnInfo()
	var err error
	var encoded []byte
	var valueFormat int16
	if conn.Config().PreferSimpleProtocol {
		encoded, err = value.EncodeText(ci, encoded)
		valueFormat = pgtype.TextFormatCode
	} else {
		encoded, err = value.EncodeBinary(ci, encoded)
		valueFormat = pgtype.BinaryFormatCode
	}
	if err != nil {
		return errors.Errorf("failed to encode binary: %w", err)
	}
	batch.ExecParams(
		INSERT_MIGRATION_VERSION,
		[][]byte{[]byte(m.Version), []byte(m.Name), encoded},
		[]uint32{pgtype.TextOID, pgtype.TextOID, pgtype.TextArrayOID},
		[]int16{pgtype.TextFormatCode, pgtype.TextFormatCode, valueFormat},
		nil,
	)
	return nil
}

type SeedFile struct {
	Path  string
	Hash  string
	Dirty bool `db:"-"`
}

func NewSeedFile(path string, fsys fs.FS) (*SeedFile, error) {
	sql, err := fsys.Open(path)
	if err != nil {
		return nil, errors.Errorf("failed to open seed file: %w", err)
	}
	defer sql.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, sql); err != nil {
		return nil, errors.Errorf("failed to hash file: %w", err)
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	return &SeedFile{Path: path, Hash: digest}, nil
}

func (m *SeedFile) ExecBatchWithCache(ctx context.Context, conn *pgx.Conn, fsys fs.FS) error {
	// Parse each file individually to reduce memory usage
	lines, err := parseFile(m.Path, fsys)
	if err != nil {
		return err
	}
	// Data statements don't mutate schemas, safe to use statement cache
	batch := pgx.Batch{}
	if !m.Dirty {
		for _, line := range lines {
			batch.Queue(line)
		}
	}
	batch.Queue(UPSERT_SEED_FILE, m.Path, m.Hash)
	// No need to track version here because there are no schema changes
	if err := conn.SendBatch(ctx, &batch).Close(); err != nil {
		return errors.Errorf("failed to send batch: %w", err)
	}
	return nil
}
