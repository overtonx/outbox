// Package migrations содержит версионированную схему БД пакета outbox,
// встроенную в бинарь через go:embed, и минимальный идемпотентный раннер.
//
// Внешние пользователи, уже управляющие своими миграциями через
// golang-migrate/goose и т.п., могут использовать FS() как источник вместо
// встроенного Migrate.
package migrations

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
)

//go:embed *.sql
var migrationFiles embed.FS

// FS возвращает embed.FS со всеми файлами миграций outbox для использования
// внешними инструментами миграции (например, golang-migrate iofs source).
func FS() fs.FS {
	return migrationFiles
}

// Migrate применяет все ещё не применённые встроенные миграции по порядку
// имён файлов, отслеживая применённые версии в таблице
// outbox_schema_migrations. Идемпотентна — безопасно вызывать при каждом
// старте процесса.
func Migrate(ctx context.Context, db *sql.DB) error {
	if err := ensureMigrationsTable(ctx, db); err != nil {
		return fmt.Errorf("outbox/migrations: failed to ensure migrations table: %w", err)
	}

	names, err := migrationNames()
	if err != nil {
		return fmt.Errorf("outbox/migrations: failed to list embedded migrations: %w", err)
	}

	for _, name := range names {
		applied, err := isApplied(ctx, db, name)
		if err != nil {
			return fmt.Errorf("outbox/migrations: failed to check migration %s: %w", name, err)
		}
		if applied {
			continue
		}

		contents, err := fs.ReadFile(migrationFiles, name)
		if err != nil {
			return fmt.Errorf("outbox/migrations: failed to read migration %s: %w", name, err)
		}

		if err := applyMigration(ctx, db, name, string(contents)); err != nil {
			return fmt.Errorf("outbox/migrations: failed to apply migration %s: %w", name, err)
		}
	}

	return nil
}

func migrationNames() ([]string, error) {
	entries, err := fs.ReadDir(migrationFiles, ".")
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || path.Ext(e.Name()) != ".sql" {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}

func ensureMigrationsTable(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS outbox_schema_migrations (
			version    VARCHAR(255) NOT NULL PRIMARY KEY,
			applied_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`)
	return err
}

func isApplied(ctx context.Context, db *sql.DB, version string) (bool, error) {
	var exists int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM outbox_schema_migrations WHERE version = ?`, version).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// applyMigration executes each statement directly on db rather than inside a
// transaction: MySQL implicitly commits on DDL, so wrapping CREATE TABLE in a
// transaction would give no real atomicity.
func applyMigration(ctx context.Context, db *sql.DB, version, contents string) error {
	for _, stmt := range splitStatements(contents) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("statement failed: %w", err)
		}
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO outbox_schema_migrations (version) VALUES (?)`, version); err != nil {
		return fmt.Errorf("failed to record applied migration: %w", err)
	}

	return nil
}

func splitStatements(sqlText string) []string {
	parts := strings.Split(sqlText, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
