package pgx

import (
	"context"
	"fmt"
	"time"

	"github.com/go-raptor/connector"
)

type Migration interface {
	Up(conn PgConnector) error
	Down(conn PgConnector) error
	Name() string
}

type Migrations map[string]Migration

type SchemaMigration struct {
	Version    string
	Name       string
	ExecutedAt time.Time
}

type PgxMigrator struct {
	conn       PgConnector
	migrations Migrations
}

func NewPgxMigrator(conn PgConnector, migrations Migrations) *PgxMigrator {
	return &PgxMigrator{
		conn:       conn,
		migrations: migrations,
	}
}

func (pm *PgxMigrator) createMigrationsTable() error {
	sql := `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version VARCHAR(255) PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			executed_at TIMESTAMP NOT NULL
		)`

	_, err := pm.conn.Exec(context.Background(), sql)
	return err
}

func (pm *PgxMigrator) Up() error {
	if err := pm.createMigrationsTable(); err != nil {
		return err
	}

	var executed []string
	rows, err := pm.conn.Query(context.Background(),
		"SELECT version FROM schema_migrations ORDER BY version ASC")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return err
		}
		executed = append(executed, version)
	}

	pending := make([]string, 0)
	for version := range pm.migrations {
		isExecuted := false
		for _, ex := range executed {
			if version == ex {
				isExecuted = true
				break
			}
		}
		if !isExecuted {
			pending = append(pending, version)
		}
	}

	for _, version := range pending {
		migration := pm.migrations[version]

		tx, err := pm.conn.Begin(context.Background())
		if err != nil {
			return err
		}

		if err := migration.Up(pm.conn); err != nil {
			tx.Rollback(context.Background())
			return err
		}

		_, err = tx.Exec(context.Background(),
			`INSERT INTO schema_migrations (version, name, executed_at) 
             VALUES ($1, $2, $3)`,
			version, migration.Name(), time.Now())

		if err != nil {
			tx.Rollback(context.Background())
			return err
		}

		if err := tx.Commit(context.Background()); err != nil {
			return err
		}
	}

	return nil
}

func (pm *PgxMigrator) Down() error {
	return fmt.Errorf("not implemented")
}

func (pm *PgxMigrator) UpTo(version string) error {
	return fmt.Errorf("not implemented")
}

func (pm *PgxMigrator) DownTo(version string) error {
	return fmt.Errorf("not implemented")
}

func (pm *PgxMigrator) Status() ([]connector.MigrationStatus, error) {
	var statuses []connector.MigrationStatus

	rows, err := pm.conn.Query(context.Background(),
		`SELECT version, name, executed_at FROM schema_migrations ORDER BY version ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var status connector.MigrationStatus
		var executedAt time.Time
		if err := rows.Scan(&status.Version, &status.Name, &executedAt); err != nil {
			return nil, err
		}
		status.ExecutedAt = &executedAt
		status.IsApplied = true
		statuses = append(statuses, status)
	}

	return statuses, nil
}

func (pm *PgxMigrator) Version() (string, error) {
	var version string
	err := pm.conn.QueryRow(context.Background(),
		`SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`).
		Scan(&version)
	if err != nil {
		return "", err
	}
	return version, nil
}
