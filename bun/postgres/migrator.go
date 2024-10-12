package postgres

import (
	"context"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/uptrace/bun"
)

type Migration func(db *bun.DB) error

type Migrations map[int]Migration

type SchemaMigration struct {
	ID         int       `bun:"id,pk,autoincrement"`
	Version    string    `bun:"version,notnull"`
	MigratedAt time.Time `bun:"migrated_at,notnull"`
	CreatedAt  time.Time `bun:"created_at,notnull"`
	UpdatedAt  time.Time `bun:"updated_at,notnull"`
}

func (c *PostgresConnector) migrate() error {
	_, err := c.conn.NewCreateTable().
		Model((*SchemaMigration)(nil)).
		IfNotExists().
		Exec(context.Background())
	if err != nil {
		return err
	}

	var currentVersion int
	err = c.conn.NewSelect().
		Model((*SchemaMigration)(nil)).
		ColumnExpr("COUNT(*)").
		Scan(context.Background(), &currentVersion)
	if err != nil {
		return err
	}

	for i := currentVersion + 1; i <= len(c.migrations); i++ {
		funcName := strings.Split(runtime.FuncForPC(reflect.ValueOf(c.migrations[i]).Pointer()).Name(), "/")
		migrationName := funcName[len(funcName)-1]

		tx, err := c.conn.BeginTx(context.Background(), nil)
		if err != nil {
			return err
		}

		err = c.migrations[i](c.conn)
		if err == nil {
			_, err = tx.NewInsert().
				Model(&SchemaMigration{
					Version:    migrationName,
					MigratedAt: time.Now(),
					CreatedAt:  time.Now(),
					UpdatedAt:  time.Now(),
				}).
				Exec(context.Background())
			if err != nil {
				tx.Rollback()
				return err
			}
		} else {
			tx.Rollback()
			return err
		}

		err = tx.Commit()
		if err != nil {
			return err
		}
	}

	return nil
}
