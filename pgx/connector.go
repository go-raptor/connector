package pgx

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-raptor/connector"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PgConnector interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
	Begin(context.Context) (pgx.Tx, error)
	Ping(context.Context) error
}

type ConnWrapper struct {
	PgConnector
	closeFunc func(context.Context) error
}

func (w *ConnWrapper) Close(ctx context.Context) error {
	return w.closeFunc(ctx)
}

func wrapPoolClose(closeFunc func()) func(context.Context) error {
	return func(context.Context) error {
		closeFunc()
		return nil
	}
}

type ConnType int

const (
	SingleConn ConnType = iota
	PoolConn
)

type PgxConnector struct {
	migrations Migrations
	config     interface{}
	connType   ConnType
	conn       *ConnWrapper
	migrator   *PgxMigrator
}

func NewPgxConnector(config interface{}, migrations Migrations, connType ConnType) connector.DatabaseConnector {
	return &PgxConnector{
		config:     config,
		migrations: migrations,
		connType:   connType,
	}
}

func (c *PgxConnector) Conn() any {
	return c.conn
}

func (c *PgxConnector) Migrator() connector.Migrator {
	return c.migrator
}

func (c *PgxConnector) Init() error {
	val := reflect.ValueOf(c.config)

	if val.Kind() != reflect.Struct {
		return fmt.Errorf("input is not a struct")
	}

	hostField := val.FieldByName("Host")
	portField := val.FieldByName("Port")
	userField := val.FieldByName("Username")
	passwordField := val.FieldByName("Password")
	nameField := val.FieldByName("Name")

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=disable",
		hostField.Interface().(string),
		userField.Interface().(string),
		passwordField.Interface().(string),
		nameField.Interface().(string),
		portField.Interface().(int),
	)

	var wrapper *ConnWrapper

	if c.connType == SingleConn {
		conn, err := pgx.Connect(context.Background(), dsn)
		if err != nil {
			return fmt.Errorf("failed to create connection: %w", err)
		}
		wrapper = &ConnWrapper{
			PgConnector: conn,
			closeFunc:   conn.Close,
		}
	} else {
		config, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			return fmt.Errorf("failed to parse DSN: %w", err)
		}
		pool, err := pgxpool.NewWithConfig(context.Background(), config)
		if err != nil {
			return fmt.Errorf("failed to create pool: %w", err)
		}
		wrapper = &ConnWrapper{
			PgConnector: pool,
			closeFunc:   wrapPoolClose(pool.Close),
		}
	}

	if err := wrapper.Ping(context.Background()); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	c.conn = wrapper
	c.migrator = NewPgxMigrator(wrapper, c.migrations)

	if err := c.migrator.Up(); err != nil {
		return fmt.Errorf("failed to migrate: %w", err)
	}

	return nil
}
