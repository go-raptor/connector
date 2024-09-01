package postgres

import (
	"fmt"
	"reflect"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

type PostgresConnector struct {
}

func NewPostgresConnector() *PostgresConnector {
	return &PostgresConnector{}
}

func (c *PostgresConnector) Connect(config interface{}) (*sqlx.DB, error) {
	val := reflect.ValueOf(config)

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("input is not a struct")
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

	db, err := sqlx.Connect("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	return db, nil
}
