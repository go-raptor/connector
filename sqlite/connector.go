package sqlite

import (
	"fmt"
	"reflect"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type SqliteConnector struct {
}

func NewSqliteConnector() *SqliteConnector {
	return &SqliteConnector{}
}

func (c *SqliteConnector) Connect(config interface{}) (*gorm.DB, error) {
	val := reflect.ValueOf(config)

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("input is not a struct")
	}

	nameField := val.FieldByName("Name")
	if !nameField.IsValid() {
		return nil, fmt.Errorf("struct does not have a 'Name' field")
	}

	if nameField.Type().Kind() != reflect.String {
		return nil, fmt.Errorf("'Name' field is not of type string")
	}

	name := nameField.Interface().(string)
	return gorm.Open(sqlite.Open(name), &gorm.Config{})
}
