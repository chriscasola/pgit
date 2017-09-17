package pgit

import (
	"fmt"

	"github.com/chriscasola/sqlgo"
	"github.com/pkg/errors"
)

// DatabaseConnection defines the interface to connections to various
// types of databases.
type DatabaseConnection interface {
	readMigrationState() (*migrationState, error)
	writeMigrationState(m *migrationState) error
}

// SQLDatabaseConnection contains pointers to the data about what migration state
// the database is in. In other words, it tracks the versions of each schema
// file that are currently applied to the database.
type SQLDatabaseConnection struct {
	dbURL     string
	tableName string
	executor  *sqlgo.Executor
}

// NewSQLDatabaseConnection returns a new DatabaseConnection using the given database
// URL and table name to track the migration state. If tableName is the empty
// string then the default table name of "pgit" will be used.
func NewSQLDatabaseConnection(dbURL, tableName string) (*SQLDatabaseConnection, error) {
	executor, err := sqlgo.NewExecutor(dbURL)
	if err != nil {
		return nil, errors.Wrap(err, "unable to connect to database")
	}
	if tableName == "" {
		tableName = "pgit"
	}
	return &SQLDatabaseConnection{dbURL: dbURL, tableName: tableName, executor: executor}, nil
}

type fileMigrationState struct {
	version string
	path    string
}

func (f *fileMigrationState) FromRow(s sqlgo.ScannerFunction) error {
	return s(&f.path, &f.version)
}

type migrationState struct {
	fileStates map[string]fileMigrationState
}

func (d *SQLDatabaseConnection) readMigrationState() (*migrationState, error) {
	result, err := d.executor.Query("SELECT file, version FROM $1;", d.tableName)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read migration state from database")
	}

	defer result.Close()

	m := migrationState{}

	for result.Next() {
		f := fileMigrationState{}
		if err := result.Read(&f); err != nil {
			return nil, errors.Wrap(err, "error reading file migration state")
		}
		m.fileStates[f.path] = f
	}

	if result.Err() != nil {
		return nil, errors.Wrap(err, "error reading file migration states")
	}

	return &m, nil
}

func (d *SQLDatabaseConnection) writeMigrationState(m *migrationState) error {
	s := sqlgo.NewSerializer()
	query := "BEGIN;\n"
	for _, val := range m.fileStates {
		query += fmt.Sprintf(
			"INSERT INTO %v (file, version) VALUES (%v, %v);\n",
			s.Add(d.tableName), s.Add(val.path), s.Add(val.version),
		)
	}
	query += "COMMIT;\n"
	_, err := d.executor.Query(query, s.Params()...)
	return err
}
