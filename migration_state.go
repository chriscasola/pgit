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
	applyAndUpdateStateForFile(f *fileMigrationState, updateSQL string, newVersion string) error
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
	fileStates map[string]*fileMigrationState
}

func newMigrationState() *migrationState {
	return &migrationState{fileStates: make(map[string]*fileMigrationState)}
}

func (d *SQLDatabaseConnection) readMigrationState() (*migrationState, error) {
	result, err := d.executor.Query(`
		CREATE TABLE IF NOT EXISTS ` + d.tableName + ` (
			file text PRIMARY KEY,
			version text NOT NULL
		);
		SELECT file, version FROM ` + d.tableName + `;`,
	)

	if err != nil {
		return nil, errors.Wrap(err, "unable to read migration state from database")
	}

	defer result.Close()

	m := newMigrationState()

	for result.Next() {
		f := &fileMigrationState{}
		if err := result.Read(f); err != nil {
			return nil, errors.Wrap(err, "error reading file migration state")
		}
		m.fileStates[f.path] = f
	}

	if result.Err() != nil {
		return nil, errors.Wrap(err, "error reading file migration states")
	}

	return m, nil
}

func (d *SQLDatabaseConnection) applyAndUpdateStateForFile(f *fileMigrationState, updateSQL string, newVersion string) error {
	sql := fmt.Sprintf(`
		BEGIN;
		%v	
		INSERT INTO %v (file, version)
		VALUES ('%v', '%v')
		ON CONFLICT (file) DO UPDATE SET version = '%v';
		COMMIT;
	`, updateSQL, d.tableName, f.path, newVersion, newVersion)

	result, err := d.executor.Query(sql)

	if err != nil {
		return err
	}

	defer result.Close()

	return nil
}
