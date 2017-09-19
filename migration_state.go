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
	applyAndUpdateStateForFile(f *fileMigrationState, updateSQL string, newVersion string, migration *migration) error
	createNewMigration() (*migration, error)
	finishMigration(m *migration) error
	rollbackFile(f *fileMigrationState, rollbackSQL string, newVersion string, lastMigration *migration) error
	removeMigration(m *migration) error
	getFilesInMigration(m *migration) ([]fileMigrationState, error)
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
	version   string
	path      string
	migration int
}

func (f *fileMigrationState) FromRow(s sqlgo.ScannerFunction) error {
	return s(&f.path, &f.version, &f.migration)
}

type migration struct {
	id        int
	completed bool
}

func (m *migration) FromRow(s sqlgo.ScannerFunction) error {
	return s(&m.id, &m.completed)
}

type migrationState struct {
	fileStates    map[string]*fileMigrationState
	lastMigration *migration
}

func newMigrationState() *migrationState {
	return &migrationState{fileStates: make(map[string]*fileMigrationState), lastMigration: &migration{}}
}

func (d *SQLDatabaseConnection) readMigrationState() (*migrationState, error) {
	result, err := d.executor.Query(`
		CREATE TABLE IF NOT EXISTS ` + d.tableName + `_migrations (
			id serial PRIMARY KEY,
			completed boolean DEFAULT false NOT NULL
		);
		SELECT id, completed FROM ` + d.tableName + `_migrations ORDER BY id DESC LIMIT 1;`,
	)

	if err != nil {
		return nil, errors.Wrap(err, "unable to read from migrations table")
	}

	defer result.Close()

	m := newMigrationState()

	if result.Next() {
		if err = result.Read(m.lastMigration); err != nil {
			return nil, errors.Wrap(err, "error reading result from migrations table")
		}
	}

	if err = result.Err(); err != nil {
		return nil, errors.Wrap(err, "error reading result from migrations table")
	}

	filesResult, err := d.executor.Query(`
		CREATE TABLE IF NOT EXISTS ` + d.tableName + ` (
			file text NOT NULL,
			version text NOT NULL,
			migration integer NOT NULL REFERENCES ` + d.tableName + `_migrations (id),
			PRIMARY KEY (file, version)
		);
		SELECT file, version, migration FROM ` + d.tableName + `;`,
	)

	if err != nil {
		return nil, errors.Wrap(err, "unable to read migration state from database")
	}

	defer filesResult.Close()

	for filesResult.Next() {
		f := &fileMigrationState{}
		if err := filesResult.Read(f); err != nil {
			return nil, errors.Wrap(err, "error reading file migration state")
		}
		m.fileStates[f.path] = f
	}

	if filesResult.Err() != nil {
		return nil, errors.Wrap(err, "error reading file migration states")
	}

	return m, nil
}

func (d *SQLDatabaseConnection) createNewMigration() (*migration, error) {
	result, err := d.executor.Query(`
		INSERT INTO ` + d.tableName + `_migrations (completed) VALUES (false) RETURNING id, completed;
	`)

	if err != nil {
		return nil, errors.Wrap(err, "unable to create new migration in database")
	}

	defer result.Close()

	if !result.Next() {
		return nil, errors.New("no migration created in database")
	}

	m := &migration{}

	if err = result.Read(m); err != nil {
		return nil, errors.Wrap(err, "failed to read new migration from database")
	}

	return m, nil
}

func (d *SQLDatabaseConnection) applyAndUpdateStateForFile(
	f *fileMigrationState,
	updateSQL string,
	newFileVersion string,
	migration *migration,
) error {
	sql := fmt.Sprintf(`
		BEGIN;
		%v	
		INSERT INTO %v (file, version, migration)
		VALUES ('%v', '%v', %v);
		COMMIT;
	`, updateSQL, d.tableName, f.path, newFileVersion, migration.id)

	result, err := d.executor.Query(sql)

	if err != nil {
		return err
	}

	defer result.Close()

	return nil
}

func (d *SQLDatabaseConnection) rollbackFile(f *fileMigrationState, rollbackSQL string, newVersion string, m *migration) error {
	sql := fmt.Sprintf(`
		BEGIN;
		%v
		DELETE FROM %v WHERE file = '%v'AND migration = '%v';
		COMMIT;
	`, rollbackSQL, d.tableName, f.path, m.id)

	result, err := d.executor.Query(sql)

	if err != nil {
		return err
	}

	defer result.Close()

	return nil
}

func (d *SQLDatabaseConnection) removeMigration(m *migration) error {
	result, err := d.executor.Query(`
		DELETE FROM `+d.tableName+`_migrations WHERE id = $1;
	`, m.id)

	if err != nil {
		return err
	}

	defer result.Close()

	return nil
}

func (d *SQLDatabaseConnection) finishMigration(m *migration) error {
	result, err := d.executor.Query(`
		UPDATE `+d.tableName+`_migrations SET completed = true WHERE id = $1 RETURNING id, completed;
	`, m.id)

	if err != nil {
		return errors.Wrap(err, "unable to mark migration as finished in the database")
	}

	defer result.Close()

	if !result.Next() {
		return errors.New("unable to finish migration because the migration does not exist in the database")
	}

	if err = result.Read(m); err != nil {
		return errors.Wrap(err, "unable to udpate migration state in database")
	}

	return nil
}

func (d *SQLDatabaseConnection) getFilesInMigration(m *migration) ([]fileMigrationState, error) {
	result, err := d.executor.Query(`
		SELECT file, version, migration FROM `+d.tableName+` WHERE migration = $1;
	`, m.id)

	if err != nil {
		return nil, errors.Wrapf(err, "unable to determine files in migration %v", m.id)
	}

	defer result.Close()

	files := make([]fileMigrationState, 0)

	for result.Next() {
		file := fileMigrationState{}
		if err := result.Read(&file); err != nil {
			return nil, errors.Wrapf(err, "unable to deserialize file migration state for migration %v", m.id)
		}
		files = append(files, file)
	}

	if result.Err() != nil {
		return nil, errors.Wrapf(err, "unable to deserialize file migration state for migration %v", m.id)
	}

	return files, nil
}
