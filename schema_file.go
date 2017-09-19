package pgit

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

// schemaFile represents a file containing descriptions of a database schema
// that can be applied to the database
type schemaFile interface {
	getApplySQL(currentVersion string) (string, string, error)
	getRollbackSQL(currentVersion string) (string, string, error)
	getPath() string
}

// schemaDirectory represents the directory containing the files
// that define a database schema.
type schemaDirectory struct {
	root  string
	files map[string]schemaFile
	state *migrationState
}

func newSchemaDirectory(root string) *schemaDirectory {
	return &schemaDirectory{root: root, files: make(map[string]schemaFile), state: &migrationState{}}
}

func (s *schemaDirectory) rollback(db DatabaseConnection) error {
	if err := s.readFromDisk(); err != nil {
		return errors.Wrap(err, "failed to populate schema from disk")
	}

	if err := s.readMigrationState(db); err != nil {
		return errors.Wrap(err, "failed to read migration state")
	}

	filesInLastMigration, err := db.getFilesInMigration(s.state.lastMigration)

	if err != nil {
		return errors.Wrap(err, "unable to determine files involved in last migration")
	}

	for _, file := range filesInLastMigration {
		rollbackSQL, newVersion, err := s.files[file.path].getRollbackSQL(file.version)

		if err != nil {
			fmt.Printf("WARNING: failed to get SQL for rolling back update (file=%v version=%v msg=%v)\n", file.path, file.version, err)
			continue
		}

		if err = db.rollbackFile(&file, rollbackSQL, newVersion, s.state.lastMigration); err != nil {
			fmt.Printf("WARNING: unable to rollback file (file=%v version=%v msg=%v)\n", file.path, newVersion, err)
			return errors.Wrap(err, "unable to rollback changes to file")
		}
	}

	if err = db.removeMigration(s.state.lastMigration); err != nil {
		return errors.Wrap(err, "unable to remove migration after rolling back files")
	}

	return nil
}

func (s *schemaDirectory) applyLatest(db DatabaseConnection) error {
	if err := s.readFromDisk(); err != nil {
		return errors.Wrap(err, "failed to populate schema from disk")
	}

	if err := s.readMigrationState(db); err != nil {
		return errors.Wrap(err, "failed to read migration state")
	}

	var migration *migration

	for filePath, file := range s.files {
		fileState, ok := s.state.fileStates[filePath]

		if !ok {
			s.state.fileStates[filePath] = &fileMigrationState{version: "", path: filePath}
			fileState = s.state.fileStates[filePath]
		}

		sql, newVersion, err := file.getApplySQL(fileState.version)

		if err != nil {
			fmt.Printf("WARNING: failed to get SQL for applying update (file=%v version=%v msg=%v)\n", file.getPath(), fileState.version, err)
			continue
		}

		if newVersion == fileState.version {
			continue
		}

		if migration == nil {
			migration, err = db.createNewMigration()
			if err != nil {
				return err
			}
		}

		if err = db.applyAndUpdateStateForFile(fileState, sql, newVersion, migration); err != nil {
			fmt.Printf("WARNING: unable to apply update for file (file=%v version=%v msg=%v)\n", file.getPath(), newVersion, err)
			continue
		}
	}

	if migration != nil {
		return db.finishMigration(migration)
	}

	return nil
}

// readMigrationState reads the current migration state of the database using
// the provided DatabaseConnection
func (s *schemaDirectory) readMigrationState(d DatabaseConnection) error {
	m, err := d.readMigrationState()
	if err != nil {
		return err
	}
	s.state = m
	return nil
}

// readFromDisk reads all of the files from the schema directory and
// creates an in-memory representation of the content that can then be
// applied to the database
func (s *schemaDirectory) readFromDisk() error {
	return s.readDirectory(s.root, "")
}

func (s *schemaDirectory) readDirectory(path, relativePath string) error {
	dirContent, err := ioutil.ReadDir(path)

	if err != nil {
		return errors.Wrap(err, "failed to read directory "+path)
	}

	for _, entry := range dirContent {
		if entry.IsDir() {
			err := s.readDirectory(filepath.Join(path, entry.Name()), filepath.Join(relativePath, entry.Name()))
			if err != nil {
				return err
			}
		} else {
			err := s.readFile(filepath.Join(path, entry.Name()), filepath.Join(relativePath, entry.Name()))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

var fileTypeCommentRegexp = regexp.MustCompile(`-- pgit type=(\S+)`)

func (s *schemaDirectory) readFile(path, relativePath string) error {
	fileContent, err := ioutil.ReadFile(path)

	if err != nil {
		return errors.Wrap(err, "failed to read file "+path)
	}

	firstLineLength := strings.IndexAny(string(fileContent), "\r\n")

	if firstLineLength == -1 {
		return errors.New("empty schema file " + relativePath)
	}

	firstLine := fileContent[0:firstLineLength]

	tokens := fileTypeCommentRegexp.FindStringSubmatch(string(firstLine))

	if len(tokens) != 2 {
		return errors.New("invalid file annotation for " + relativePath)
	}

	switch tokens[1] {
	case "changeset":
		c := changesetFile{path: relativePath}
		if err := c.parse(fileContent[firstLineLength:]); err != nil {
			return err
		}
		s.files[relativePath] = &c
	default:
		return errors.New("unknown file annotation for " + relativePath)
	}

	return nil
}
