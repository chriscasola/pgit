package pgit

import (
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

func (s *schemaDirectory) applyLatest(db DatabaseConnection) error {
	if err := s.readFromDisk(); err != nil {
		return errors.Wrap(err, "failed to populate schema from disk")
	}

	if err := s.readMigrationState(db); err != nil {
		return errors.Wrap(err, "failed to read migration state")
	}

	for filePath, file := range s.files {
		fileState, ok := s.state.fileStates[filePath]

		if !ok {
			s.state.fileStates[filePath] = &fileMigrationState{version: "", path: filePath}
			fileState = s.state.fileStates[filePath]
		}

		sql, newVersion, err := file.getApplySQL(fileState.version)

		if err != nil {
			return errors.Wrapf(err, "failed to get apply SQL for %v", file.getPath())
		}

		if err = db.applyAndUpdateStateForFile(fileState, sql, newVersion); err != nil {
			return errors.Wrapf(err, "failed to update state for %v", file.getPath())
		}
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
