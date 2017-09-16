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
type schemaFile interface{}

// schemaDirectory represent the directory containing the files
// that define a database schema.
type schemaDirectory struct {
	root  string
	files []schemaFile
}

// readFromDisk reads all of the files from the schema directory and
// creates an in-memory representation of the content that can then be
// applied to the database
func (s *schemaDirectory) readFromDisk() error {
	return s.readDirectory(s.root)
}

func (s *schemaDirectory) readDirectory(path string) error {
	dirContent, err := ioutil.ReadDir(path)

	if err != nil {
		return errors.Wrap(err, "failed to read directory "+path)
	}

	for _, entry := range dirContent {
		if entry.IsDir() {
			err := s.readDirectory(filepath.Join(path, entry.Name()))
			if err != nil {
				return err
			}
		} else {
			err := s.readFile(filepath.Join(path, entry.Name()))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

var fileTypeCommentRegexp = regexp.MustCompile(`-- pgit type=(\S+)`)

func (s *schemaDirectory) readFile(path string) error {
	fileContent, err := ioutil.ReadFile(path)

	if err != nil {
		return errors.Wrap(err, "failed to read file "+path)
	}

	firstLineLength := strings.IndexAny(string(fileContent), "\r\n")

	if firstLineLength == -1 {
		return errors.New("empty schema file " + path)
	}

	firstLine := fileContent[0:firstLineLength]

	tokens := fileTypeCommentRegexp.FindStringSubmatch(string(firstLine))

	if len(tokens) != 2 {
		return errors.New("invalid file annotation for " + path)
	}

	switch tokens[1] {
	case "changeset":
		c := changesetFile{path: path}
		c.parse(fileContent[firstLineLength:])
		s.files = append(s.files, c)
	default:
		return errors.New("unknown file annotation for " + path)
	}

	return nil
}
