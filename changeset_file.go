package pgit

import (
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

const (
	changesetAnnotation = "-- change"
	rollbackAnnotation  = "-- rollback"
)

// changesetFile represents a file using the "changeset" method for describing
// database schema.
type changesetFile struct {
	path       string
	changesets []changeset
}

// changeset contains the SQL for applying and rolling back a changeset
type changeset struct {
	applySQL    string
	rollbackSQL string
}

func (c *changesetFile) getPath() string {
	return c.path
}

func (c *changesetFile) getApplySQL(currentVersion string) (string, string, error) {
	applySQL := ""
	currentVersionNum, err := strconv.ParseUint(currentVersion, 10, 64)

	if err != nil {
		if currentVersion == "" {
			currentVersionNum = 0
		} else {
			return "", "", errors.Wrap(err, "expected integer for currentVersion")
		}
	}

	if currentVersionNum > uint64(len(c.changesets)) {
		return "", "", errors.New("no changesets defined to reach specified version")
	}

	if currentVersionNum == uint64(len(c.changesets)) {
		return "", currentVersion, nil
	}

	for i := currentVersionNum; i < uint64(len(c.changesets)); i++ {
		applySQL += c.changesets[i].applySQL + "\n"
	}

	return applySQL, strconv.FormatInt(int64(len(c.changesets)), 10), nil
}

func (c *changesetFile) getRollbackSQL(currentVersion string) (string, string, error) {
	currentVersionNum, err := strconv.ParseUint(currentVersion, 10, 64)

	if err != nil {
		if currentVersion == "" {
			return "", "0", nil
		}

		return "", "", errors.Wrap(err, "expected integer for currentVersion")
	}

	if currentVersionNum == 0 {
		return "", "0", nil
	}

	return c.changesets[currentVersionNum-1].rollbackSQL, strconv.FormatUint(currentVersionNum-1, 10), nil
}

// readFromFile populates the changeset
func (c *changesetFile) parse(fileContent []byte) error {
	sep := "\r\n"

	lines := strings.Split(string(fileContent), sep)

	if len(lines) == 1 {
		sep = "\n"
		lines = strings.Split(string(fileContent), sep)
	}

	if len(lines) < 1 {
		return errors.New("empty file")
	}

	captureRollback := func(i int) (int, string, error) {
		r := ""

		for j := i; j < len(lines); j++ {
			if lines[j] == rollbackAnnotation {
				return j, r, errors.Errorf("unexpected rollback statement (line %v)", j+1)
			} else if lines[j] != changesetAnnotation {
				r += lines[j] + "\n"
			} else {
				return j, r, nil
			}
		}

		return len(lines), r, nil
	}

	captureChangeset := func(i int) (int, *changeset, error) {
		c := &changeset{}
		j := i
		for ; j < len(lines); j++ {
			if lines[j] == changesetAnnotation {
				return 0, nil, errors.Errorf("unexpected changeset statement (line %v", j+1)
			} else if lines[j] != rollbackAnnotation {
				c.applySQL += lines[j] + "\n"
			} else {
				j, rollback, err := captureRollback(j + 1)
				if err != nil {
					return 0, nil, err
				}
				c.rollbackSQL = rollback
				return j - 1, c, nil
			}
		}

		if c.rollbackSQL == "" {
			return 0, nil, errors.Errorf("missing rollback statement (line %v)", j)
		}

		return len(lines), c, nil
	}

	for i := 0; i < len(lines); i++ {
		if lines[i] == changesetAnnotation {
			newIndex, changeset, err := captureChangeset(i + 1)
			if err != nil {
				return err
			}
			c.changesets = append(c.changesets, *changeset)
			i = newIndex
		} else if lines[i] == rollbackAnnotation {
			return errors.Errorf("unexpected rollback statement (line %v)", i+1)
		} else if lines[i] != "" {
			return errors.New("could not parse file")
		}
	}

	return nil
}
