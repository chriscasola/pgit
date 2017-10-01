package pgit

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

const (
	uncommittedVersion = "uncommitted"
)

type definitionFile struct {
	gitRoot string
	path    string
	content []byte
}

func (d *definitionFile) getPath() string {
	return d.path
}

func (d *definitionFile) parse(fileContent []byte) (string, string, error) {
	sep := "\r\n"

	lines := strings.Split(string(fileContent), sep)

	if len(lines) == 1 {
		sep = "\n"
		lines = strings.Split(string(fileContent), sep)
	}

	if len(lines) < 1 {
		return "", "", errors.New("empty file")
	}

	definition, rollback := "", ""
	inDefinition, inRollback := false, false

	for i := 0; i < len(lines); i++ {
		if lines[i] == definitionAnnotation {
			inDefinition = true
		} else if lines[i] == rollbackAnnotation {
			inRollback = true
			inDefinition = false
		} else if inDefinition {
			definition += (lines[i] + sep)
		} else if inRollback {
			rollback += (lines[i] + sep)
		}
	}

	if len(definition) == 0 {
		return "", "", errors.New("must specify a definition")
	}

	if len(rollback) == 0 {
		return "", "", errors.New("must specify a rollback")
	}

	return definition, rollback, nil
}

var shaRegexp = regexp.MustCompile(`^([a-fA-F0-9]{40})\s*`)

func (d *definitionFile) getCurrentSHA() (string, error) {
	// check for uncommitted changes
	cmd := exec.Command("git", "status", d.path, "--porcelain")
	cmd.Dir = d.gitRoot
	fileStatus, err := cmd.Output()

	if err != nil || strings.Contains(string(fileStatus), d.path) {
		return uncommittedVersion, nil
	}

	cmd = exec.Command("git", "log", "--pretty=oneline", "--follow", "-n", "1", d.path)
	cmd.Dir = d.gitRoot
	result, err := cmd.Output()

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			fmt.Printf("git command error: %v", string(exitErr.Stderr))
			return "", errors.Wrapf(err, "git command failure: %v", exitErr.Error())
		}
		return "", errors.Wrap(err, "git command failed")
	}

	tokens := shaRegexp.FindStringSubmatch(string(result))

	if len(tokens) != 2 {
		return "", errors.New("unable to parse git log")
	}

	return tokens[1], nil
}

func (d *definitionFile) getApplySQL(currentVersion string) (string, string, error) {
	if currentVersion == uncommittedVersion {
		fmt.Println("Cannot apply migration to an uncommitted version, please rollback the last migration first!")
		return "", "", errors.New("cannot apply migration to an uncommitted version")
	}

	fileVersion, err := d.getCurrentSHA()

	if err != nil {
		return "", "", errors.Wrap(err, "unable to get current SHA of file")
	}

	if fileVersion == currentVersion {
		return "", currentVersion, nil
	}

	if fileVersion == uncommittedVersion {
		fmt.Printf("Applying uncommitted file %v, be sure to rollback before committing!\n", d.path)
	}

	apply, _, err := d.parse(d.content)

	if err != nil {
		return "", "", errors.Wrap(err, "unable to parse file to get apply SQL")
	}

	if currentVersion == "" {
		return strings.TrimSpace(apply), fileVersion, nil
	}

	prevRevisionContent, err := d.getFileAtCommit(currentVersion)

	if err != nil {
		return "", "", errors.Wrap(err, "unable to get previous version of file")
	}

	_, rollback, err := d.parse(prevRevisionContent)

	if err != nil {
		return "", "", errors.Wrap(err, "unable to get rollback SQL from previous version of file")
	}

	return strings.TrimSpace(rollback) + ";\n" + strings.TrimSpace(apply), fileVersion, nil
}

func (d *definitionFile) getRollbackSQL(currentVersion string) (string, string, error) {
	if currentVersion == "" {
		return "", "", nil
	}

	commits, err := d.getFileCommits()
	if err != nil {
		return "", "", err
	}

	previousVersion := ""
	currentFileContent := make([]byte, 0)
	previousFileContent := make([]byte, 0)

	if currentVersion == uncommittedVersion {
		currentFileContent, err = ioutil.ReadFile(filepath.Join(d.gitRoot, d.path))
		if err != nil {
			return "", "", err
		}
	} else {
		currentFileContent, err = d.getFileAtCommit(currentVersion)
	}

	if currentVersion == uncommittedVersion {
		if len(commits) > 0 {
			previousVersion = commits[0]
			previousFileContent, err = d.getFileAtCommit(previousVersion)
			if err != nil {
				return "", "", err
			}
		}
	} else {
		for i, c := range commits {
			if c == currentVersion && i < len(commits)-1 {
				previousVersion = commits[i+1]
				previousFileContent, err = d.getFileAtCommit(previousVersion)
				if err != nil {
					return "", "", err
				}
				break
			}
		}
	}

	apply, rollback := "", ""

	if len(previousFileContent) > 0 {
		apply, _, err = d.parse(previousFileContent)
		if err != nil {
			return "", "", err
		}
	}

	if len(currentFileContent) > 0 {
		_, rollback, err = d.parse(currentFileContent)
		if err != nil {
			return "", "", err
		}
	}

	if len(previousFileContent) > 0 && len(currentFileContent) > 0 {
		return strings.TrimSpace(rollback) + ";\n" + strings.TrimSpace(apply), previousVersion, nil
	}

	return strings.TrimSpace(rollback), strings.TrimSpace(previousVersion), nil
}

func (d *definitionFile) getFileCommits() ([]string, error) {
	cmd := exec.Command("git", "log", "--format=%H", "--follow", d.path)
	cmd.Dir = d.gitRoot
	history, err := cmd.Output()
	if err != nil {
		return make([]string, 0), nil
	}

	result := make([]string, 0)
	for _, s := range strings.Split(string(history), "\n") {
		trimmed := strings.TrimSpace(s)
		if len(trimmed) > 0 {
			result = append(result, strings.TrimSpace(s))
		}
	}

	return result, nil
}

func (d *definitionFile) getFileAtCommit(version string) ([]byte, error) {
	var result []byte

	cmd := exec.Command("git", "show", version+":"+d.path)
	cmd.Dir = d.gitRoot
	result, err := cmd.Output()

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			fmt.Printf("git command error: %v", string(exitErr.Stderr))
			return result, errors.Wrapf(err, "git command failure: %v", exitErr.Error())
		}
		return result, errors.Wrap(err, "git command failed")
	}

	return result, nil
}
