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
			definition += lines[i]
		} else if inRollback {
			rollback += lines[i]
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
	relativePath, err := d.getPathRelativeToGitRoot()
	if err != nil {
		return "", err
	}

	// check for uncommitted changes
	cmd := exec.Command("git", "status", relativePath, "--porcelain")
	cmd.Dir = filepath.Dir(d.path)
	fileStatus, err := cmd.Output()

	if len(fileStatus) > 1 {
		return uncommittedVersion, nil
	}

	cmd = exec.Command("git", "log", "--pretty=oneline", "--follow", "-n", "1", d.path)
	cmd.Dir = filepath.Dir(d.path)
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
		relativePath, _ := d.getPathRelativeToGitRoot()
		fmt.Printf("Applying uncommitted file %v, be sure to rollback before committing!\n", relativePath)
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
		currentFileContent, err = ioutil.ReadFile(d.path)
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
	cmd.Dir = filepath.Dir(d.path)
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

	relativePath, err := d.getPathRelativeToGitRoot()
	if err != nil {
		return result, errors.Wrap(err, "unable to get path of file relative to git root")
	}

	cmd := exec.Command("git", "show", version+":"+relativePath)
	cmd.Dir = filepath.Dir(d.path)
	result, err = cmd.Output()

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			fmt.Printf("git command error: %v", string(exitErr.Stderr))
			return result, errors.Wrapf(err, "git command failure: %v", exitErr.Error())
		}
		return result, errors.Wrap(err, "git command failed")
	}

	return result, nil
}

func (d *definitionFile) getPathRelativeToGitRoot() (string, error) {
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	cmd.Dir = filepath.Dir(d.path)
	gitRootPath, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			fmt.Printf("git command error: %v", string(exitErr.Stderr))
			return "", errors.Wrapf(err, "git command failure: %v", exitErr.Error())
		}
		return "", err
	}

	// git evaluates symlinks when searching for the root directory so
	// we need to remove symlinks from the file path so we can properly
	// determine the path of the file relative to the git root
	filePath, err := filepath.EvalSymlinks(d.path)
	if err != nil {
		return "", nil
	}

	if gitRootPath[len(gitRootPath)-1] != filepath.Separator {
		gitRootPath = append(gitRootPath, filepath.Separator)
	}

	// can't use filepath.Rel because it annoyingly prefixes the relative
	// path with "../" which git doesn't like
	relativePath := filePath[len(gitRootPath)-1:]

	return relativePath, nil
}
