package pgit

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefinitionFileParse(t *testing.T) {
	fileContent, err := ioutil.ReadFile("./testdata/definition_style.sql")

	d := definitionFile{}

	if err != nil {
		assert.FailNowf(t, "unable to read test data", "got error: %v", err)
	}

	apply, rollback, err := d.parse(fileContent)

	assert.NoError(t, err, "should parse the file")

	assert.Equal(t, apply, "CREATE TABLE test_table (col_a text);")
	assert.Equal(t, rollback, "DROP TABLE test_table;")
}

func TestDefinitionFileGitOps(t *testing.T) {
	name, err := ioutil.TempDir("", "pgit-test")

	assert.NoError(t, err, "failed to create temp directory for test repo")

	defer func() {
		assert.NoError(t, os.RemoveAll(name), "failed to remove temp directory")
	}()

	runCommand(t, name, "git", "init")

	fileName := filepath.Join(name, "/test_def.sql")

	file, err := os.Create(fileName)

	assert.NoError(t, err, "failed to create definition file")

	defer func() {
		assert.NoError(t, file.Close(), "failed to close definition file")
	}()

	writeTestFile(file, `
-- definition
CREATE FUNCTION func (text, text);

-- rollback
DROP FUNCTION func (text, text);
	`)

	runCommand(t, name, "git", "add", "-A")
	runCommand(t, name, "git", "commit", "-m", `"commit 1"`)

	shaTest := regexp.MustCompile("^[0-9a-f]{40}$")
	var firstVersion string

	t.Run("getCurrentSHA", func(t *testing.T) {
		d := definitionFile{path: fileName}

		var err error
		firstVersion, err = d.getCurrentSHA()

		assert.NoError(t, err, "should not error when getting file sha")
		assert.Regexp(t, shaTest, firstVersion, "sha should be a git sha")
	})

	t.Run("get apply SQL for first version", func(t *testing.T) {
		fileContent, err := ioutil.ReadFile(fileName)
		assert.NoError(t, err, "failed to read test file")

		d := definitionFile{path: fileName, content: fileContent}

		sql, version, err := d.getApplySQL("")

		assert.NoError(t, err, "should not error when getting apply SQL")
		assert.Equal(
			t,
			"CREATE FUNCTION func (text, text);",
			sql,
			"should return apply SQL",
		)
		assert.Equal(t, firstVersion, version, "version should be current git sha")
	})

	writeTestFile(file, `
-- definition
CREATE FUNCTION func (text, text, text);

-- rollback
DROP FUNCTION func (text, text, text);
	`)

	runCommand(t, name, "git", "add", "-A")
	runCommand(t, name, "git", "commit", "-m", `"commit 2"`)

	t.Run("get apply SQL for second version", func(t *testing.T) {
		fileContent, err := ioutil.ReadFile(fileName)
		assert.NoError(t, err, "failed to read test file")

		d := definitionFile{path: fileName, content: fileContent}

		sql, version, err := d.getApplySQL(firstVersion)

		assert.NoError(t, err, "should not error when getting apply SQL")
		assert.Equal(
			t,
			"DROP FUNCTION func (text, text);\t;\\nCREATE FUNCTION func (text, text, text);",
			sql,
			"should return apply SQL",
		)
		assert.Regexp(t, shaTest, version, "version should be a git sha")
		assert.NotEqual(t, firstVersion, version, "should return new git sha")
	})
}

func runCommand(t *testing.T, dir string, command string, args ...string) {
	cmd := exec.Command(command, args...)
	cmd.Dir = dir
	err := cmd.Run()
	if exitErr, ok := err.(*exec.ExitError); ok {
		fmt.Printf("git command error: %v", string(exitErr.Stderr))
	}
	assert.NoErrorf(t, err, "command failed: %v", command)
}

func writeTestFile(file *os.File, fileContent string) {
	file.Truncate(0)
	file.Seek(0, 0)
	file.WriteString(fileContent)
	file.Sync()
}
