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

	assert.Equal(t, "CREATE TABLE test_table (col_a text);\n\n", apply)
	assert.Equal(t, "DROP TABLE test_table;\n\n", rollback)
}

func TestDefinitionFileGitOps(t *testing.T) {
	gitRoot, err := ioutil.TempDir("", "pgit-test")

	assert.NoError(t, err, "failed to create temp directory for test repo")

	defer func() {
		assert.NoError(t, os.RemoveAll(gitRoot), "failed to remove temp directory")
	}()

	runCommand(t, gitRoot, "git", "init")
	runCommand(t, gitRoot, "git", "config", "user.email", "test@test.com")
	runCommand(t, gitRoot, "git", "config", "user.name", "Test Name")

	fileName := "test_def.sql"
	filePath := filepath.Join(gitRoot, "/test_def.sql")

	file, err := os.Create(filePath)

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

	t.Run("get apply SQL for empty repo", func(t *testing.T) {
		fileContent, err := ioutil.ReadFile(filePath)
		assert.NoError(t, err, "failed to read test file")

		d := definitionFile{path: fileName, content: fileContent, gitRoot: gitRoot}

		sql, version, err := d.getApplySQL("")

		assert.NoError(t, err, "should not error when getting apply SQL")
		assert.Equal(
			t,
			"CREATE FUNCTION func (text, text);",
			sql,
			"should return apply SQL",
		)
		assert.Equal(t, uncommittedVersion, version, "version should be uncommitted")
	})

	t.Run("get rollback SQL for empty repo", func(t *testing.T) {
		fileContent, err := ioutil.ReadFile(filePath)
		assert.NoError(t, err, "failed to read test file")

		d := definitionFile{path: fileName, content: fileContent, gitRoot: gitRoot}

		sql, version, err := d.getRollbackSQL(uncommittedVersion)

		assert.NoError(t, err, "should not error when getting rollback SQL")
		assert.Equal(
			t,
			"DROP FUNCTION func (text, text);",
			sql,
			"should return rollback SQL",
		)
		assert.Equal(t, "", version, "version should be empty")
	})

	runCommand(t, gitRoot, "git", "add", "-A")
	runCommand(t, gitRoot, "git", "commit", "-m", `"commit 1"`)

	shaTest := regexp.MustCompile("^[0-9a-f]{40}$")
	var firstVersion string

	t.Run("getCurrentSHA", func(t *testing.T) {
		d := definitionFile{path: fileName, gitRoot: gitRoot}

		var err error
		firstVersion, err = d.getCurrentSHA()

		assert.NoError(t, err, "should not error when getting file sha")
		assert.Regexp(t, shaTest, firstVersion, "sha should be a git sha")
	})

	t.Run("get apply SQL for first version", func(t *testing.T) {
		fileContent, err := ioutil.ReadFile(filePath)
		assert.NoError(t, err, "failed to read test file")

		d := definitionFile{path: fileName, content: fileContent, gitRoot: gitRoot}

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

	t.Run("get rollback SQL for first version", func(t *testing.T) {
		fileContent, err := ioutil.ReadFile(filePath)
		assert.NoError(t, err, "failed to read test file")

		d := definitionFile{path: fileName, content: fileContent, gitRoot: gitRoot}

		sql, version, err := d.getRollbackSQL(firstVersion)

		assert.NoError(t, err, "should not error when getting rollback SQL")
		assert.Equal(
			t,
			"DROP FUNCTION func (text, text);",
			sql,
			"should return rollback SQL",
		)
		assert.Equal(t, "", version, "version should be current git sha")
	})

	writeTestFile(file, `
-- definition
CREATE FUNCTION func (text, text, text);

-- rollback
DROP FUNCTION func (text, text, text);
	`)

	runCommand(t, gitRoot, "git", "add", "-A")
	runCommand(t, gitRoot, "git", "commit", "-m", `"commit 2"`)

	var secondVersion string

	t.Run("get apply SQL for second version", func(t *testing.T) {
		fileContent, err := ioutil.ReadFile(filePath)
		assert.NoError(t, err, "failed to read test file")

		d := definitionFile{path: fileName, content: fileContent, gitRoot: gitRoot}

		var sql string
		sql, secondVersion, err = d.getApplySQL(firstVersion)

		assert.NoError(t, err, "should not error when getting apply SQL")
		assert.Equal(
			t,
			"DROP FUNCTION func (text, text);;\nCREATE FUNCTION func (text, text, text);",
			sql,
			"should return apply SQL",
		)
		assert.Regexp(t, shaTest, secondVersion, "version should be a git sha")
		assert.NotEqual(t, firstVersion, secondVersion, "should return new git sha")
	})

	t.Run("get rollback SQL for second version", func(t *testing.T) {
		fileContent, err := ioutil.ReadFile(filePath)
		assert.NoError(t, err, "failed to read test file")

		d := definitionFile{path: fileName, content: fileContent, gitRoot: gitRoot}

		sql, version, err := d.getRollbackSQL(secondVersion)

		assert.NoError(t, err, "should not error when getting rollback SQL")
		assert.Equal(
			t,
			"DROP FUNCTION func (text, text, text);;\nCREATE FUNCTION func (text, text);",
			sql,
			"should return rollback SQL",
		)
		assert.Equal(t, firstVersion, version, "version should be current git sha")
	})

	writeTestFile(file, `
-- definition
CREATE FUNCTION func (text, text, text, text);

-- rollback
DROP FUNCTION func (text, text, text, text);
	`)

	t.Run("get apply SQL for uncommitted version", func(t *testing.T) {
		fileContent, err := ioutil.ReadFile(filePath)
		assert.NoError(t, err, "failed to read test file")

		d := definitionFile{path: fileName, content: fileContent, gitRoot: gitRoot}

		sql, version, err := d.getApplySQL(secondVersion)

		assert.NoError(t, err, "should not error when getting apply SQL")
		assert.Equal(
			t,
			"DROP FUNCTION func (text, text, text);;\nCREATE FUNCTION func (text, text, text, text);",
			sql,
			"should return apply SQL",
		)
		assert.Equal(t, uncommittedVersion, version, `version should be "uncommitted"`)
	})

	t.Run("get rollback SQL for uncommitted version", func(t *testing.T) {
		fileContent, err := ioutil.ReadFile(filePath)
		assert.NoError(t, err, "failed to read test file")

		d := definitionFile{path: fileName, content: fileContent, gitRoot: gitRoot}

		sql, version, err := d.getRollbackSQL(uncommittedVersion)

		assert.NoError(t, err, "should not error when getting rollback SQL")
		assert.Equal(
			t,
			"DROP FUNCTION func (text, text, text, text);;\nCREATE FUNCTION func (text, text, text);",
			sql,
			"should return rollback SQL",
		)
		assert.Equal(t, secondVersion, version, "should rollback to second version")
	})

	t.Run("get apply SQL for second uncommitted version", func(t *testing.T) {
		fileContent, err := ioutil.ReadFile(filePath)
		assert.NoError(t, err, "failed to read test file")

		d := definitionFile{path: fileName, content: fileContent, gitRoot: gitRoot}

		_, _, err = d.getApplySQL(uncommittedVersion)

		assert.Error(t, err, "should not allow a migration when the currently applied version is uncommitted")
	})

	secondFileName := "test_def2.sql"
	secondFilePath := filepath.Join(gitRoot, "/test_def2.sql")

	secondFile, err := os.Create(secondFilePath)

	assert.NoError(t, err, "failed to create second definition file")

	defer func() {
		assert.NoError(t, secondFile.Close(), "failed to close second definition file")
	}()

	writeTestFile(secondFile, `
-- definition
CREATE FUNCTION func (text, text, text, text);

-- rollback
DROP FUNCTION func (text, text, text, text);
	`)

	t.Run("get apply SQL for untracked file", func(t *testing.T) {
		fileContent, err := ioutil.ReadFile(secondFilePath)
		assert.NoError(t, err, "failed to read test file")

		d := definitionFile{path: secondFileName, content: fileContent, gitRoot: gitRoot}

		sql, version, err := d.getApplySQL("")

		assert.NoError(t, err, "should not error when getting apply SQL")
		assert.Equal(
			t,
			"CREATE FUNCTION func (text, text, text, text);",
			sql,
			"should return apply SQL",
		)
		assert.Equal(t, uncommittedVersion, version, `version should be "uncommitted"`)
	})

	t.Run("get rollback SQL for untracked file", func(t *testing.T) {
		fileContent, err := ioutil.ReadFile(secondFilePath)
		assert.NoError(t, err, "failed to read test file")

		d := definitionFile{path: secondFileName, content: fileContent, gitRoot: gitRoot}

		sql, version, err := d.getRollbackSQL(uncommittedVersion)

		assert.NoError(t, err, "should not error when getting rollback SQL")
		assert.Equal(
			t,
			"DROP FUNCTION func (text, text, text, text);",
			sql,
			"should return rollback SQL",
		)
		assert.Equal(t, "", version, "should rollback to empty version")
	})

	t.Run("get apply SQL for already untracked file", func(t *testing.T) {
		fileContent, err := ioutil.ReadFile(secondFilePath)
		assert.NoError(t, err, "failed to read test file")

		d := definitionFile{path: secondFileName, content: fileContent, gitRoot: gitRoot}

		_, _, err = d.getApplySQL(uncommittedVersion)

		assert.Error(t, err, "should error when getting apply SQL")
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
