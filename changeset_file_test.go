package pgit

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChangesetFileParse(t *testing.T) {
	fileContent, err := ioutil.ReadFile("./testdata/change_style_a.sql")

	c := changesetFile{}

	if err != nil {
		assert.FailNowf(t, "unable to read test data", "got error: %v", err)
	}

	if err := c.parse(fileContent); err != nil {
		assert.FailNowf(t, "should read from disk", "got error: %v", err)
	}

	if len(c.changesets) != 2 {
		assert.FailNow(t, "should read all changesets from file")
	}

	assert.Equal(t, changeset{
		applySQL:    "CREATE TABLE awesome_table (\n    col_a text,\n    col_b text\n);\n\n",
		rollbackSQL: "DROP TABLE awesome_table;\n\n",
	}, c.changesets[0], "should populate first changeset")

	assert.Equal(t, changeset{
		applySQL:    "ALTER TABLE awesome_table ADD COLUMN col_c text;\n\n",
		rollbackSQL: "ALTER TABLE awesome_table DROP COLUMN col_c;\n\n",
	}, c.changesets[1], "should populate second changeset")
}

func TestChangesetInvalidParse(t *testing.T) {
	c := changesetFile{}
	fileContent, err := ioutil.ReadFile("./testdata/change_style_a_invalid.sql")

	if err != nil {
		assert.FailNowf(t, "error reading test data", "got error: %v", err)
	}

	assert.Error(t, c.parse(fileContent), "should detect invalid formatting of file")
}

func TestChangesetGetApplySQL(t *testing.T) {
	fileContent, err := ioutil.ReadFile("./testdata/change_style_a.sql")

	c := changesetFile{}

	if err != nil {
		assert.FailNowf(t, "unable to read test data", "got error: %v", err)
	}

	if err := c.parse(fileContent); err != nil {
		assert.FailNowf(t, "should read from disk", "got error: %v", err)
	}

	applyAllSQL, newVersion, err := c.getApplySQL("")

	if err != nil {
		assert.FailNowf(t, "should return apply SQL", "got error: %v", err)
	}

	assert.Equal(
		t,
		"CREATE TABLE awesome_table (\n    col_a text,\n    col_b text\n);\n\n\nALTER TABLE awesome_table ADD COLUMN col_c text;\n\n\n",
		applyAllSQL,
		"should return the SQL to apply all of the changes in the file",
	)
	assert.Equal(t, "2", newVersion, "should return the new version of the schema defined in the file")

	applyAllSQL, newVersion, err = c.getApplySQL("1")

	if err != nil {
		assert.FailNowf(t, "should return apply SQL", "got error: %v", err)
	}

	assert.Equal(
		t,
		"ALTER TABLE awesome_table ADD COLUMN col_c text;\n\n\n",
		applyAllSQL,
		"should return the SQL to apply missing changes in the file",
	)
	assert.Equal(t, "2", newVersion, "should return the new version of the schema defined in the file")
}

func TestChangesetGetRollbackSQL(t *testing.T) {
	fileContent, err := ioutil.ReadFile("./testdata/change_style_a.sql")

	c := changesetFile{}

	if err != nil {
		assert.FailNowf(t, "unable to read test data", "got error: %v", err)
	}

	if err := c.parse(fileContent); err != nil {
		assert.FailNowf(t, "should read from disk", "got error: %v", err)
	}

	rollbackSQL, newVersion, err := c.getRollbackSQL("")

	if err != nil {
		assert.FailNowf(t, "should return rollback SQL", "got error: %v", err)
	}

	assert.Equal(t, "", rollbackSQL, "should return rollback SQL when there is nothing to roll back")
	assert.Equal(t, "0", newVersion, "should return zero as the version if rolled all the way back")

	rollbackSQL, newVersion, err = c.getRollbackSQL("2")

	if err != nil {
		assert.FailNowf(t, "should return rollback SQL", "got error: %v", err)
	}

	assert.Equal(
		t,
		"ALTER TABLE awesome_table DROP COLUMN col_c;\n\n",
		rollbackSQL,
		"should return rollback SQL for version 2",
	)
	assert.Equal(t, "1", newVersion, "should return 1 as the version if rolled back 2")

	rollbackSQL, newVersion, err = c.getRollbackSQL("1")

	if err != nil {
		assert.FailNowf(t, "should return rollback SQL", "got error: %v", err)
	}

	assert.Equal(
		t,
		"DROP TABLE awesome_table;\n\n",
		rollbackSQL,
		"should return rollback SQL for version 1",
	)
	assert.Equal(t, "0", newVersion, "should return 0 as the version if rolled back 1")
}
