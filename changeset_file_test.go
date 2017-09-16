package pgit

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChangesetFileReadFromDisk(t *testing.T) {
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

func TestChangesetInvalidReadFromDisk(t *testing.T) {
	c := changesetFile{}
	fileContent, err := ioutil.ReadFile("./testdata/change_style_a_invalid.sql")

	if err != nil {
		assert.FailNowf(t, "error reading test data", "got error: %v", err)
	}

	assert.Error(t, c.parse(fileContent), "should detect invalid formatting of file")
}
