package pgit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchemaDirectoryReadFromDisk(t *testing.T) {
	s := schemaDirectory{root: "./testdata/good_root"}

	if err := s.readFromDisk(); err != nil {
		assert.FailNowf(t, "should read from disk", "got error: %v", err)
	}

	s = schemaDirectory{root: "./testdata/bad_root"}

	assert.Error(t, s.readFromDisk(), "should detect invalid file type")
}
