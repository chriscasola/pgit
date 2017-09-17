package pgit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSchemaDirectoryReadFromDisk(t *testing.T) {
	s := newSchemaDirectory("./testdata/good_root")

	if err := s.readFromDisk(); err != nil {
		assert.FailNowf(t, "should read from disk", "got error: %v", err)
	}

	assert.Equal(t, 2, len(s.files), "should read two files from the root")
	assert.IsType(t, &changesetFile{}, s.files["changelist_file.sql"], "file should be a changeset file")
	assert.IsType(t, &changesetFile{}, s.files["subdir/changelist_file.sql"], "file should be a changeset file")
	assert.Equal(t, "changelist_file.sql", s.files["changelist_file.sql"].getPath(), "sets file path relative to root")
	assert.Equal(t, "subdir/changelist_file.sql", s.files["subdir/changelist_file.sql"].getPath(), "sets file path relative to root for subdirectory")

	s = newSchemaDirectory("./testdata/bad_root")

	assert.Error(t, s.readFromDisk(), "should detect invalid file type")
}

type MockDatabaseConnection struct {
	mock.Mock
}

func (m *MockDatabaseConnection) readMigrationState() (*migrationState, error) {
	args := m.Called()
	mockMigrationState, _ := args.Get(0).(*migrationState)
	return mockMigrationState, args.Error(1)
}

func (m *MockDatabaseConnection) writeMigrationState(s *migrationState) error {
	args := m.Called(s)
	return args.Error(0)
}

func TestSchemaDirectoryReadMigrationState(t *testing.T) {
	s := newSchemaDirectory("./testdata/good_root")

	mockConnection := &MockDatabaseConnection{}
	expectedMigrationState := &migrationState{}

	mockConnection.On("readMigrationState").Return(expectedMigrationState, nil)

	s.readMigrationState(mockConnection)

	mockConnection.AssertExpectations(t)
	assert.Exactly(t, expectedMigrationState, s.state, "reads the migration state using the database connection")
}

func TestSchemaDirectoryWriteMigrationState(t *testing.T) {
	s := newSchemaDirectory("./testdata/good_root")

	mockConnection := &MockDatabaseConnection{}
	expectedMigrationState := &migrationState{}
	s.state = expectedMigrationState

	mockConnection.On("writeMigrationState", expectedMigrationState).Return(nil)

	if err := s.writeMigrationState(mockConnection); err != nil {
		assert.FailNow(t, "writeMigrationState should not fail")
	}

	mockConnection.AssertExpectations(t)
}
