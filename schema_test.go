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

func (m *MockDatabaseConnection) applyAndUpdateStateForFile(f *fileMigrationState, updateSQL string, newVersion string, mig *migration) error {
	args := m.Called(f, updateSQL, newVersion, mig)
	return args.Error(0)
}

func (m *MockDatabaseConnection) createNewMigration() (*migration, error) {
	args := m.Called()
	mockMigration, _ := args.Get(0).(*migration)
	return mockMigration, args.Error(1)
}

func (m *MockDatabaseConnection) finishMigration(mig *migration) error {
	args := m.Called(mig)
	return args.Error(0)
}

func (m *MockDatabaseConnection) rollbackFile(f *fileMigrationState, rollbackSQL string, newVersion string, lastMigration *migration) error {
	args := m.Called(f, rollbackSQL, newVersion, lastMigration)
	return args.Error(0)
}

func (m *MockDatabaseConnection) removeMigration(mig *migration) error {
	args := m.Called(mig)
	return args.Error(0)
}

func (m *MockDatabaseConnection) getFilesInMigration(mig *migration) ([]fileMigrationState, error) {
	args := m.Called(mig)
	mockFileState, _ := args.Get(0).([]fileMigrationState)
	return mockFileState, args.Error(1)
}

func TestSchemaDirectoryReadMigrationState(t *testing.T) {
	s := newSchemaDirectory("./testdata/good_root")

	mockConnection := &MockDatabaseConnection{}
	expectedMigrationState := &migrationState{}

	mockConnection.On("readMigrationState").Return(expectedMigrationState, nil)

	s.readMigrationState(mockConnection)

	mockConnection.AssertExpectations(t)
	assert.Exactly(t, expectedMigrationState, s.state, "reads the migration state using the database connection")

	mockConnection.AssertExpectations(t)
}

func TestSchemaDirectoryApplyLatest(t *testing.T) {
	s := newSchemaDirectory("./testdata/good_root")

	mockConnection := &MockDatabaseConnection{}
	expectedMigrationState := &migrationState{
		fileStates: make(map[string]*fileMigrationState),
	}

	mockMigration := &migration{id: 1, completed: false}

	mockConnection.On("readMigrationState").Return(expectedMigrationState, nil)
	mockConnection.On("createNewMigration").Return(mockMigration, nil)

	mockConnection.On(
		"applyAndUpdateStateForFile",
		&fileMigrationState{
			version: "",
			path:    "changelist_file.sql",
		},
		"CREATE TABLE test_table (\n    col_a text\n);\n\n\n",
		"1",
		mockMigration,
	).Return(nil)

	mockConnection.On("finishMigration", mockMigration).Return(nil)

	assert.NoError(t, s.applyLatest(mockConnection), "should apply schema successfully")

	mockConnection.AssertExpectations(t)
}

func TestSchemaDirectoryRollback(t *testing.T) {
	s := newSchemaDirectory("./testdata/good_root")
	mockConnection := &MockDatabaseConnection{}

	expectedMigration := migration{id: 1, completed: true}

	expectedMigrationState := &migrationState{
		fileStates:    make(map[string]*fileMigrationState),
		lastMigration: &expectedMigration,
	}

	fileState := &fileMigrationState{
		version:   "1",
		path:      "changelist_file.sql",
		migration: 1,
	}

	expectedMigrationState.fileStates["changelist_file.sql"] = fileState
	expectedFilesInMigration := []fileMigrationState{*fileState}

	mockConnection.On("readMigrationState").Return(expectedMigrationState, nil)

	mockConnection.On("getFilesInMigration", &expectedMigration).Return(expectedFilesInMigration, nil)

	mockConnection.On("rollbackFile", fileState, "DROP TABLE test_table\n\n", "0", &expectedMigration).Return(nil)

	mockConnection.On("removeMigration", &expectedMigration).Return(nil)

	assert.NoError(t, s.rollback(mockConnection), "should rollback successfully")
}
