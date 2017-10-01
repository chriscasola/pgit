package pgit

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSchemaDirectory(t *testing.T) {
	wd, err := os.Getwd()
	assert.NoError(t, err, "failed to get working directory")
	cmd := exec.Command("git", "init")
	cmd.Dir = filepath.Join(wd, "testdata/good_root")
	assert.NoError(t, cmd.Run(), "failed to initailize git repo in good_root")
	defer func() {
		os.RemoveAll("./testdata/good_root/.git")
	}()

	cmd = exec.Command("git", "init")
	cmd.Dir = filepath.Join(wd, "testdata/bad_root")
	assert.NoError(t, cmd.Run(), "failed to initailize git repo in bad_root")
	defer func() {
		os.RemoveAll("./testdata/bad_root/.git")
	}()

	t.Run("read schema directory from disk", func(t *testing.T) {
		s, err := newSchemaDirectory("./testdata/good_root/migrations")
		assert.NoError(t, err, "failed to create test schema directory")

		if err := s.readFromDisk(); err != nil {
			assert.FailNowf(t, "should read from disk", "got error: %v", err)
		}

		assert.Equal(t, 2, len(s.files), "should read two files from the root")
		assert.IsType(t, &changesetFile{}, s.files["migrations/changelist_file.sql"], "file should be a changeset file")
		assert.IsType(t, &changesetFile{}, s.files["migrations/subdir/changelist_file.sql"], "file should be a changeset file")
		assert.Equal(t, "migrations/changelist_file.sql", s.files["migrations/changelist_file.sql"].getPath(), "sets file path relative to git root")
		assert.Equal(t, "migrations/subdir/changelist_file.sql", s.files["migrations/subdir/changelist_file.sql"].getPath(), "sets file path relative to git root for subdirectory")

		s, err = newSchemaDirectory("./testdata/bad_root/migrations")
		assert.NoError(t, err, "failed to create test schema directory")

		assert.Error(t, s.readFromDisk(), "should detect invalid file type")
	})

	t.Run("read migration state", func(t *testing.T) {
		s, err := newSchemaDirectory("./testdata/good_root/migrations")
		assert.NoError(t, err, "failed to create test schema directory")

		mockConnection := &MockDatabaseConnection{}
		expectedMigrationState := &migrationState{}

		mockConnection.On("readMigrationState").Return(expectedMigrationState, nil)

		s.readMigrationState(mockConnection)

		mockConnection.AssertExpectations(t)
		assert.Exactly(t, expectedMigrationState, s.state, "reads the migration state using the database connection")

		mockConnection.AssertExpectations(t)
	})

	t.Run("apply latest", func(t *testing.T) {
		s, err := newSchemaDirectory("./testdata/good_root/migrations")
		assert.NoError(t, err, "failed to create test schema directory")

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
				path:    "migrations/changelist_file.sql",
			},
			"CREATE TABLE test_table (\n    col_a text\n);\n\n\n",
			"1",
			mockMigration,
		).Return(nil)

		mockConnection.On("finishMigration", mockMigration).Return(nil)

		assert.NoError(t, s.applyLatest(mockConnection), "should apply schema successfully")

		mockConnection.AssertExpectations(t)
	})

	t.Run("rollback", func(t *testing.T) {
		s, err := newSchemaDirectory("./testdata/good_root/migrations")
		assert.NoError(t, err, "failed to create test schema directory")
		mockConnection := &MockDatabaseConnection{}

		expectedMigration := migration{id: 1, completed: true}

		expectedMigrationState := &migrationState{
			fileStates:    make(map[string]*fileMigrationState),
			lastMigration: &expectedMigration,
		}

		fileState := &fileMigrationState{
			version:   "1",
			path:      "migrations/changelist_file.sql",
			migration: 1,
		}

		expectedMigrationState.fileStates["migrations/changelist_file.sql"] = fileState
		expectedFilesInMigration := []fileMigrationState{*fileState}

		mockConnection.On("readMigrationState").Return(expectedMigrationState, nil)

		mockConnection.On("getFilesInMigration", &expectedMigration).Return(expectedFilesInMigration, nil)

		mockConnection.On("rollbackFile", fileState, "DROP TABLE test_table\n\n", "0", &expectedMigration).Return(nil)

		mockConnection.On("removeMigration", &expectedMigration).Return(nil)

		assert.NoError(t, s.rollback(mockConnection), "should rollback successfully")
	})
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
