package pgit

// Pgit is an instance of Pgit that is bound to a specific rootPath
// where the database schema is located and a particulare database
// connection.
type Pgit struct {
	rootPath string
	db       DatabaseConnection
	schema   *schemaDirectory
}

// New initializes and returns a new Pgit instance
func New(rootPath string, db DatabaseConnection) (*Pgit, error) {
	schema := newSchemaDirectory(rootPath)
	return &Pgit{rootPath: rootPath, db: db, schema: schema}, nil
}

// ApplyLatest ensures the latest version of the schema has been applied
// to the database, and if not applies it.
func (p *Pgit) ApplyLatest() error {
	return p.schema.applyLatest(p.db)
}

// Rollback rolls back the last migration that was applied
func (p *Pgit) Rollback() error {
	return p.schema.rollback(p.db)
}
