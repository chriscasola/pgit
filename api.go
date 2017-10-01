package pgit

// Pgit is an instance of Pgit that is bound to a specific schema
// directory where the database schema is located and a particular
// database connection.
type Pgit struct {
	db     DatabaseConnection
	schema *schemaDirectory
}

// New initializes and returns a new Pgit instance
func New(rootPath string, db DatabaseConnection) (*Pgit, error) {
	schema, err := newSchemaDirectory(rootPath)
	if err != nil {
		return nil, err
	}
	return &Pgit{db: db, schema: schema}, nil
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
