# pgit [![CircleCI](https://circleci.com/gh/chriscasola/pgit.svg?style=svg)](https://circleci.com/gh/chriscasola/pgit)

Pronounce "pee-git" - a database migration tool that lets you keep your schema and migrations in a git repository.

## Roadmap
- [x] Implement support for the `changeset` file type
- [x] Implement support for the `definition` file type
- [x] Build out a CLI

## Installing

go get github.com/chriscasola/pgit

## Usage 

With pgit you should place all of the `.sql` files describing your database and migrations into a single directory
inside your project (there can be nested directories).

To perform a migration run `pgit -database <database-connection-string> -root <path-to-sql-directory> migrate`

### File Types

Each file will have `-- pgit type=<some_type>` on the first line where `<some_type>` is replace with one of the
supported file types:

#### changeset

This type of file is most useful for statements that create tables, modify tables, or similar operations that you want to record as a sequence of steps in your file.

```SQL
-- pgit type=changeset

-- change
CREATE TABLE some_table
(
    col_a text
);

-- rollback
DROP TABLE some_table

-- change
ALTER TABLE some_table ADD COLUMN col_b text;

-- rollback
ALTER TABLE some_table DROP COLUMN col_b;
```

#### definition

This type of file is most useful for stored procedures or functions. For this type of file pgit will use the git history to track revisions. You need only keep the most recent version of the definition in the file, along with SQL to rollback that version.

```SQL
-- pgit type=definition

-- definition
CREATE FUNCTION do_something(
    param_a text
)
BEGIN
END;

-- rollback
DROP FUNCTION do_something(text);
```

## Developers

Run tests with `go test`
