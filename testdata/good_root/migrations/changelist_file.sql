-- pgit type=changeset

-- change
CREATE TABLE test_table (
    col_a text
);

-- rollback
DROP TABLE test_table
