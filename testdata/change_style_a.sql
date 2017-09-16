-- change
CREATE TABLE awesome_table (
    col_a text,
    col_b text
);

-- rollback
DROP TABLE awesome_table;

-- change
ALTER TABLE awesome_table ADD COLUMN col_c text;

-- rollback
ALTER TABLE awesome_table DROP COLUMN col_c;
