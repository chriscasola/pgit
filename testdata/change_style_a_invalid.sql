-- change
ALTER TABLE awesome_table ADD COLUMN col_c text;

-- rollback
ALTER TABLE awesome_table DROP COLUMN col_c;

-- change
ALTER TABLE awesome_table ADD COLUMN col_d text;
