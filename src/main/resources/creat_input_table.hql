CREATE TABLE IF NOT EXISTS output_table (
some_column STRING COMMENT 'string column',
some_other_column STRING COMMENT 'some other string column'
)
COMMENT 'This is test table'
LOCATION '/output_table'
STORED AS TEXTFILE;
