-- https://github.com/duckdb/duckdb/blob/main/test/sql/types/date/test_date.test

CREATE TABLE dates(i DATE ,ts TIMESTAMP TIME INDEX);

INSERT INTO dates VALUES ('1993-08-14', 1000), (NULL, 2000);

SELECT * FROM dates;

SELECT year(i) FROM dates;

SELECT cast(i AS VARCHAR) FROM dates;
 
SELECT i + 5 FROM dates;

SELECT i - 5 FROM dates;

SELECT i * 3 FROM dates;

SELECT i / 3 FROM dates;

SELECT i % 3 FROM dates;

SELECT i + i FROM dates;

SELECT (i + 5) - i FROM dates;

SELECT ''::DATE;

SELECT '  '::DATE;

SELECT '1992'::DATE;

SELECT '1992-'::DATE;

SELECT '1992-01'::DATE;

SELECT '1992-01-'::DATE;

SELECT '30000307-01-01 (BC)'::DATE;
