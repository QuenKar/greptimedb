-- https://github.com/duckdb/duckdb/blob/main/test/sql/types/date/test_incorrect_dates.test
CREATE TABLE dates(i DATE, ts TIMESTAMP TIME INDEX);

INSERT INTO dates VALUES ('blabla', 1000);

-- month out of range
INSERT INTO dates VALUES ('1993-20-14', 1000);

-- day out of range
INSERT INTO dates VALUES ('1993-08-99', 1000);

-- day out of range because not a leapyear
INSERT INTO dates VALUES ('1993-02-29', 1000);

-- day out of range because not a leapyear
INSERT INTO dates VALUES ('1900-02-29', 1000);

-- day in range because of leapyear
INSERT INTO dates VALUES ('1992-02-29', 2000);

-- day in range because of leapyear
INSERT INTO dates VALUES ('2000-02-29', 3000);

-- test incorrect date formats
-- dd-mm-YYYY
INSERT INTO dates VALUES ('02-02-1992', 1000);

-- different separators are not supported
INSERT INTO dates VALUES ('1900a01a01', 1000);

-- this should work though
INSERT INTO dates VALUES ('1900-1-1', 4000);

-- out of range dates
INSERT INTO dates VALUES ('-100000000-01-01', 1000);

INSERT INTO dates VALUES ('1000000000-01-01', 1000);
