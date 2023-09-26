CREATE TABLE datetimes(
    host String,
    event_date DateTime,
    ts Timestamp time index
);

INSERT INTO datetimes VALUES
('host1', '2019-01-01 00:00:00', 1000),
('host2', '2023-01-01 00:00:00', 2000);

SELECT * FROM datetimes;

DROP TABLE datetimes;