CREATE TABLE datetimes(
    host String,
    event_date DateTime,
    ts Timestamp time index
);

INSERT INTO datetimes VALUES
('host1', '2019-01-01 00:00:00', 1000),
('host2', '2023-01-01 00:00:00', 2000),
('host3', '2023-01-02 00:00:00', 3000),
('host4', '2023-01-03 00:00:00', 4000);

SELECT * FROM datetimes;

SELECT * FROM datetimes WHERE event_date > '2020-01-01 00:00:00'::DateTime;

DROP TABLE datetimes;