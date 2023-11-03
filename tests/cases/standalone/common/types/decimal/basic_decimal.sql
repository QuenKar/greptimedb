create table decimals(
    d decimal(10,6),
    ts timestamp time index,
);

insert into decimals values(0.12345678, '2019-01-01 00:00:00');

select * from decimals;

drop table decimals;