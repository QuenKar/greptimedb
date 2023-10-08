select try_cast('' as date);

select try_cast('    ' as date);

select try_cast('1111' as date);

select try_cast('1111-' as date);

select try_cast('1111-11' as date);

select try_cast('1111-111-1' as date);

select try_cast('1111-11-11' as date);

select try_cast('1111-11-11 (bc)' as date);

select try_cast('2001-02-29' as date);

select try_cast('2004-02-29' as date);

select try_cast('2004/02/29' as date);

select try_cast('2004/02-29' as date);

select try_cast('-infinity' as date);

select try_cast('5881580-07-10' as date);

select try_cast('5881580-07-11' as date);

select try_cast('infinity' as date);

select try_cast('5881580-08-11' as date);

select try_cast('99999999-01-01' as date);

select try_cast('294246-12-31'::date as timestamp);

select try_cast('294247-12-31'::date as timestamp);
