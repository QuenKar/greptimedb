-- port from DuckDB test/sql/types/decimal/test_decimal.test
-- description: Test basic decimals


-- default decimal type is (38,10)

SELECT arrow_typeof('0.1'::DECIMAL);

SELECT '0.1'::DECIMAL::VARCHAR, '922337203685478.758'::DECIMAL::VARCHAR;

SELECT '-0.1'::DECIMAL::VARCHAR, '-922337203685478.758'::DECIMAL::VARCHAR;


-- some more difficult string conversions
SELECT '   7   '::DECIMAL::VARCHAR, '9.'::DECIMAL::VARCHAR, '.1'::DECIMAL::VARCHAR;

-- trailing decimals get truncated
SELECT '0.123456789'::DECIMAL::VARCHAR, '-0.123456789'::DECIMAL::VARCHAR;


SELECT '9223372036854788.758'::DECIMAL;

-- trailing decimals with scale=0
SELECT '0.1'::DECIMAL(3, 0)::VARCHAR;

-- default scale is 0
SELECT '123.4'::DECIMAL(9)::VARCHAR;

-- scale = width also works
SELECT '0.1'::DECIMAL(3, 3)::VARCHAR, '-0.1'::DECIMAL(3, 3)::VARCHAR;

-- any value >= 1 becomes out of range, though
SELECT '1'::DECIMAL(3, 3)::VARCHAR;

SELECT '-1'::DECIMAL(3, 3)::VARCHAR;

-- repeat the same cast many times
select '0.1'::decimal::decimal::decimal;

select '12345.6789'::decimal(3,2);

select '12345.6789'::decimal(10,6)::decimal(3,2);


-- string casts of various decimal sizes
select '123.4'::DECIMAL(4,1)::VARCHAR;

select '2.001'::DECIMAL(4,3)::VARCHAR;

select '123456.789'::DECIMAL(9,3)::VARCHAR;

select '123456789'::DECIMAL(9,0)::VARCHAR;

select '123456789'::DECIMAL(18,3)::VARCHAR;

select '1701411834604692317316873037.1588410572'::DECIMAL(38,10)::VARCHAR;

select '0'::DECIMAL(38,10)::VARCHAR;

select '0.00003'::DECIMAL(38,10)::VARCHAR;


-- various error conditions

-- scale is bigger than or equal to width
SELECT '0.1'::DECIMAL(3, 4);

-- cannot have string variable as scale
SELECT '0.1'::DECIMAL('hello');

-- ...or negative numbers
SELECT '0.1'::DECIMAL(-17);

-- width/scale out of range
SELECT '0.1'::DECIMAL(1000);

-- invalid arguments
SELECT '0.1'::DECIMAL(1, 2, 3);
