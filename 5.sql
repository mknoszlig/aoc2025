-- Problem and input available here: https://adventofcode.com/2025/day/5

CREATE OR REPLACE TABLE data_in AS (
  WITH raw AS (
       SELECT * FROM read_csv(
  	 '5_long.txt',
	 header=False,
	 names=['val']
       )
  ),
  indexed AS (
       SELECT
	val,
	row_number() OVER () AS index
	FROM raw
  )
  SELECT * FROM indexed
);

CREATE OR REPLACE TABLE fresh_ranges AS (
WITH piv AS (
     SELECT index FROM data_in
     WHERE val IS NULL
),
arrs AS (
     SELECT
	string_split(val, '-') AS bounds
     FROM
     data_in,
     piv
     WHERE
     data_in.index < piv.index
)
SELECT
    bounds[1]::long AS lower,
    bounds[2]::long AS upper
FROM
    arrs
);

CREATE OR REPLACE TABLE products AS (
WITH piv AS (
     SELECT index FROM data_in
     WHERE val IS NULL
)
SELECT
	val::long AS id
FROM
	data_in,
	piv
WHERE data_in.index > piv.index
);

-- Part 1

SELECT
	COUNT(DISTINCT(products.id)) AS result,
FROM
	products
INNER JOIN fresh_ranges
ON products.id
BETWEEN fresh_ranges.lower AND fresh_ranges.upper;


-- Part 2

-- again, create a copy of the input data to iterate on.
CREATE OR REPLACE TABLE unified AS(
       SELECT * FROM fresh_ranges
);

-- we can simply iterate this statement, each iteration
-- will merge adjacent intervals and it will converge
-- to the final set of non overlapping ranges.
CREATE OR REPLACE TABLE unified AS (
   WITH ordered AS ( -- ensure the rows are ordered
      SELECT * FROM unified ORDER BY lower, upper
   ),
   aligned AS ( -- check if adjacent rows overlap
      SELECT
         min(lower)  over w AS min_low,
	 max(lower)  over w AS max_low,
	 min(upper)  over w as min_high,
	 max(upper)  over w as max_high,
	 max_low <= min_high AS overlap
      FROM ordered
      WINDOW w AS (
         ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
      )),
   merged AS ( -- merge ranges if they overlap, keep the higher one if not
      SELECT
         CASE WHEN overlap THEN min_low ELSE max_low END as lower,
       	 max_high AS upper
       	 FROM aligned
   )
   SELECT -- finally, consolidate all rows starting on the same position
	lower, max(upper) AS upper
	FROM merged
	GROUP BY lower
);

SELECT COUNT(*) FROM unified;

SELECT SUM(upper - lower + 1) AS result FROM unified;
