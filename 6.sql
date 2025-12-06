-- Problem and input available here: https://adventofcode.com/2025/day/5

-- Main Problem is reading the data in a way that makes the transposition easier
-- in Part 2. The last lines containing the operation are best suited to anchor this,
-- because they're always in pos 1 of each block.

CREATE OR REPLACE TABLE data_in AS (
  WITH raw AS (
       SELECT
	val,
	row_number() OVER () AS n
       FROM read_csv(
  	 '6_long.txt',
	 header=False,
	 names=['val']
       )
  ),
  cols AS (
    SELECT list_slice(regexp_split_to_array(val[2:-1], ' \*| \+'), 1, -1) cols
    FROM raw
    ORDER BY  n DESC
    LIMIT 1 -- only interested in the last row containing the ops for this.
  ),
  col_lengths AS (
    SELECT
      len(unnest(cols)) + 1 AS step,
    FROM cols
  ),
  indexes AS ( -- calculate the positions to extract from the source line
    SELECT
      step,
      row_number() OVER () AS c,
      COALESCE(sum(step) OVER(ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
	   + c AS i -- + c because there's one extra space separating the entries per col
    FROM col_lengths
  ),
  chopped AS (
    SELECT
      n,
      c,
      substring(val, i::long, step) AS digit
    FROM
      raw,
      indexes
  )
  SELECT list(digit::string ORDER BY n) AS digit
  FROM chopped
  GROUP BY c
);

-- Part 1

SELECT
  SUM(
    CASE WHEN trim(digit[-1]) = '+'
    THEN
      list_aggr(
	list_transform(list_slice(digit, 1, -2), lambda x: x::long), 'sum'
      )
    ELSE
      list_aggr(
	list_transform(list_slice(digit, 1, -2), lambda x: x::long), 'product')::long
    END
  ) AS result
FROM data_in;


-- Part 2


CREATE OR REPLACE MACRO arr_transpose(list_in) AS (
  WITH unnested AS (
     SELECT
     unnest(list_in) AS v,
     -- this was needed to preserve ordering, had a mean bug here
     generate_subscripts(list_in, 1) AS r
     ORDER BY r
  ),
  split AS (
    SELECT *,
    	   unnest(str_split(v, '')) AS d,
	   -- same as above
	   generate_subscripts(str_split(v, ''), 1) AS c,
    FROM unnested
  ),
  aggregated AS (
  SELECT c, list(d order by r) AS l FROM split
  GROUP BY c
  ORDER BY c
 )
 SELECT list(array_to_string(l, '')::long ORDER BY c) FROM aggregated
);

-- Logic is the same as in Part 1, except we transpose the array first.
SELECT
  SUM(
    CASE WHEN trim(digit[-1]) = '+'
    THEN
      list_aggr(
        arr_transpose(list_slice(digit, 1, -2)), 'sum'
      )
    ELSE
      list_aggr(
        arr_transpose(list_slice(digit, 1, -2)), 'product')::long
    END
  ) AS result
FROM data_in;
