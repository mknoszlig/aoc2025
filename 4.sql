-- Problem and input available here: https://adventofcode.com/2025/day/4

CREATE OR REPLACE TABLE data_in AS (
  WITH raw AS (
       SELECT * FROM read_csv(
  	 '4_long.txt',
	 header=False
       )
  ), split AS (
    SELECT
	string_split(column0, '') AS split,
	row_number() OVER () AS y,
	generate_series(1, len(column0)) AS xs
	FROM raw
  )
  SELECT
	UNNEST(split) AS content,
	UNNEST(xs) AS x,
	y
  FROM split
);



CREATE OR REPLACE TABLE neighbours AS (
    SELECT
	x,
	y
    FROM
	(SELECT * FROM (VALUES (-1),(0),(1)) xs(x)),
	(SELECT * FROM (VALUES (-1),(0),(1)) ys(y))
     WHERE NOT (x = 0 AND y = 0)
);


-- Part 1

CREATE OR REPLACE TABLE solutions AS (
WITH bounds AS (
     SELECT
	max(x) AS max_x,
	max(y) AS max_y
     FROM data_in
),
affecting AS (
     SELECT *
     FROM data_in
     WHERE content = '@'
),
affected AS (
    SELECT
	a.x + n.x AS ax,
	a.y + n.y AS ay,
    FROM
	affecting a,
	neighbours n,
	bounds
    WHERE
	ax > 0 AND ax <= max_x AND ay > 0 and ay <= max_y
)
SELECT
	x,
	y,
	count(*) AS affected_by
FROM
	affecting
LEFT JOIN affected
ON ax = x AND ay = y
GROUP BY
     x, y
HAVING
     affected_by < 4
ORDER BY
      x, y
);

SELECT count(*) AS result FROM solutions;


-- Visualization of identified spots for debugging
CREATE OR REPLACE TABLE viz AS (
SELECT
	list(CASE WHEN solutions.x THEN 'x' ELSE data_in.content END ORDER BY data_in.x) AS content
FROM
data_in
LEFT JOIN
solutions
ON
data_in.x = solutions.x
AND data_in.y = solutions.y
GROUP BY data_in.y
ORDER BY data_in.y
);

COPY viz TO '4_debug.csv';


-- Part 2

-- Setup

-- We're setting up some table that we will update with the removed paper rolls
CREATE OR REPLACE TABLE iterable AS (SELECT * FROM data_in);

-- This is more or less solutions from above, but based on `iterable`
-- The complete part below can be repeatedly called until nothing more
-- can be removed, ie a 0 is added to the table.
CREATE OR REPLACE TABLE removable AS (
WITH bounds AS (
     SELECT
	max(x) AS max_x,
	max(y) AS max_y
     FROM iterable
),
affecting AS (
     SELECT *
     FROM iterable
     WHERE content = '@'
),
affected AS (
    SELECT
	a.x + n.x AS ax,
	a.y + n.y AS ay,
    FROM
	affecting a,
	neighbours n,
	bounds
    WHERE
	ax > 0 AND ax <= max_x AND ay > 0 and ay <= max_y
)
SELECT
	x,
	y,
	count(*) AS affected_by
FROM
	affecting
LEFT JOIN affected
ON ax = x AND ay = y
GROUP BY
     x, y
HAVING
     affected_by < 4
ORDER BY
      x, y
);

-- check if we need to continue
select count(*) FROM removable;

-- update the iterable table to account for the removed paper rolls
MERGE INTO iterable
      USING (
      	    SELECT x, y FROM removable
      )
      USING (x,y)
      WHEN MATCHED THEN update SET content = '.';


-- After all removable paper rolls are gone, just count how many
-- were removed

SELECT
	count(*) AS result,
FROM
	data_in
LEFT JOIN
	iterable
ON
data_in.x = iterable.x AND data_in.y = iterable.y
WHERE
data_in.content = '@' and iterable.content = '.'
;
