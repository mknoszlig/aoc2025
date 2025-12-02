-- Problem and input available at https://adventofcode.com/2025/day/1

CREATE OR REPLACE TABLE data_in AS (
  SELECT * FROM read_csv('1_long.txt', header=False, names=['value'])
);

CREATE OR REPLACE TABLE clicks AS (
WITH components AS (
     SELECT
	CASE WHEN substring(value, 1, 1) = 'L'
	THEN -1 ELSE 1 END AS direction,
	substring(value, 2)::int AS amount from data_in
)
SELECT direction * amount AS clicks FROM components
);


-- Part 1

WITH c_lists AS (
     SELECT list(clicks)
     OVER(ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW) AS cl FROM clicks
), positions AS (
SELECT list_reduce(
	cl,
       	lambda acc, x : (acc + x + 100) % 100,
	50
       ) AS end_pos
       FROM c_lists
)

SELECT count(*) AS password FROM positions WHERE end_pos = 0;


-- Part 2

CREATE OR REPLACE TABLE step_calculations AS (
WITH c_lists AS (
     SELECT list(clicks)
     OVER(ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW) AS cl FROM clicks
)
SELECT list_reduce(
	cl,
       	lambda acc, x : (acc + (x % 100) + 100) % 100,
	50
       ) AS end_pos,
       cl[-1] AS last_move,
       last_move % 100 AS net_move,
       -- (end_pos - net_move + 100) % 100 AS start_pos,
       abs(last_move) // 100 AS revolutions,
       CASE WHEN (end_pos - net_move) < 0 OR (end_pos - net_move > 100) OR end_pos = 0
       THEN 1 ELSE 0 END AS zero_pass
       FROM c_lists
);

SELECT SUM(zero_pass + revolutions) AS password FROM step_calculations;
