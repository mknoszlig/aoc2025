-- Problem and input available here: https://adventofcode.com/2025/day/2

-- Load and parse inputs, generate pids from ranges
CREATE OR REPLACE TABLE data_in AS (
  WITH raw AS (
       SELECT * FROM read_csv(
  	 '2_long.txt',
	 header=False
       )
  ),
  flipped AS (
  	  UNPIVOT raw ON *
  ),
  parsed AS (
  	 SELECT
	 	str_split(value, '-')[1]::long AS i_start,
		str_split(value, '-')[2]::long AS i_end
  	 FROM flipped
  )
  SELECT unnest(generate_series(i_start, i_end)) AS pid
  FROM parsed
);
	 

-- Part 1

CREATE OR REPLACE MACRO is_repetitive(s, l) AS (
       l <= len(s) / 2 -- make sure our substring pattern fits at least twice
       AND s = repeat(substring(s, 1, l), 2)
);

CREATE OR REPLACE TABLE matches AS (
SELECT
	pid,
	is_repetitive(pid::string, 1) AS r_1,
	is_repetitive(pid::string, 2) AS r_2,
	is_repetitive(pid::string, 3) AS r_3,
	is_repetitive(pid::string, 4) AS r_4,
	is_repetitive(pid::string, 5) AS r_5
 FROM data_in
WHERE
r_1 OR r_2 OR r_3 OR r_4 OR r_5
);

SELECT SUM(pid) AS result FROM matches;


-- part 2

CREATE OR REPLACE MACRO is_more_repetitive(s, l) AS (
       len(s) % l = 0 -- if string length isn't divisible by pattern length, don't bother
       AND l <= len(s) / 2 -- make sure our pattern length fits at least twice
       -- regexp is really slow (took about 12 secs to create `matches_p2`)
       -- AND regexp_full_match(s, CONCAT('(', substring(s, 1, l), '){2,}'))
       -- comparing against constructed repeat is much faster (sub second)
       AND (s = repeat(substring(s, 1, l), len(s) // l))
);

CREATE OR REPLACE TABLE matches_p2 AS (
SELECT
	pid,
	is_more_repetitive(pid::string, 1) AS r_1,
	is_more_repetitive(pid::string, 2) AS r_2,
	is_more_repetitive(pid::string, 3) AS r_3,
	is_more_repetitive(pid::string, 4) AS r_4,
	is_more_repetitive(pid::string, 5) AS r_5
FROM data_in
WHERE
r_1 OR r_2 OR r_3 OR r_4 OR r_5
);

SELECT SUM(pid) AS result FROM matches_p2;


-- Bonus: don't make assumptions about the pattern lengths
-- that can occur but deduce them from the data

CREATE OR REPLACE TABLE pattern_lengths AS (
  WITH max_len AS (
    SELECT max(len(pid::string)) as l FROM data_in
  ),
  series AS (
    -- we already know we're not interested in patterns that are longer
    -- than half of the max string length
    SELECT generate_series(1, l // 2) AS i FROM max_len
  )
  SELECT UNNEST(i) AS i FROM series
);

CREATE OR REPLACE TABLE dynamic_matches_p2 AS (
  WITH matched AS (
    SELECT
      pid,
      CASE -- we reuse the predicate to check for repetitions
      WHEN (is_more_repetitive(pid::string, i))
      THEN i
      ELSE NULL
      END AS matches
    FROM
      data_in,
      pattern_lengths
    WHERE
      matches IS NOT NULL -- drop results that don't match
  )
  SELECT
    pid,
    -- keep a list of matching pattern lenghts, not necessary
    -- but possibly useful for debugging,
    list(matches) AS matches
  FROM matched
  GROUP BY pid
);

SELECT sum(pid) AS result FROM dynamic_matches_p2;
