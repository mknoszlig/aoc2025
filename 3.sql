-- Problem and input available here: https://adventofcode.com/2025/day/3

CREATE OR REPLACE TABLE data_in AS (
  WITH raw AS (
       SELECT * FROM read_csv(
  	 '3_long.txt',
	 header=False,
	 columns={'batteries': 'VARCHAR'}
       )
  )
  SELECT string_split(batteries::string, '') AS batteries FROM raw
);


-- Part 1

CREATE OR REPLACE MACRO joltage(batteries) AS (
       WITH indexed AS (
         SELECT
	   UNNEST(batteries) AS bank,
	   UNNEST(generate_series(1, len(batteries))) AS pos
       )
       SELECT max(indexed.bank::integer * 10 + i2.bank::integer) AS joltage FROM
       indexed, indexed AS i2
       WHERE i2.pos > indexed.pos
);

SELECT sum(joltage(batteries)) FROM data_in;

-- Part 2

CREATE OR REPLACE MACRO max_joltage(batteries) AS (
WITH
indexed AS (
         SELECT
	   UNNEST(batteries) AS bank,
	   UNNEST(generate_series(1, len(batteries))) AS l,
	   l AS r
	 ORDER BY bank DESC, l ASC
),
pairs AS (
      SELECT
	MAX(CONCAT(i1.bank, i2.bank)) AS banks,
	i1.l AS l,
	i2.r AS r
      FROM
        indexed AS i1,
	indexed AS i2
      WHERE
	i2.r > i1.l
      GROUP BY i1.l, i2.r
),
quads AS (
      SELECT
      max(CONCAT(i1.banks, i2.banks)) AS banks,
      i1.l AS l,
      i2.r AS r
      FROM
	pairs AS i1,
	pairs AS i2
      WHERE
	i2.l > i1.r
      GROUP BY i1.l, i2.r
),
sixes AS (
      SELECT
      max(CONCAT(i1.banks, i2.banks)) AS banks,
      i1.l AS l,
      i2.r AS r
      FROM
        quads AS i1,
	pairs AS i2
      WHERE
	i2.l > i1.r
      GROUP BY i1.l, i2.r
)
SELECT
   max(CONCAT(i1.banks, i2.banks)::long) AS banks
   FROM
   sixes i1,
   sixes i2
   WHERE
   i2.l > i1.r
);


--
-- simply applying the macro in one go on the whole table OOMs for me,
-- unless i set threads to something low, in wich case it's slower (3 min vs 1 min)
--
-- This is sort of a compromise which looks pretty ugly. This could of course be driven
-- by a python script does this in a loop.

CREATE OR REPLACE TABLE joltages
(joltage LONG);


 INSERT INTO joltages (
       WITH sub AS (SELECT * FROM data_in LIMIT 20)
       SELECT max_joltage(batteries) AS joltage FROM sub
 );

INSERT INTO joltages (
      WITH sub AS (SELECT * FROM data_in LIMIT 20 offset 20)
      SELECT max_joltage(batteries) AS joltage FROM sub
);

INSERT INTO joltages (
      WITH sub AS (SELECT * FROM data_in LIMIT 20 offset 40)
      SELECT max_joltage(batteries) AS joltage FROM sub
);

INSERT INTO joltages (
      WITH sub AS (SELECT * FROM data_in LIMIT 20 offset 60)
      SELECT max_joltage(batteries) AS joltage FROM sub
);

INSERT INTO joltages (
      WITH sub AS (SELECT * FROM data_in LIMIT 20 offset 80)
      SELECT max_joltage(batteries) AS joltage FROM sub
);

INSERT INTO joltages (
      WITH sub AS (SELECT * FROM data_in LIMIT 20 offset 100)
      SELECT max_joltage(batteries) AS joltage FROM sub
);

INSERT INTO joltages (
      WITH sub AS (SELECT * FROM data_in LIMIT 20 offset 120)
      SELECT max_joltage(batteries) AS joltage FROM sub
);

INSERT INTO joltages (
      WITH sub AS (SELECT * FROM data_in LIMIT 20 offset 140)
      SELECT max_joltage(batteries) AS joltage FROM sub
);


INSERT INTO joltages (
      WITH sub AS (SELECT * FROM data_in LIMIT 20 offset 160)
      SELECT max_joltage(batteries) AS joltage FROM sub
);

INSERT INTO joltages (
      WITH sub AS (SELECT * FROM data_in LIMIT 20 offset 180)
      SELECT max_joltage(batteries) AS joltage FROM sub
);

select sum(joltage) FROM joltages;

