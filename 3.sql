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
	CONCAT(i1.bank, i2.bank) AS banks,
	i1.l AS l,
	i2.r AS r
      FROM
        indexed AS i1,
	indexed AS i2
      WHERE
	i2.r > i1.l
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


CREATE OR REPLACE TEMP TABLE joltages1 AS (
       with sub1 as (select * from data_in limit 100)
       select max_joltage(batteries) from sub1
 );

CREATE OR REPLACE TEMP TABLE joltages2 AS (
      with sub2 as (select * from data_in limit 100 offset 100)
      select max_joltage(batteries) from sub2
);


CREATE OR REPLACE TABLE joltages AS (
(select "max_joltage(batteries)" as joltage FROM joltages1)
UNION ALL
(select "max_joltage(batteries)" as joltage fROM joltages2)
);

select sum(joltage) FROM joltages;
