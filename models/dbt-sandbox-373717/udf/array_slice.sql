CREATE OR REPLACE FUNCTION udf.array_slice(arr ANY TYPE, start_index INT64, end_index INT64) AS (
  ARRAY(
    SELECT
      * EXCEPT (offset)
    FROM
      UNNEST(arr)
      WITH OFFSET
    WHERE
      offset
      BETWEEN start_index
      AND end_index
    ORDER BY
      offset
  )
);
