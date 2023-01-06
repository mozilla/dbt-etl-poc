{% test array_equals(model, actual, expected) %}

SELECT *
FROM (
    SELECT "error"
)
WHERE (
    EXISTS(
      (
        SELECT
          *
        FROM
          UNNEST({{ expected }})
          WITH OFFSET
        EXCEPT DISTINCT
        SELECT
          *
        FROM
          UNNEST({{ actual }})
          WITH OFFSET
      )
      UNION ALL
        (
          SELECT
            *
          FROM
            UNNEST({{ actual }})
            WITH OFFSET
          EXCEPT DISTINCT
          SELECT
            *
          FROM
            UNNEST({{ expected }})
            WITH OFFSET
        )
    )
  )
{% endtest %}
