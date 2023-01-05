{{ 
   config(
        materialized='incremental',
        partition_by = { 'field': 'submission_date', 'data_type': 'date' },
        incremental_strategy = 'insert_overwrite',
        require_partition_filter = true,
        cluster_by = ["normalized_channel", "sample_id"],
    )
}}

WITH _previous AS (
  SELECT
    * EXCEPT (submission_date)
  FROM
    {{ this }}
  WHERE
    submission_date = DATE_SUB('{{ var("submission_date") }}', INTERVAL 1 DAY)
    AND udf.shift_28_bits_one_day(days_sent_metrics_ping_bits) > 0
),
_current AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.focus_android.metrics_clients_daily` AS m
  WHERE
    submission_date = '{{ var("submission_date") }}'
)
SELECT
  DATE('{{ var("submission_date") }}') AS submission_date,
  _current.client_id,
  _current.sample_id,
  _current.normalized_channel,
  _current.n_metrics_ping,
  udf.combine_adjacent_days_28_bits(
    _previous.days_sent_metrics_ping_bits,
    _current.days_sent_metrics_ping_bits
  ) AS days_sent_metrics_ping_bits,
  COALESCE(_current.uri_count, _previous.uri_count) AS uri_count,
  COALESCE(_current.is_default_browser, _previous.is_default_browser) AS is_default_browser,
FROM
  _previous
FULL JOIN
  _current
ON
  _previous.client_id = _current.client_id
  AND _previous.sample_id = _current.sample_id
  AND (
    _previous.normalized_channel = _current.normalized_channel
    OR (_previous.normalized_channel IS NULL AND _current.normalized_channel IS NULL)
  )
WHERE
  _current.client_id IS NOT NULL
