{{ 
   config(
        materialized='incremental',
        partition_by = { 'field': 'submission_date', 'data_type': 'date' },
        incremental_strategy = 'insert_overwrite',
        require_partition_filter = true,
    )
}}

SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  sample_id,
  "release" AS normalized_channel,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
  SUM(metrics.counter.browser_total_uri_count) AS uri_count,
  LOGICAL_OR(metrics.counter.app_opened_as_default_browser > 0) AS is_default_browser,
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_focus.metrics` AS m
WHERE
  DATE(submission_timestamp) = '{{ var("submission_date") }}'
GROUP BY
  submission_date,
  client_id,
  sample_id,
  normalized_channel
