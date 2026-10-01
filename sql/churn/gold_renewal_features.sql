-- gold.churn_renewal_features: one row per subscription renewal, features as of T-7.
--
-- Executed by src/jobs/churn/03_publish_gold_features.py (Spark SQL, Iceberg) with
-- $silver = lakehouse.silver. scripts/build_churn_gold_local.py is the pandas
-- equivalent for the no-Docker path; both must produce the same columns.
--
-- Point-in-time rule: every feature reads events dated on or before as_of
-- (the snapshot taken 7 days before current_period_end). Bronze keeps usage and
-- cap events after as_of on purpose; nothing below may read them.
-- The outcome is read from billing events around the renewal date instead.

WITH renewals AS (
  SELECT subscription_id, user_name, plan_tier, city, started_at,
         snapshot_date AS as_of, current_period_end AS renewal_date
  FROM $silver.churn_subscription_snapshots
  WHERE snapshot_date = date_sub(current_period_end, 7)
),
u28 AS (
  SELECT r.subscription_id,
         COUNT(DISTINCT u.activity_date)                                  AS active_days_28d,
         SUM(CASE WHEN dayofweek(u.activity_date) IN (1, 7) THEN 1 ELSE 0 END) AS weekend_days,
         SUM(u.ide_sessions) AS ide_sessions_28d, SUM(u.cli_sessions) AS cli_sessions_28d,
         SUM(u.agent_requests) AS agent_requests, SUM(u.cheap_model_requests) AS cheap_requests,
         SUM(u.suggestions_shown) AS shown, SUM(u.suggestions_accepted) AS accepted,
         SUM(u.agent_tasks) AS tasks, SUM(u.agent_tasks_kept) AS tasks_kept,
         SUM(u.total_requests) AS total_requests, SUM(u.failed_requests) AS failed_requests,
         MAX(CASE WHEN EXISTS(i.windows, w -> u.activity_date BETWEEN w.starts_on AND w.ends_on)
                  THEN 1 ELSE 0 END)                                      AS incident_exposed_28d
  FROM renewals r
  JOIN $silver.churn_usage_daily u
    ON u.subscription_id = r.subscription_id
   AND u.activity_date > date_sub(r.as_of, 28) AND u.activity_date <= r.as_of
  CROSS JOIN (SELECT collect_list(struct(starts_on, ends_on)) AS windows
              FROM $silver.churn_incidents) i
  GROUP BY r.subscription_id
),
u7 AS (
  SELECT r.subscription_id, COUNT(DISTINCT u.activity_date) AS active_days_7d
  FROM renewals r JOIN $silver.churn_usage_daily u
    ON u.subscription_id = r.subscription_id
   AND u.activity_date > date_sub(r.as_of, 7) AND u.activity_date <= r.as_of
  GROUP BY r.subscription_id
),
prev AS (  -- the 28 days before the 28-day window, for accept-rate change
  SELECT r.subscription_id, SUM(u.suggestions_shown) AS prev_shown,
         SUM(u.suggestions_accepted) AS prev_accepted
  FROM renewals r JOIN $silver.churn_usage_daily u
    ON u.subscription_id = r.subscription_id
   AND u.activity_date > date_sub(r.as_of, 56) AND u.activity_date <= date_sub(r.as_of, 28)
  GROUP BY r.subscription_id
),
last_seen AS (
  SELECT r.subscription_id, MAX(u.activity_date) AS last_active
  FROM renewals r JOIN $silver.churn_usage_daily u
    ON u.subscription_id = r.subscription_id AND u.activity_date <= r.as_of
  GROUP BY r.subscription_id
),
hits AS (
  SELECT r.subscription_id, COUNT(*) AS limit_hits_14d
  FROM renewals r JOIN $silver.churn_limit_events l
    ON l.subscription_id = r.subscription_id
   AND l.hit_date > date_sub(r.as_of, 14) AND l.hit_date <= r.as_of
  GROUP BY r.subscription_id
),
overage AS (
  SELECT r.subscription_id, SUM(c.amount_usd) AS overage_usd_28d
  FROM renewals r JOIN $silver.churn_overage_charges c
    ON c.subscription_id = r.subscription_id
   AND c.charged_at > date_sub(r.as_of, 28) AND c.charged_at <= r.as_of
  GROUP BY r.subscription_id
),
overage_state AS (
  SELECT r.subscription_id,
         CASE WHEN max_by(o.overage, o.changed_at) = 'disabled'
               AND SUM(CASE WHEN o.overage = 'enabled' THEN 1 ELSE 0 END) > 0
              THEN 1 ELSE 0 END AS overage_toggled_off
  FROM renewals r JOIN $silver.churn_overage_settings o
    ON o.subscription_id = r.subscription_id AND o.changed_at <= r.as_of
  GROUP BY r.subscription_id
),
tickets AS (
  SELECT r.subscription_id, COUNT(*) AS support_tickets_90d
  FROM renewals r JOIN $silver.churn_support_tickets t
    ON t.subscription_id = r.subscription_id
   AND t.created_date > date_sub(r.as_of, 90) AND t.created_date <= r.as_of
  GROUP BY r.subscription_id
),
paid_before AS (
  SELECT r.subscription_id, COUNT(*) AS renewals_completed
  FROM renewals r JOIN $silver.churn_invoices v
    ON v.subscription_id = r.subscription_id
   AND v.status = 'paid' AND v.invoice_date < r.renewal_date
  GROUP BY r.subscription_id
),
pricing AS (
  SELECT r.subscription_id,
         SUM(CASE WHEN p.effective_date <= r.as_of THEN 1 ELSE 0 END) AS cuts_so_far,
         MAX(CASE WHEN p.effective_date >= date_sub(r.renewal_date, 30)
                   AND p.effective_date < r.renewal_date
                   AND r.started_at < p.effective_date THEN 1 ELSE 0 END) AS first_renewal_after_pricing_change
  FROM renewals r CROSS JOIN $silver.churn_pricing_changes p
  GROUP BY r.subscription_id
),
billing AS (  -- outcome inputs, read after the renewal
  SELECT s.subscription_id,
         (SELECT MAX(event_date) FROM $silver.churn_subscription_events e
           WHERE e.subscription_id = s.subscription_id AND e.event_type = 'cancel_scheduled') AS scheduled_at,
         (SELECT MAX(event_date) FROM $silver.churn_subscription_events e
           WHERE e.subscription_id = s.subscription_id AND e.event_type = 'canceled') AS canceled_at,
         (SELECT MAX(invoice_date) FROM $silver.churn_invoices v
           WHERE v.subscription_id = s.subscription_id AND v.status = 'paid') AS paid_at,
         (SELECT MIN(invoice_date) FROM $silver.churn_invoices v
           WHERE v.subscription_id = s.subscription_id AND v.status = 'failed') AS failed_at
  FROM renewals s
),
today AS (SELECT MAX(snapshot_date) AS d FROM $silver.churn_subscription_snapshots),
joined AS (
  SELECT r.*, b.scheduled_at, b.canceled_at, b.paid_at, b.failed_at,
         COALESCE(u28.active_days_28d, 0) AS active_days_28d,
         COALESCE(u7.active_days_7d, 0)   AS active_days_7d,
         COALESCE(u28.weekend_days, 0)    AS weekend_days,
         COALESCE(u28.ide_sessions_28d, 0) AS ide_sessions_28d,
         COALESCE(u28.cli_sessions_28d, 0) AS cli_sessions_28d,
         COALESCE(u28.agent_requests, 0)  AS agent_requests,
         COALESCE(u28.cheap_requests, 0)  AS cheap_requests,
         COALESCE(u28.shown, 0) AS shown, COALESCE(u28.accepted, 0) AS accepted,
         COALESCE(u28.tasks, 0) AS tasks, COALESCE(u28.tasks_kept, 0) AS tasks_kept,
         COALESCE(u28.total_requests, 0) AS total_requests,
         COALESCE(u28.failed_requests, 0) AS failed_requests,
         COALESCE(u28.incident_exposed_28d, 0) AS incident_exposed_28d,
         COALESCE(prev.prev_shown, 0) AS prev_shown, COALESCE(prev.prev_accepted, 0) AS prev_accepted,
         last_seen.last_active,
         COALESCE(hits.limit_hits_14d, 0) AS limit_hits_14d,
         COALESCE(overage.overage_usd_28d, 0.0) AS overage_usd_28d,
         COALESCE(overage_state.overage_toggled_off, 0) AS overage_toggled_off,
         COALESCE(tickets.support_tickets_90d, 0) AS support_tickets_90d,
         COALESCE(paid_before.renewals_completed, 0) AS renewals_completed,
         COALESCE(pricing.cuts_so_far, 0) AS cuts_so_far,
         COALESCE(pricing.first_renewal_after_pricing_change, 0) AS first_renewal_after_pricing_change,
         today.d AS today
  FROM renewals r
  LEFT JOIN billing b       ON b.subscription_id = r.subscription_id
  LEFT JOIN u28             ON u28.subscription_id = r.subscription_id
  LEFT JOIN u7              ON u7.subscription_id = r.subscription_id
  LEFT JOIN prev            ON prev.subscription_id = r.subscription_id
  LEFT JOIN last_seen       ON last_seen.subscription_id = r.subscription_id
  LEFT JOIN hits            ON hits.subscription_id = r.subscription_id
  LEFT JOIN overage         ON overage.subscription_id = r.subscription_id
  LEFT JOIN overage_state   ON overage_state.subscription_id = r.subscription_id
  LEFT JOIN tickets         ON tickets.subscription_id = r.subscription_id
  LEFT JOIN paid_before     ON paid_before.subscription_id = r.subscription_id
  LEFT JOIN pricing         ON pricing.subscription_id = r.subscription_id
  CROSS JOIN today
),
labelled AS (
  SELECT *,
    CASE
      WHEN paid_at = renewal_date THEN 'renewed'
      WHEN canceled_at = renewal_date AND scheduled_at <= renewal_date THEN 'voluntary_lapse'
      WHEN failed_at = renewal_date AND canceled_at IS NOT NULL THEN 'involuntary_lapse'
      ELSE 'pending' END AS outcome
  FROM joined
)
SELECT
  subscription_id AS user_id,
  user_name,
  plan_tier,
  CAST(renewals_completed AS INT) AS renewals_completed,
  CAST(active_days_7d AS INT)  AS active_days_7d,
  CAST(active_days_28d AS INT) AS active_days_28d,
  bround(least(greatest(active_days_7d / greatest(1.0, active_days_28d / 4.0), 0.0), 4.0), 4) AS engagement_trend,
  CAST(least(greatest(COALESCE(datediff(as_of, last_active), 90), 0), 90) AS INT) AS last_active_days_ago,
  CAST(agent_requests AS INT) AS agent_requests_28d,
  bround(least(greatest(agent_requests / (
      CASE plan_tier WHEN 'pro' THEN 550 WHEN 'pro_plus' THEN 1650 WHEN 'ultra' THEN 11000 END
      * pow(0.83, cuts_so_far)), 0.0), 3.0), 4) AS allowance_used_pct,
  CAST(limit_hits_14d AS INT) AS limit_hits_14d,
  bround(CASE WHEN agent_requests > 0 THEN cheap_requests / agent_requests ELSE 0.0 END, 4) AS cheap_model_share_28d,
  bround(overage_usd_28d, 2) AS overage_usd_28d,
  CAST(overage_toggled_off AS INT) AS overage_toggled_off,
  bround(CASE WHEN shown > 0 THEN accepted / shown ELSE 0.0 END, 4) AS suggestion_accept_rate_28d,
  bround(least(greatest(CASE WHEN shown > 0 AND prev_shown > 0 AND prev_accepted > 0
      THEN bround(accepted / shown, 4) / (prev_accepted / prev_shown) ELSE 1.0 END, 0.0), 3.0), 4) AS accept_rate_change,
  bround(CASE WHEN tasks > 0 THEN tasks_kept / tasks ELSE 0.0 END, 4) AS agent_task_success_rate,
  bround(CASE WHEN total_requests > 0 THEN failed_requests / total_requests ELSE 0.0 END, 4) AS failed_requests_rate,
  CAST(incident_exposed_28d AS INT) AS incident_exposed_28d,
  CAST(support_tickets_90d AS INT) AS support_tickets_90d,
  CAST(ide_sessions_28d AS INT) AS ide_sessions_28d,
  CAST(cli_sessions_28d AS INT) AS cli_sessions_28d,
  bround(CASE WHEN active_days_28d > 0 THEN weekend_days / active_days_28d ELSE 0.0 END, 4) AS weekend_usage_ratio,
  CAST(first_renewal_after_pricing_change AS INT) AS first_renewal_after_pricing_change,
  CASE WHEN outcome = 'voluntary_lapse' THEN 1 ELSE 0 END AS churned,
  outcome,
  CASE
    WHEN outcome = 'renewed' THEN 'model'
    WHEN outcome = 'voluntary_lapse' AND scheduled_at <= as_of THEN 'cancel_flow'
    WHEN outcome = 'voluntary_lapse' THEN 'model'
    WHEN outcome = 'involuntary_lapse' THEN 'dunning'
    WHEN as_of = today THEN 'score_today'
    ELSE 'pending' END AS route,
  as_of AS feature_as_of,
  renewal_date,
  city
FROM labelled
