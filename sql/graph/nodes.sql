-- Renewal graph (renewal-graph/v1), node tables in Spark SQL: the lakehouse-native twin of
-- src/lakehouse_graph/build.py:build_tables() (the pandas builder is the reference; the parity
-- check scripts/check_graph_parity.py compares the two table by table).
--
-- Executed section by section by src/jobs/graph/01_publish_gold_graph.py (mounted at /opt/sql):
--   $silver = the silver churn tables, $gold = the gold schema holding churn_renewal_features.
-- The job binds both to snapshot-pinned views (every input is read at ONE snapshot id, recorded
-- and tagged graph_<build_id>); the parity check binds them to the pandas twin's frames.
--
-- Sections: "-- view: <name>" registers a temporary view used by later sections (and by
-- edges.sql / similar_to.sql); "-- table: <Label>" is the node table of that label, with the
-- columns of lakehouse_graph.spec.NODE_SCHEMA[<Label>] in spec order, key first. Every table
-- section is also registered as the view g_<label>.
--
-- Ids of event nodes are <prefix>:<subscription_id>:NNN in event order (build._seq_ids): the
-- ORDER BY keys are the pandas sort keys, NULLS LAST like pandas, plus tie-breakers that only
-- order rows the pandas key leaves equal (identical rows, or the one frame order pandas keeps).
-- Rounding follows numpy: round(x, 2) = rint(x * 100) / 100 (half to even on the binary value).

-- view: g_limit_events
SELECT subscription_id,
       format_string('lh:%s:%03d', subscription_id, seq) AS event_id,
       hit_date AS event_date,
       limit_type
FROM (
  SELECT subscription_id, hit_date, limit_type,
         ROW_NUMBER() OVER (PARTITION BY subscription_id
                            ORDER BY hit_at ASC NULLS LAST, limit_type ASC NULLS LAST) AS seq
  FROM $silver.churn_limit_events
) l;

-- view: g_overage_settings
SELECT subscription_id,
       format_string('ovs:%s:%03d', subscription_id, seq) AS event_id,
       changed_at AS event_date,
       overage AS state
FROM (
  SELECT subscription_id, changed_at, overage,
         ROW_NUMBER() OVER (PARTITION BY subscription_id
                            ORDER BY changed_at ASC NULLS LAST, overage ASC NULLS LAST) AS seq
  FROM $silver.churn_overage_settings
) o;

-- view: g_overage_charges
SELECT subscription_id,
       format_string('ovc:%s:%03d', subscription_id, seq) AS event_id,
       charged_at AS event_date,
       CAST(amount_usd AS DOUBLE) AS amount_usd
FROM (
  SELECT subscription_id, charged_at, amount_usd,
         ROW_NUMBER() OVER (PARTITION BY subscription_id
                            ORDER BY charged_at ASC NULLS LAST, amount_usd ASC NULLS LAST) AS seq
  FROM $silver.churn_overage_charges
) c;

-- view: g_billing
-- Billing: cancel events + renewal-cycle invoices (invoice_date >= renewal_date: paid at T and
-- failed dunning attempts). History invoices stay aggregated in Renewal.renewals_completed.
-- outcome_evidence: everything except a cancel_scheduled on/before as_of (that one is known at T-7).
WITH renewal_dates AS (
  SELECT user_id AS subscription_id, renewal_date, feature_as_of AS as_of
  FROM $gold.churn_renewal_features
),
bill AS (
  SELECT e.subscription_id, e.event_date, e.event_type,
         CAST(NULL AS DOUBLE) AS amount_usd, CAST(NULL AS BIGINT) AS attempt, 0 AS frame_order
  FROM $silver.churn_subscription_events e
  UNION ALL
  SELECT v.subscription_id, v.invoice_date AS event_date, concat('invoice_', v.status) AS event_type,
         CAST(v.amount_usd AS DOUBLE) AS amount_usd, CAST(v.attempt AS BIGINT) AS attempt, 1 AS frame_order
  FROM $silver.churn_invoices v
  JOIN renewal_dates d ON d.subscription_id = v.subscription_id
  WHERE v.invoice_date >= d.renewal_date
)
SELECT b.subscription_id,
       format_string('bill:%s:%03d', b.subscription_id, b.seq) AS event_id,
       b.event_date, b.event_type, b.amount_usd, b.attempt,
       NOT COALESCE(b.event_type = 'cancel_scheduled' AND b.event_date <= d.as_of, FALSE) AS outcome_evidence
FROM (
  SELECT bill.*,
         ROW_NUMBER() OVER (PARTITION BY subscription_id
                            ORDER BY event_date ASC NULLS LAST, event_type ASC NULLS LAST, frame_order,
                                     attempt ASC NULLS LAST, amount_usd ASC NULLS LAST) AS seq
  FROM bill
) b
LEFT JOIN renewal_dates d ON d.subscription_id = b.subscription_id;

-- table: Subscription
SELECT subscription_id, user_name, city, plan_tier, started_at
FROM $silver.churn_subscription_snapshots;

-- table: Renewal
-- One renewal per T-7 gold row. Features are the gold values (ints as BIGINT, the rest as DOUBLE:
-- CAST of Spark's decimal engagement_trend to DOUBLE is exact). outcome_observed_on: the renewal
-- date if renewed, the last canceled event if lapsed, else null. cuts_so_far: pricing changes
-- effective on/before as_of. allowance_at_as_of = round(plan allowance x 0.83^cuts_so_far, 2).
WITH canceled AS (
  SELECT subscription_id, MAX(event_date) AS canceled_on
  FROM $silver.churn_subscription_events
  WHERE event_type = 'canceled'
  GROUP BY subscription_id
),
cuts AS (
  SELECT g.user_id, CAST(COUNT(p.change_id) AS BIGINT) AS cuts_so_far
  FROM $gold.churn_renewal_features g
  LEFT JOIN $silver.churn_pricing_changes p ON p.effective_date <= g.feature_as_of
  GROUP BY g.user_id
)
SELECT concat(g.user_id, ':', date_format(g.renewal_date, 'yyyy-MM-dd')) AS renewal_id,
       g.user_id AS subscription_id,
       g.plan_tier,
       g.feature_as_of AS as_of,
       g.renewal_date,
       CAST(g.renewals_completed AS BIGINT) AS renewals_completed,
       CAST(g.active_days_7d AS BIGINT) AS active_days_7d,
       CAST(g.active_days_28d AS BIGINT) AS active_days_28d,
       CAST(g.engagement_trend AS DOUBLE) AS engagement_trend,
       CAST(g.last_active_days_ago AS BIGINT) AS last_active_days_ago,
       CAST(g.agent_requests_28d AS BIGINT) AS agent_requests_28d,
       CAST(g.allowance_used_pct AS DOUBLE) AS allowance_used_pct,
       CAST(g.limit_hits_14d AS BIGINT) AS limit_hits_14d,
       CAST(g.cheap_model_share_28d AS DOUBLE) AS cheap_model_share_28d,
       CAST(g.overage_usd_28d AS DOUBLE) AS overage_usd_28d,
       CAST(g.overage_toggled_off AS BIGINT) AS overage_toggled_off,
       CAST(g.suggestion_accept_rate_28d AS DOUBLE) AS suggestion_accept_rate_28d,
       CAST(g.accept_rate_change AS DOUBLE) AS accept_rate_change,
       CAST(g.agent_task_success_rate AS DOUBLE) AS agent_task_success_rate,
       CAST(g.failed_requests_rate AS DOUBLE) AS failed_requests_rate,
       CAST(g.incident_exposed_28d AS BIGINT) AS incident_exposed_28d,
       CAST(g.support_tickets_90d AS BIGINT) AS support_tickets_90d,
       CAST(g.ide_sessions_28d AS BIGINT) AS ide_sessions_28d,
       CAST(g.cli_sessions_28d AS BIGINT) AS cli_sessions_28d,
       CAST(g.weekend_usage_ratio AS DOUBLE) AS weekend_usage_ratio,
       CAST(g.first_renewal_after_pricing_change AS BIGINT) AS first_renewal_after_pricing_change,
       CAST(g.churned AS BIGINT) AS churned,
       g.outcome,
       g.route,
       (g.route = 'model') AS is_reference,
       CASE WHEN g.outcome = 'renewed' THEN g.renewal_date
            WHEN g.outcome IN ('voluntary_lapse', 'involuntary_lapse') THEN c.canceled_on
       END AS outcome_observed_on,
       k.cuts_so_far,
       rint(CASE g.plan_tier WHEN 'pro' THEN 550D WHEN 'pro_plus' THEN 1650D WHEN 'ultra' THEN 11000D END
            * pow(0.83D, CAST(k.cuts_so_far AS DOUBLE)) * 100D) / 100D AS allowance_at_as_of
FROM $gold.churn_renewal_features g
LEFT JOIN canceled c ON c.subscription_id = g.user_id
LEFT JOIN cuts k ON k.user_id = g.user_id;

-- table: Plan
-- Curated constants: list price (generator PRICE = spec.PLAN_PRICE_USD) and the 28-day agent
-- request allowance of the gold rule (550 / 1,650 / 11,000).
SELECT plan_tier, price_usd, base_allowance_28d
FROM VALUES ('pro', 20.0D, 550L), ('pro_plus', 60.0D, 1650L), ('ultra', 200.0D, 11000L)
  AS plans(plan_tier, price_usd, base_allowance_28d);

-- table: Incident
SELECT incident_id, starts_on, ends_on, CAST(datediff(ends_on, starts_on) + 1 AS BIGINT) AS days
FROM $silver.churn_incidents;

-- table: PricingChange
-- cap_multiplier: every cut multiplies the allowance by 0.83 (the gold rule's pow(0.83, cuts)).
SELECT change_id, effective_date, description, 0.83D AS cap_multiplier
FROM $silver.churn_pricing_changes;

-- table: LimitHit
SELECT event_id, event_date, limit_type FROM g_limit_events;

-- table: OverageChange
SELECT event_id, event_date, state FROM g_overage_settings;

-- table: OverageCharge
SELECT event_id, event_date, amount_usd FROM g_overage_charges;

-- table: Ticket
SELECT ticket_id, created_date AS event_date FROM $silver.churn_support_tickets;

-- table: BillingEvent
SELECT event_id, event_date, event_type, amount_usd, attempt FROM g_billing;
