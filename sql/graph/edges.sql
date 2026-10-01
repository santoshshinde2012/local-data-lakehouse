-- Renewal graph (renewal-graph/v1), edge tables in Spark SQL (everything except SIMILAR_TO,
-- which is similar_to.sql). Runs after nodes.sql in the same session: it reads the views that
-- file registers (g_renewal, g_plan, g_limit_events, g_overage_settings, g_overage_charges,
-- g_billing) and the same $silver / $gold bindings.
--
-- Each "-- table: <TYPE>" section returns src, dst and the properties of
-- lakehouse_graph.spec.EDGE_SCHEMA[<TYPE>] in spec order. Every event-derived edge carries its
-- event_date; the point-in-time filter is applied by the readers (tool templates, contract), the
-- graph itself keeps the leak surface on purpose (PLAN 6.4).

-- table: HAS_RENEWAL
SELECT subscription_id AS src, renewal_id AS dst, as_of FROM g_renewal;

-- table: ON_PLAN
SELECT renewal_id AS src, plan_tier AS dst, as_of FROM g_renewal;

-- table: HIT_LIMIT
SELECT subscription_id AS src, event_id AS dst, event_date FROM g_limit_events;

-- table: CHANGED_OVERAGE
SELECT subscription_id AS src, event_id AS dst, event_date, state FROM g_overage_settings;

-- table: CHARGED_OVERAGE
SELECT subscription_id AS src, event_id AS dst, event_date, amount_usd FROM g_overage_charges;

-- table: OPENED
SELECT subscription_id AS src, ticket_id AS dst, created_date AS event_date
FROM $silver.churn_support_tickets;

-- table: BILLED
SELECT subscription_id AS src, event_id AS dst, event_date, event_type, outcome_evidence FROM g_billing;

-- table: EXPOSED_TO
-- One edge per active usage day inside a declared incident window (both ends inclusive).
SELECT u.subscription_id AS src, i.incident_id AS dst, u.activity_date AS event_date
FROM $silver.churn_usage_daily u
JOIN $silver.churn_incidents i ON u.activity_date >= i.starts_on AND u.activity_date <= i.ends_on;

-- table: FIRST_RENEWAL_AFTER
-- The declared point-in-time exception: the gold rule (renewal_date - 30 <= effective_date <
-- renewal_date, subscription started before the change), flagged known_by_as_of.
SELECT r.renewal_id AS src, p.change_id AS dst, p.effective_date AS event_date,
       (p.effective_date <= r.as_of) AS known_by_as_of
FROM g_renewal r
JOIN $silver.churn_subscription_snapshots s ON s.subscription_id = r.subscription_id
JOIN $silver.churn_pricing_changes p
  ON p.effective_date >= date_sub(r.renewal_date, 30)
 AND p.effective_date < r.renewal_date
 AND s.started_at < p.effective_date;

-- table: CUT_CAP
SELECT p.change_id AS src, pl.plan_tier AS dst, p.effective_date AS event_date, 0.83D AS multiplier
FROM $silver.churn_pricing_changes p
CROSS JOIN g_plan pl;
