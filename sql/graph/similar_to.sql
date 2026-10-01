-- SIMILAR_TO (spec similar_to/renewal-v1) in Spark SQL: the lakehouse-native twin of
-- lakehouse_graph.build.similar_to_full(). GENERATED from src/lakehouse_graph/spec.py
-- (FEATURES, K = 10, QUANT = 1000000000, REFERENCE_ROUTE = 'model') by
--   python scripts/check_graph_parity.py sql --write
-- Do not edit by hand: tests/graph/test_parity_sql.py fails when this file and the spec disagree.
--
-- Inputs (temporary views the caller registers; no placeholders in this file):
--   g_renewal            Renewal node rows (renewal_id, plan_tier, route and the 20 features)
--   g_similar_to_scaler  the PERSISTED scaler rows (feature, mean, std, n_ref, spec_version). The
--                        Spark job fits it with the first section, writes it to
--                        lakehouse.gold.graph_similar_to_scaler and reads it back; the parity
--                        check's hard gate binds the numpy build's similar_to_scaler.parquet.
-- Rules that make the result bit-identical to the numpy builder given the same input and scaler:
--   z_f  = CASE WHEN std_f > 0 THEN (CAST(x_f AS DOUBLE) - mean_f) / std_f ELSE 0D END
--   d2   = t_1 + t_2 + ... + t_20 with t_f = (a.z_f - b.z_f) * (a.z_f - b.z_f), in feature order
--          (Spark keeps the left-to-right order of a sum of non-constant doubles)
--   d2_q = CAST(FLOOR(d2 * 1000000000D + 0.5D) AS BIGINT), the one rounding mode of every engine
--   rank = ROW_NUMBER() OVER (PARTITION BY src ORDER BY d2_q, dst), rank <= 10; self excluded;
--          candidates: route = 'model' in the same plan_tier. rid is a dense INT assigned in
--          renewal_id order, so ordering by rid is ordering by renewal_id
--   dist = SQRT(d2); mutual = the reverse edge is also in the top 10
-- Performance: the sources are repartitioned by rid (a small table is one input partition) and
-- the candidates broadcast; the caller caches g_similar_to_z and g_similar_to_topk.

-- view: g_similar_to_scaler_fit
-- The scaler fitted in SQL: population mean and std (ddof = 0) over the reference set.
SELECT inline(array(
         named_struct('feature', 'renewals_completed', 'mean', m_renewals_completed, 'std', s_renewals_completed),
         named_struct('feature', 'active_days_7d', 'mean', m_active_days_7d, 'std', s_active_days_7d),
         named_struct('feature', 'active_days_28d', 'mean', m_active_days_28d, 'std', s_active_days_28d),
         named_struct('feature', 'engagement_trend', 'mean', m_engagement_trend, 'std', s_engagement_trend),
         named_struct('feature', 'last_active_days_ago', 'mean', m_last_active_days_ago, 'std', s_last_active_days_ago),
         named_struct('feature', 'allowance_used_pct', 'mean', m_allowance_used_pct, 'std', s_allowance_used_pct),
         named_struct('feature', 'limit_hits_14d', 'mean', m_limit_hits_14d, 'std', s_limit_hits_14d),
         named_struct('feature', 'cheap_model_share_28d', 'mean', m_cheap_model_share_28d, 'std', s_cheap_model_share_28d),
         named_struct('feature', 'overage_usd_28d', 'mean', m_overage_usd_28d, 'std', s_overage_usd_28d),
         named_struct('feature', 'overage_toggled_off', 'mean', m_overage_toggled_off, 'std', s_overage_toggled_off),
         named_struct('feature', 'suggestion_accept_rate_28d', 'mean', m_suggestion_accept_rate_28d, 'std', s_suggestion_accept_rate_28d),
         named_struct('feature', 'accept_rate_change', 'mean', m_accept_rate_change, 'std', s_accept_rate_change),
         named_struct('feature', 'agent_task_success_rate', 'mean', m_agent_task_success_rate, 'std', s_agent_task_success_rate),
         named_struct('feature', 'failed_requests_rate', 'mean', m_failed_requests_rate, 'std', s_failed_requests_rate),
         named_struct('feature', 'incident_exposed_28d', 'mean', m_incident_exposed_28d, 'std', s_incident_exposed_28d),
         named_struct('feature', 'support_tickets_90d', 'mean', m_support_tickets_90d, 'std', s_support_tickets_90d),
         named_struct('feature', 'ide_sessions_28d', 'mean', m_ide_sessions_28d, 'std', s_ide_sessions_28d),
         named_struct('feature', 'cli_sessions_28d', 'mean', m_cli_sessions_28d, 'std', s_cli_sessions_28d),
         named_struct('feature', 'weekend_usage_ratio', 'mean', m_weekend_usage_ratio, 'std', s_weekend_usage_ratio),
         named_struct('feature', 'first_renewal_after_pricing_change', 'mean', m_first_renewal_after_pricing_change, 'std', s_first_renewal_after_pricing_change))),
       n_ref, 'similar_to/renewal-v1' AS spec_version
FROM (
  SELECT AVG(CAST(renewals_completed AS DOUBLE)) AS m_renewals_completed, STDDEV_POP(CAST(renewals_completed AS DOUBLE)) AS s_renewals_completed,
         AVG(CAST(active_days_7d AS DOUBLE)) AS m_active_days_7d, STDDEV_POP(CAST(active_days_7d AS DOUBLE)) AS s_active_days_7d,
         AVG(CAST(active_days_28d AS DOUBLE)) AS m_active_days_28d, STDDEV_POP(CAST(active_days_28d AS DOUBLE)) AS s_active_days_28d,
         AVG(CAST(engagement_trend AS DOUBLE)) AS m_engagement_trend, STDDEV_POP(CAST(engagement_trend AS DOUBLE)) AS s_engagement_trend,
         AVG(CAST(last_active_days_ago AS DOUBLE)) AS m_last_active_days_ago, STDDEV_POP(CAST(last_active_days_ago AS DOUBLE)) AS s_last_active_days_ago,
         AVG(CAST(allowance_used_pct AS DOUBLE)) AS m_allowance_used_pct, STDDEV_POP(CAST(allowance_used_pct AS DOUBLE)) AS s_allowance_used_pct,
         AVG(CAST(limit_hits_14d AS DOUBLE)) AS m_limit_hits_14d, STDDEV_POP(CAST(limit_hits_14d AS DOUBLE)) AS s_limit_hits_14d,
         AVG(CAST(cheap_model_share_28d AS DOUBLE)) AS m_cheap_model_share_28d, STDDEV_POP(CAST(cheap_model_share_28d AS DOUBLE)) AS s_cheap_model_share_28d,
         AVG(CAST(overage_usd_28d AS DOUBLE)) AS m_overage_usd_28d, STDDEV_POP(CAST(overage_usd_28d AS DOUBLE)) AS s_overage_usd_28d,
         AVG(CAST(overage_toggled_off AS DOUBLE)) AS m_overage_toggled_off, STDDEV_POP(CAST(overage_toggled_off AS DOUBLE)) AS s_overage_toggled_off,
         AVG(CAST(suggestion_accept_rate_28d AS DOUBLE)) AS m_suggestion_accept_rate_28d, STDDEV_POP(CAST(suggestion_accept_rate_28d AS DOUBLE)) AS s_suggestion_accept_rate_28d,
         AVG(CAST(accept_rate_change AS DOUBLE)) AS m_accept_rate_change, STDDEV_POP(CAST(accept_rate_change AS DOUBLE)) AS s_accept_rate_change,
         AVG(CAST(agent_task_success_rate AS DOUBLE)) AS m_agent_task_success_rate, STDDEV_POP(CAST(agent_task_success_rate AS DOUBLE)) AS s_agent_task_success_rate,
         AVG(CAST(failed_requests_rate AS DOUBLE)) AS m_failed_requests_rate, STDDEV_POP(CAST(failed_requests_rate AS DOUBLE)) AS s_failed_requests_rate,
         AVG(CAST(incident_exposed_28d AS DOUBLE)) AS m_incident_exposed_28d, STDDEV_POP(CAST(incident_exposed_28d AS DOUBLE)) AS s_incident_exposed_28d,
         AVG(CAST(support_tickets_90d AS DOUBLE)) AS m_support_tickets_90d, STDDEV_POP(CAST(support_tickets_90d AS DOUBLE)) AS s_support_tickets_90d,
         AVG(CAST(ide_sessions_28d AS DOUBLE)) AS m_ide_sessions_28d, STDDEV_POP(CAST(ide_sessions_28d AS DOUBLE)) AS s_ide_sessions_28d,
         AVG(CAST(cli_sessions_28d AS DOUBLE)) AS m_cli_sessions_28d, STDDEV_POP(CAST(cli_sessions_28d AS DOUBLE)) AS s_cli_sessions_28d,
         AVG(CAST(weekend_usage_ratio AS DOUBLE)) AS m_weekend_usage_ratio, STDDEV_POP(CAST(weekend_usage_ratio AS DOUBLE)) AS s_weekend_usage_ratio,
         AVG(CAST(first_renewal_after_pricing_change AS DOUBLE)) AS m_first_renewal_after_pricing_change, STDDEV_POP(CAST(first_renewal_after_pricing_change AS DOUBLE)) AS s_first_renewal_after_pricing_change,
         COUNT(*) AS n_ref
  FROM g_renewal
  WHERE route = 'model'
) a;

-- view: g_similar_to_z
SELECT r.renewal_id, r.plan_tier, (r.route = 'model') AS is_ref,
       CAST(ROW_NUMBER() OVER (ORDER BY r.renewal_id) AS INT) AS rid,
       CASE WHEN s.s_renewals_completed > 0 THEN (CAST(r.renewals_completed AS DOUBLE) - s.m_renewals_completed) / s.s_renewals_completed ELSE 0D END AS z_renewals_completed,
         CASE WHEN s.s_active_days_7d > 0 THEN (CAST(r.active_days_7d AS DOUBLE) - s.m_active_days_7d) / s.s_active_days_7d ELSE 0D END AS z_active_days_7d,
         CASE WHEN s.s_active_days_28d > 0 THEN (CAST(r.active_days_28d AS DOUBLE) - s.m_active_days_28d) / s.s_active_days_28d ELSE 0D END AS z_active_days_28d,
         CASE WHEN s.s_engagement_trend > 0 THEN (CAST(r.engagement_trend AS DOUBLE) - s.m_engagement_trend) / s.s_engagement_trend ELSE 0D END AS z_engagement_trend,
         CASE WHEN s.s_last_active_days_ago > 0 THEN (CAST(r.last_active_days_ago AS DOUBLE) - s.m_last_active_days_ago) / s.s_last_active_days_ago ELSE 0D END AS z_last_active_days_ago,
         CASE WHEN s.s_allowance_used_pct > 0 THEN (CAST(r.allowance_used_pct AS DOUBLE) - s.m_allowance_used_pct) / s.s_allowance_used_pct ELSE 0D END AS z_allowance_used_pct,
         CASE WHEN s.s_limit_hits_14d > 0 THEN (CAST(r.limit_hits_14d AS DOUBLE) - s.m_limit_hits_14d) / s.s_limit_hits_14d ELSE 0D END AS z_limit_hits_14d,
         CASE WHEN s.s_cheap_model_share_28d > 0 THEN (CAST(r.cheap_model_share_28d AS DOUBLE) - s.m_cheap_model_share_28d) / s.s_cheap_model_share_28d ELSE 0D END AS z_cheap_model_share_28d,
         CASE WHEN s.s_overage_usd_28d > 0 THEN (CAST(r.overage_usd_28d AS DOUBLE) - s.m_overage_usd_28d) / s.s_overage_usd_28d ELSE 0D END AS z_overage_usd_28d,
         CASE WHEN s.s_overage_toggled_off > 0 THEN (CAST(r.overage_toggled_off AS DOUBLE) - s.m_overage_toggled_off) / s.s_overage_toggled_off ELSE 0D END AS z_overage_toggled_off,
         CASE WHEN s.s_suggestion_accept_rate_28d > 0 THEN (CAST(r.suggestion_accept_rate_28d AS DOUBLE) - s.m_suggestion_accept_rate_28d) / s.s_suggestion_accept_rate_28d ELSE 0D END AS z_suggestion_accept_rate_28d,
         CASE WHEN s.s_accept_rate_change > 0 THEN (CAST(r.accept_rate_change AS DOUBLE) - s.m_accept_rate_change) / s.s_accept_rate_change ELSE 0D END AS z_accept_rate_change,
         CASE WHEN s.s_agent_task_success_rate > 0 THEN (CAST(r.agent_task_success_rate AS DOUBLE) - s.m_agent_task_success_rate) / s.s_agent_task_success_rate ELSE 0D END AS z_agent_task_success_rate,
         CASE WHEN s.s_failed_requests_rate > 0 THEN (CAST(r.failed_requests_rate AS DOUBLE) - s.m_failed_requests_rate) / s.s_failed_requests_rate ELSE 0D END AS z_failed_requests_rate,
         CASE WHEN s.s_incident_exposed_28d > 0 THEN (CAST(r.incident_exposed_28d AS DOUBLE) - s.m_incident_exposed_28d) / s.s_incident_exposed_28d ELSE 0D END AS z_incident_exposed_28d,
         CASE WHEN s.s_support_tickets_90d > 0 THEN (CAST(r.support_tickets_90d AS DOUBLE) - s.m_support_tickets_90d) / s.s_support_tickets_90d ELSE 0D END AS z_support_tickets_90d,
         CASE WHEN s.s_ide_sessions_28d > 0 THEN (CAST(r.ide_sessions_28d AS DOUBLE) - s.m_ide_sessions_28d) / s.s_ide_sessions_28d ELSE 0D END AS z_ide_sessions_28d,
         CASE WHEN s.s_cli_sessions_28d > 0 THEN (CAST(r.cli_sessions_28d AS DOUBLE) - s.m_cli_sessions_28d) / s.s_cli_sessions_28d ELSE 0D END AS z_cli_sessions_28d,
         CASE WHEN s.s_weekend_usage_ratio > 0 THEN (CAST(r.weekend_usage_ratio AS DOUBLE) - s.m_weekend_usage_ratio) / s.s_weekend_usage_ratio ELSE 0D END AS z_weekend_usage_ratio,
         CASE WHEN s.s_first_renewal_after_pricing_change > 0 THEN (CAST(r.first_renewal_after_pricing_change AS DOUBLE) - s.m_first_renewal_after_pricing_change) / s.s_first_renewal_after_pricing_change ELSE 0D END AS z_first_renewal_after_pricing_change
FROM g_renewal r
CROSS JOIN (
  SELECT MAX(CASE WHEN feature = 'renewals_completed' THEN mean END) AS m_renewals_completed, MAX(CASE WHEN feature = 'renewals_completed' THEN std END) AS s_renewals_completed,
         MAX(CASE WHEN feature = 'active_days_7d' THEN mean END) AS m_active_days_7d, MAX(CASE WHEN feature = 'active_days_7d' THEN std END) AS s_active_days_7d,
         MAX(CASE WHEN feature = 'active_days_28d' THEN mean END) AS m_active_days_28d, MAX(CASE WHEN feature = 'active_days_28d' THEN std END) AS s_active_days_28d,
         MAX(CASE WHEN feature = 'engagement_trend' THEN mean END) AS m_engagement_trend, MAX(CASE WHEN feature = 'engagement_trend' THEN std END) AS s_engagement_trend,
         MAX(CASE WHEN feature = 'last_active_days_ago' THEN mean END) AS m_last_active_days_ago, MAX(CASE WHEN feature = 'last_active_days_ago' THEN std END) AS s_last_active_days_ago,
         MAX(CASE WHEN feature = 'allowance_used_pct' THEN mean END) AS m_allowance_used_pct, MAX(CASE WHEN feature = 'allowance_used_pct' THEN std END) AS s_allowance_used_pct,
         MAX(CASE WHEN feature = 'limit_hits_14d' THEN mean END) AS m_limit_hits_14d, MAX(CASE WHEN feature = 'limit_hits_14d' THEN std END) AS s_limit_hits_14d,
         MAX(CASE WHEN feature = 'cheap_model_share_28d' THEN mean END) AS m_cheap_model_share_28d, MAX(CASE WHEN feature = 'cheap_model_share_28d' THEN std END) AS s_cheap_model_share_28d,
         MAX(CASE WHEN feature = 'overage_usd_28d' THEN mean END) AS m_overage_usd_28d, MAX(CASE WHEN feature = 'overage_usd_28d' THEN std END) AS s_overage_usd_28d,
         MAX(CASE WHEN feature = 'overage_toggled_off' THEN mean END) AS m_overage_toggled_off, MAX(CASE WHEN feature = 'overage_toggled_off' THEN std END) AS s_overage_toggled_off,
         MAX(CASE WHEN feature = 'suggestion_accept_rate_28d' THEN mean END) AS m_suggestion_accept_rate_28d, MAX(CASE WHEN feature = 'suggestion_accept_rate_28d' THEN std END) AS s_suggestion_accept_rate_28d,
         MAX(CASE WHEN feature = 'accept_rate_change' THEN mean END) AS m_accept_rate_change, MAX(CASE WHEN feature = 'accept_rate_change' THEN std END) AS s_accept_rate_change,
         MAX(CASE WHEN feature = 'agent_task_success_rate' THEN mean END) AS m_agent_task_success_rate, MAX(CASE WHEN feature = 'agent_task_success_rate' THEN std END) AS s_agent_task_success_rate,
         MAX(CASE WHEN feature = 'failed_requests_rate' THEN mean END) AS m_failed_requests_rate, MAX(CASE WHEN feature = 'failed_requests_rate' THEN std END) AS s_failed_requests_rate,
         MAX(CASE WHEN feature = 'incident_exposed_28d' THEN mean END) AS m_incident_exposed_28d, MAX(CASE WHEN feature = 'incident_exposed_28d' THEN std END) AS s_incident_exposed_28d,
         MAX(CASE WHEN feature = 'support_tickets_90d' THEN mean END) AS m_support_tickets_90d, MAX(CASE WHEN feature = 'support_tickets_90d' THEN std END) AS s_support_tickets_90d,
         MAX(CASE WHEN feature = 'ide_sessions_28d' THEN mean END) AS m_ide_sessions_28d, MAX(CASE WHEN feature = 'ide_sessions_28d' THEN std END) AS s_ide_sessions_28d,
         MAX(CASE WHEN feature = 'cli_sessions_28d' THEN mean END) AS m_cli_sessions_28d, MAX(CASE WHEN feature = 'cli_sessions_28d' THEN std END) AS s_cli_sessions_28d,
         MAX(CASE WHEN feature = 'weekend_usage_ratio' THEN mean END) AS m_weekend_usage_ratio, MAX(CASE WHEN feature = 'weekend_usage_ratio' THEN std END) AS s_weekend_usage_ratio,
         MAX(CASE WHEN feature = 'first_renewal_after_pricing_change' THEN mean END) AS m_first_renewal_after_pricing_change, MAX(CASE WHEN feature = 'first_renewal_after_pricing_change' THEN std END) AS s_first_renewal_after_pricing_change
  FROM g_similar_to_scaler
) s;

-- view: g_similar_to_topk
SELECT a.renewal_id AS src, b.renewal_id AS dst, t.rank, t.d2, t.d2_q
FROM (
  SELECT src_rid, dst_rid, CAST(rank AS BIGINT) AS rank, d2, d2_q
  FROM (
    SELECT src_rid, dst_rid, d2, d2_q,
           ROW_NUMBER() OVER (PARTITION BY src_rid ORDER BY d2_q, dst_rid) AS rank
    FROM (
      SELECT src_rid, dst_rid, d2, CAST(FLOOR(d2 * 1000000000D + 0.5D) AS BIGINT) AS d2_q
      FROM (
        SELECT /*+ BROADCAST(b) */ a.rid AS src_rid, b.rid AS dst_rid,
               (a.z_renewals_completed - b.z_renewals_completed) * (a.z_renewals_completed - b.z_renewals_completed)
              + (a.z_active_days_7d - b.z_active_days_7d) * (a.z_active_days_7d - b.z_active_days_7d)
              + (a.z_active_days_28d - b.z_active_days_28d) * (a.z_active_days_28d - b.z_active_days_28d)
              + (a.z_engagement_trend - b.z_engagement_trend) * (a.z_engagement_trend - b.z_engagement_trend)
              + (a.z_last_active_days_ago - b.z_last_active_days_ago) * (a.z_last_active_days_ago - b.z_last_active_days_ago)
              + (a.z_allowance_used_pct - b.z_allowance_used_pct) * (a.z_allowance_used_pct - b.z_allowance_used_pct)
              + (a.z_limit_hits_14d - b.z_limit_hits_14d) * (a.z_limit_hits_14d - b.z_limit_hits_14d)
              + (a.z_cheap_model_share_28d - b.z_cheap_model_share_28d) * (a.z_cheap_model_share_28d - b.z_cheap_model_share_28d)
              + (a.z_overage_usd_28d - b.z_overage_usd_28d) * (a.z_overage_usd_28d - b.z_overage_usd_28d)
              + (a.z_overage_toggled_off - b.z_overage_toggled_off) * (a.z_overage_toggled_off - b.z_overage_toggled_off)
              + (a.z_suggestion_accept_rate_28d - b.z_suggestion_accept_rate_28d) * (a.z_suggestion_accept_rate_28d - b.z_suggestion_accept_rate_28d)
              + (a.z_accept_rate_change - b.z_accept_rate_change) * (a.z_accept_rate_change - b.z_accept_rate_change)
              + (a.z_agent_task_success_rate - b.z_agent_task_success_rate) * (a.z_agent_task_success_rate - b.z_agent_task_success_rate)
              + (a.z_failed_requests_rate - b.z_failed_requests_rate) * (a.z_failed_requests_rate - b.z_failed_requests_rate)
              + (a.z_incident_exposed_28d - b.z_incident_exposed_28d) * (a.z_incident_exposed_28d - b.z_incident_exposed_28d)
              + (a.z_support_tickets_90d - b.z_support_tickets_90d) * (a.z_support_tickets_90d - b.z_support_tickets_90d)
              + (a.z_ide_sessions_28d - b.z_ide_sessions_28d) * (a.z_ide_sessions_28d - b.z_ide_sessions_28d)
              + (a.z_cli_sessions_28d - b.z_cli_sessions_28d) * (a.z_cli_sessions_28d - b.z_cli_sessions_28d)
              + (a.z_weekend_usage_ratio - b.z_weekend_usage_ratio) * (a.z_weekend_usage_ratio - b.z_weekend_usage_ratio)
              + (a.z_first_renewal_after_pricing_change - b.z_first_renewal_after_pricing_change) * (a.z_first_renewal_after_pricing_change - b.z_first_renewal_after_pricing_change) AS d2
        FROM (SELECT /*+ REPARTITION(8, rid) */ * FROM g_similar_to_z) a
        JOIN (SELECT * FROM g_similar_to_z WHERE is_ref) b
          ON a.plan_tier = b.plan_tier AND a.rid <> b.rid
      ) pairs
    ) keyed
  ) ranked
  WHERE rank <= 10
) t
JOIN g_similar_to_z a ON a.rid = t.src_rid
JOIN g_similar_to_z b ON b.rid = t.dst_rid;

-- table: SIMILAR_TO
SELECT t.src, t.dst, t.rank, t.d2, t.d2_q, SQRT(t.d2) AS dist, (r.src IS NOT NULL) AS mutual,
       'similar_to/renewal-v1' AS spec_version
FROM g_similar_to_topk t
LEFT JOIN g_similar_to_topk r ON r.src = t.dst AND r.dst = t.src;
