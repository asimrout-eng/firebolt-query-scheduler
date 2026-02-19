-- ============================================================================
-- INCREMENTAL UPDATE: dm_loan_details_at_monthly (Current Month Only)
-- ============================================================================
-- This script deletes current month data and re-inserts with fresh calculations
-- Optimized to avoid OOM by using staged approach with GROUP BY
-- Target: 47 columns in dm_loan_details_at_monthly
-- ============================================================================


-- Step 1: Delete current month data
DELETE FROM fair.public.dm_loan_details_at_monthly 
WHERE status_month = (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE;

-- Step 2: Create temp table for current month only
DROP TABLE IF EXISTS fair.public.tmp_current_month;
CREATE TABLE fair.public.tmp_current_month AS
SELECT (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS status_month;

-- Step 3: Get disbursement info per loan
DROP TABLE IF EXISTS fair.public.tmp_disburse_inc;
CREATE TABLE fair.public.tmp_disburse_inc AS
SELECT 
    cp.loan_id,
    MIN(TO_TIMESTAMP(cpc.udated)::DATE) AS first_disburse_date,
    MAX(TO_TIMESTAMP(cpc.udated)::DATE) AS last_disburse_date,
    SUM(cp.amount) AS disburse_amount
FROM fair.public.cent_proposal cp
INNER JOIN fair.public.cent_proposal_collection cpc 
    ON cp.id::BIGINT = cpc.proposal_id::BIGINT
WHERE cp.deleted = 'N'
    AND cp.proposal_state = '13000'
    AND cp.is_collected = 'Y'
    AND cpc.deleted = 'N'
    AND cpc.proposal_state = '14000'
    AND cpc.is_declined = '0'
GROUP BY cp.loan_id;

-- Step 4: Get loan-level EMI summary
DROP TABLE IF EXISTS fair.public.tmp_emi_summary_inc;
CREATE TABLE fair.public.tmp_emi_summary_inc AS
SELECT 
    loan_id,
    -- Portfolio type
    CASE
        WHEN SUM(CASE WHEN uid_l = 1983451 THEN 1 ELSE 0 END) > 0 THEN 'FD'
        WHEN SUM(CASE WHEN uid_l = 4297499 THEN 1 ELSE 0 END) > 0 THEN 'INDMoney'
        WHEN SUM(CASE WHEN uid_l = 5046222 THEN 1 ELSE 0 END) > 0 THEN 'MLP'
        ELSE 'Non-FD'
    END AS portfolio_type,
    -- Totals
    SUM(CASE WHEN emi_state != 'closed' THEN amount ELSE 0 END) AS loan_total_amount,
    SUM(CASE WHEN emi_state != 'closed' THEN principal ELSE 0 END) AS loan_total_principal,
    SUM(CASE WHEN emi_state != 'closed' THEN interest ELSE 0 END) AS loan_total_interest,
    -- EMI count
    COUNT(DISTINCT (DATE_TRUNC('month', TO_TIMESTAMP(due_date + 19800)) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) AS total_emi_count,
    -- Dates
    MIN(TO_TIMESTAMP(due_date + 19800)::DATE) AS first_emi_date,
    MAX(TO_TIMESTAMP(due_date + 19800)::DATE) AS last_emi_date,
    MIN(TO_TIMESTAMP(created + 19800)::DATE) AS min_emi_created
FROM fair.public.cent_emi
WHERE deleted = 'N'
GROUP BY loan_id;

-- Step 5: Combine loan info with current month (only loans with disburse <= current month)
DROP TABLE IF EXISTS fair.public.tmp_loan_current_month;
CREATE TABLE fair.public.tmp_loan_current_month AS
SELECT 
    es.loan_id,
    es.portfolio_type,
    es.loan_total_amount,
    es.loan_total_principal,
    es.loan_total_interest,
    es.total_emi_count,
    es.first_emi_date,
    es.last_emi_date,
    COALESCE(d.first_disburse_date, es.min_emi_created) AS disburse_date,
    d.first_disburse_date,
    d.last_disburse_date,
    cm.status_month
FROM fair.public.tmp_emi_summary_inc es
LEFT JOIN fair.public.tmp_disburse_inc d ON d.loan_id = es.loan_id
CROSS JOIN fair.public.tmp_current_month cm
WHERE COALESCE(d.first_disburse_date, es.min_emi_created) <= cm.status_month;

-- Step 6: Pre-aggregate EMI metrics for current month per loan using GROUP BY
DROP TABLE IF EXISTS fair.public.tmp_emi_agg_inc;
CREATE TABLE fair.public.tmp_emi_agg_inc AS
SELECT 
    e.loan_id,
    lm.status_month,
    -- Loan status (active count)
    SUM(CASE WHEN (e.emi_state = 'due') OR (e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE >= lm.status_month) THEN 1 ELSE 0 END) AS active_count,
    -- Paid EMI count
    COUNT(DISTINCT CASE WHEN e.emi_state IN ('paid', 'closed') AND TO_TIMESTAMP(e.udated + 19800)::DATE <= lm.status_month 
        THEN (DATE_TRUNC('month', TO_TIMESTAMP(e.due_date + 19800)) + INTERVAL '1 month' - INTERVAL '1 day')::DATE END) AS paid_emi_count,
    -- Paid amounts cumulative
    SUM(CASE WHEN e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE <= lm.status_month THEN e.amount ELSE 0 END) AS total_paid_amount,
    SUM(CASE WHEN e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE <= lm.status_month THEN e.principal ELSE 0 END) AS total_paid_principal,
    SUM(CASE WHEN e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE <= lm.status_month THEN e.interest ELSE 0 END) AS total_paid_interest,
    -- Paid this month
    SUM(CASE WHEN e.emi_state = 'paid' 
        AND TO_TIMESTAMP(e.udated + 19800)::DATE BETWEEN DATE_TRUNC('month', lm.status_month)::DATE AND lm.status_month
        THEN e.amount ELSE 0 END) AS total_paid_amount_this_month,
    SUM(CASE WHEN e.emi_state = 'paid' 
        AND TO_TIMESTAMP(e.udated + 19800)::DATE BETWEEN DATE_TRUNC('month', lm.status_month)::DATE AND lm.status_month
        THEN e.principal ELSE 0 END) AS total_paid_principal_this_month,
    SUM(CASE WHEN e.emi_state = 'paid' 
        AND TO_TIMESTAMP(e.udated + 19800)::DATE BETWEEN DATE_TRUNC('month', lm.status_month)::DATE AND lm.status_month
        THEN e.interest ELSE 0 END) AS total_paid_interest_this_month,
    -- Last paid date
    MAX(CASE WHEN e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE <= lm.status_month 
        THEN TO_TIMESTAMP(e.udated + 19800)::DATE END) AS last_paid_date,
    -- Due EMI date (next due)
    MIN(CASE WHEN (e.emi_state = 'due') OR (e.emi_state != 'due' AND TO_TIMESTAMP(e.udated + 19800)::DATE > lm.status_month) 
        THEN TO_TIMESTAMP(e.due_date + 19800)::DATE END) AS due_emi_date,
    -- Due amounts (outstanding)
    SUM(CASE WHEN (e.emi_state = 'due') OR (e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE > lm.status_month) 
        THEN e.amount ELSE 0 END) AS total_due_amount,
    SUM(CASE WHEN (e.emi_state = 'due') OR (e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE > lm.status_month) 
        THEN e.principal ELSE 0 END) AS total_due_principal,
    SUM(CASE WHEN (e.emi_state = 'due') OR (e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE > lm.status_month) 
        THEN e.interest ELSE 0 END) AS total_due_interest,
    -- Due to collect (overdue EMIs where emi_month <= status_month)
    SUM(CASE WHEN ((e.emi_state = 'due') OR (e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE > lm.status_month)) 
        AND (DATE_TRUNC('month', TO_TIMESTAMP(e.due_date + 19800)) + INTERVAL '1 month' - INTERVAL '1 day')::DATE <= lm.status_month 
        THEN e.amount ELSE 0 END) AS due_amount_to_collect,
    SUM(CASE WHEN ((e.emi_state = 'due') OR (e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE > lm.status_month)) 
        AND (DATE_TRUNC('month', TO_TIMESTAMP(e.due_date + 19800)) + INTERVAL '1 month' - INTERVAL '1 day')::DATE <= lm.status_month 
        THEN e.principal ELSE 0 END) AS due_principal_to_collect,
    SUM(CASE WHEN ((e.emi_state = 'due') OR (e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE > lm.status_month)) 
        AND (DATE_TRUNC('month', TO_TIMESTAMP(e.due_date + 19800)) + INTERVAL '1 month' - INTERVAL '1 day')::DATE <= lm.status_month 
        THEN e.interest ELSE 0 END) AS due_interest_to_collect,
    -- For DPD: earliest overdue EMI date
    MIN(CASE WHEN ((e.emi_state = 'due') OR (e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE > lm.status_month)) 
        AND (DATE_TRUNC('month', TO_TIMESTAMP(e.due_date + 19800)) + INTERVAL '1 month' - INTERVAL '1 day')::DATE <= lm.status_month 
        THEN TO_TIMESTAMP(e.due_date + 19800)::DATE END) AS min_overdue_emi_date,
    -- Bounce flags (for current emi_month only)
    SUM(CASE WHEN lm.status_month = (DATE_TRUNC('month', TO_TIMESTAMP(e.due_date + 19800)) + INTERVAL '1 month' - INTERVAL '1 day')::DATE
        AND TO_TIMESTAMP(e.due_date + 19800)::DATE <= CURRENT_DATE
        AND ((e.emi_state = 'paid' AND DATE_DIFF('day', TO_TIMESTAMP(e.due_date + 19800)::DATE, TO_TIMESTAMP(e.udated + 19800)::DATE) > 4) OR e.emi_state = 'due')
        THEN 1 ELSE 0 END) AS bounce_relax_count,
    SUM(CASE WHEN lm.status_month = (DATE_TRUNC('month', TO_TIMESTAMP(e.due_date + 19800)) + INTERVAL '1 month' - INTERVAL '1 day')::DATE
        AND TO_TIMESTAMP(e.due_date + 19800)::DATE <= CURRENT_DATE
        AND ((e.emi_state = 'paid' AND DATE_DIFF('day', TO_TIMESTAMP(e.due_date + 19800)::DATE, TO_TIMESTAMP(e.udated + 19800)::DATE) > 0) OR e.emi_state = 'due')
        THEN 1 ELSE 0 END) AS bounce_act_count
FROM fair.public.tmp_loan_current_month lm
INNER JOIN fair.public.cent_emi e ON e.loan_id = lm.loan_id
WHERE e.deleted = 'N'
GROUP BY e.loan_id, lm.status_month;

-- Step 7: Insert into final table (47 columns)
INSERT INTO fair.public.dm_loan_details_at_monthly (
    loan_id, portfolio_type, loan_total_amount, loan_total_principal, loan_total_interest,
    total_emi_count, first_emi_date, last_emi_date, disburse_date, first_disburse_date,
    last_disburse_date, status_month, loan_status, balance_emi_count, paid_emi_count,
    total_paid_amount, total_paid_principal, total_paid_interest,
    total_paid_amount_this_month, total_paid_principal_this_month, total_paid_interest_this_month,
    last_paid_date, due_emi_date, total_due_amount, total_due_principal, total_due_interest,
    due_amount_to_collect, due_principal_to_collect, due_interest_to_collect,
    dpd_month_current, bounce_relaxed, bounce_actual, mob_emi, mob,
    missed_payment_relaxed, missed_payment_actual, loan_close_date, dpdcurrent_classification,
    loan_registered_date, loan_type, loan_city, roi, risk_bucket,
    source_of_lead, sub_category, category, channel
)
SELECT 
    -- 1-5: Basic loan info
    lm.loan_id,
    lm.portfolio_type,
    lm.loan_total_amount,
    lm.loan_total_principal,
    lm.loan_total_interest,
    -- 6-12: Counts and dates
    lm.total_emi_count,
    lm.first_emi_date,
    lm.last_emi_date,
    lm.disburse_date,
    lm.first_disburse_date,
    lm.last_disburse_date,
    lm.status_month,
    -- 13: Loan status
    CASE WHEN IFNULL(agg.active_count, 0) > 0 THEN 'Active' ELSE 'Closed' END AS loan_status,
    -- 14-15: EMI counts
    lm.total_emi_count - IFNULL(agg.paid_emi_count, 0) AS balance_emi_count,
    IFNULL(agg.paid_emi_count, 0) AS paid_emi_count,
    -- 16-21: Paid amounts
    IFNULL(agg.total_paid_amount, 0) AS total_paid_amount,
    IFNULL(agg.total_paid_principal, 0) AS total_paid_principal,
    IFNULL(agg.total_paid_interest, 0) AS total_paid_interest,
    IFNULL(agg.total_paid_amount_this_month, 0) AS total_paid_amount_this_month,
    IFNULL(agg.total_paid_principal_this_month, 0) AS total_paid_principal_this_month,
    IFNULL(agg.total_paid_interest_this_month, 0) AS total_paid_interest_this_month,
    -- 22-23: Dates
    agg.last_paid_date,
    agg.due_emi_date,
    -- 24-29: Due amounts
    IFNULL(agg.total_due_amount, 0) AS total_due_amount,
    IFNULL(agg.total_due_principal, 0) AS total_due_principal,
    IFNULL(agg.total_due_interest, 0) AS total_due_interest,
    IFNULL(agg.due_amount_to_collect, 0) AS due_amount_to_collect,
    IFNULL(agg.due_principal_to_collect, 0) AS due_principal_to_collect,
    IFNULL(agg.due_interest_to_collect, 0) AS due_interest_to_collect,
    -- 30: DPD
    CASE
        WHEN agg.min_overdue_emi_date IS NOT NULL THEN
            CASE
                WHEN lm.status_month <= CURRENT_DATE THEN DATE_DIFF('day', agg.min_overdue_emi_date, lm.status_month)
                ELSE DATE_DIFF('day', agg.min_overdue_emi_date, CURRENT_DATE)
            END
    END AS dpd_month_current,
    -- 31-32: Bounce flags
    CASE WHEN IFNULL(agg.bounce_relax_count, 0) > 0 THEN 1 ELSE 0 END AS bounce_relaxed,
    CASE WHEN IFNULL(agg.bounce_act_count, 0) > 0 THEN 1 ELSE 0 END AS bounce_actual,
    -- 33-34: MOB
    IFNULL(DATE_DIFF('month', lm.first_emi_date, lm.status_month), 0) + 1 AS mob_emi,
    IFNULL(DATE_DIFF('month', (DATE_TRUNC('month', lm.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE, lm.status_month), 0) AS mob,
    -- 35-36: Missed payment flags
    CASE
        WHEN (DATE_TRUNC('month', lm.last_emi_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE < lm.status_month
            AND IFNULL(agg.active_count, 0) > 0 THEN 1
        ELSE CASE WHEN IFNULL(agg.bounce_relax_count, 0) > 0 THEN 1 ELSE 0 END
    END AS missed_payment_relaxed,
    CASE
        WHEN (DATE_TRUNC('month', lm.last_emi_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE < lm.status_month
            AND IFNULL(agg.active_count, 0) > 0 THEN 1
        ELSE CASE WHEN IFNULL(agg.bounce_act_count, 0) > 0 THEN 1 ELSE 0 END
    END AS missed_payment_actual,
    -- 37: Loan close date
    CASE WHEN IFNULL(agg.active_count, 0) = 0 THEN agg.last_paid_date END AS loan_close_date,
    -- 38: DPD classification
    CASE
        WHEN (DATE_TRUNC('month', lm.first_emi_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE > lm.status_month THEN 'A8_Future EMI'
        WHEN CASE WHEN agg.min_overdue_emi_date IS NOT NULL THEN 
                CASE WHEN lm.status_month <= CURRENT_DATE THEN DATE_DIFF('day', agg.min_overdue_emi_date, lm.status_month)
                ELSE DATE_DIFF('day', agg.min_overdue_emi_date, CURRENT_DATE) END
             END <= 0 THEN 'A1_Current'
        WHEN CASE WHEN agg.min_overdue_emi_date IS NOT NULL THEN 
                CASE WHEN lm.status_month <= CURRENT_DATE THEN DATE_DIFF('day', agg.min_overdue_emi_date, lm.status_month)
                ELSE DATE_DIFF('day', agg.min_overdue_emi_date, CURRENT_DATE) END
             END BETWEEN 1 AND 30 THEN 'A2_1-30 DPD'
        WHEN CASE WHEN agg.min_overdue_emi_date IS NOT NULL THEN 
                CASE WHEN lm.status_month <= CURRENT_DATE THEN DATE_DIFF('day', agg.min_overdue_emi_date, lm.status_month)
                ELSE DATE_DIFF('day', agg.min_overdue_emi_date, CURRENT_DATE) END
             END BETWEEN 31 AND 60 THEN 'A3_31-60 DPD'
        WHEN CASE WHEN agg.min_overdue_emi_date IS NOT NULL THEN 
                CASE WHEN lm.status_month <= CURRENT_DATE THEN DATE_DIFF('day', agg.min_overdue_emi_date, lm.status_month)
                ELSE DATE_DIFF('day', agg.min_overdue_emi_date, CURRENT_DATE) END
             END BETWEEN 61 AND 90 THEN 'A4_61-90 DPD'
        WHEN CASE WHEN agg.min_overdue_emi_date IS NOT NULL THEN 
                CASE WHEN lm.status_month <= CURRENT_DATE THEN DATE_DIFF('day', agg.min_overdue_emi_date, lm.status_month)
                ELSE DATE_DIFF('day', agg.min_overdue_emi_date, CURRENT_DATE) END
             END BETWEEN 91 AND 180 THEN 'A5_91-180 DPD'
        WHEN CASE WHEN agg.min_overdue_emi_date IS NOT NULL THEN 
                CASE WHEN lm.status_month <= CURRENT_DATE THEN DATE_DIFF('day', agg.min_overdue_emi_date, lm.status_month)
                ELSE DATE_DIFF('day', agg.min_overdue_emi_date, CURRENT_DATE) END
             END > 180 THEN 'A6_180+ DPD'
        WHEN agg.min_overdue_emi_date IS NULL AND IFNULL(agg.active_count, 0) = 0 THEN 'A7_Closed'
        WHEN agg.min_overdue_emi_date IS NULL AND IFNULL(agg.active_count, 0) > 0 THEN 'A1_Current'
    END AS dpdcurrent_classification,
    -- 39-47: From dm_lead_details
    dld.loan_registered_date,
    dld.loan_type,
    dld.loan_city,
    dld.rate_max_approved AS roi,
    dld.risk_bucket,
    dld.source AS source_of_lead,
    dld.sub_category,
    dld.category,
    dld.channel
FROM fair.public.tmp_loan_current_month lm
LEFT JOIN fair.public.tmp_emi_agg_inc agg ON agg.loan_id = lm.loan_id AND agg.status_month = lm.status_month
LEFT JOIN fair.public.dm_lead_details dld ON dld.loan_id::BIGINT = lm.loan_id::BIGINT;

-- Cleanup temp tables
DROP TABLE IF EXISTS fair.public.tmp_current_month;
DROP TABLE IF EXISTS fair.public.tmp_disburse_inc;
DROP TABLE IF EXISTS fair.public.tmp_emi_summary_inc;
DROP TABLE IF EXISTS fair.public.tmp_loan_current_month;
DROP TABLE IF EXISTS fair.public.tmp_emi_agg_inc;


GRANT ALL ON TABLE dm_loan_details_at_monthly in schema public to account_admin;