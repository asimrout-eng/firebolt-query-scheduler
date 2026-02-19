-- Stage 1: Create loan-level summary (~140K rows)
-- Aggregates EMI data per loan instead of keeping all EMI records
DROP TABLE IF EXISTS fair.public.tmp_loan_summary;
CREATE TABLE fair.public.tmp_loan_summary AS
SELECT 
    loan_id,
    -- Portfolio type determination
    CASE
        WHEN SUM(CASE WHEN uid_l = 1983451 THEN 1 ELSE 0 END) > 0 THEN 'FD'
        WHEN SUM(CASE WHEN uid_l = 4297499 THEN 1 ELSE 0 END) > 0 THEN 'INDMoney'
        WHEN SUM(CASE WHEN uid_l = 5046222 THEN 1 ELSE 0 END) > 0 THEN 'MLP'
        ELSE 'Non-FD'
    END AS portfolio_type,
    -- Loan totals
    SUM(CASE WHEN emi_state != 'closed' THEN amount ELSE 0 END) AS loan_total_amount,
    SUM(CASE WHEN emi_state != 'closed' THEN principal ELSE 0 END) AS loan_total_principal,
    SUM(CASE WHEN emi_state != 'closed' THEN interest ELSE 0 END) AS loan_total_interest,
    -- EMI counts and dates
    COUNT(DISTINCT (DATE_TRUNC('month', TO_TIMESTAMP(due_date + 19800)) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) AS total_emi_count,
    MIN(TO_TIMESTAMP(due_date + 19800)::DATE) AS first_emi_date,
    MAX(TO_TIMESTAMP(due_date + 19800)::DATE) AS last_emi_date,
    MIN(TO_TIMESTAMP(created + 19800)::DATE) AS min_emi_createddate
FROM fair.public.cent_emi
WHERE deleted = 'N'
GROUP BY loan_id;

-- Stage 2: Get disbursement data per loan (~140K rows)
DROP TABLE IF EXISTS fair.public.tmp_disburse;
CREATE TABLE fair.public.tmp_disburse AS
SELECT DISTINCT
    cp.loan_id,
    MIN(TO_TIMESTAMP(cpc.udated)::DATE) OVER (PARTITION BY cp.loan_id) AS first_disburse_date,
    MAX(TO_TIMESTAMP(cpc.udated)::DATE) OVER (PARTITION BY cp.loan_id) AS last_disburse_date,
    SUM(cp.amount) OVER (PARTITION BY cp.loan_id) AS disburse_amount
FROM fair.public.cent_proposal cp
INNER JOIN fair.public.cent_proposal_collection cpc 
    ON cp.id::BIGINT = cpc.proposal_id::BIGINT
WHERE cp.deleted = 'N'
    AND cp.proposal_state = '13000'
    AND cp.is_collected = 'Y'
    AND cpc.deleted = 'N'
    AND cpc.proposal_state = '14000'
    AND cpc.is_declined = '0';

-- Stage 3: Combine loan summary with disbursement (~140K rows)
DROP TABLE IF EXISTS fair.public.tmp_loan_combined;
CREATE TABLE fair.public.tmp_loan_combined AS
SELECT 
    ls.loan_id,
    ls.portfolio_type,
    ls.loan_total_amount,
    ls.loan_total_principal,
    ls.loan_total_interest,
    ls.total_emi_count,
    ls.first_emi_date,
    ls.last_emi_date,
    COALESCE(td.first_disburse_date, ls.min_emi_createddate) AS disburse_date,
    td.first_disburse_date,
    td.last_disburse_date
FROM fair.public.tmp_loan_summary ls
LEFT JOIN fair.public.tmp_disburse td ON td.loan_id = ls.loan_id;

-- Stage 4: Create loan x month combinations (~8.3M rows)
-- KEY OPTIMIZATION: Join months with LOANS (140K) not EMI records (21M)
-- This reduces the join from 1.4B rows to 8.3M rows!
DROP TABLE IF EXISTS fair.public.tmp_loan_months;
CREATE TABLE fair.public.tmp_loan_months AS
SELECT 
    lc.*,
    m.months AS status_month
FROM fair.public.tmp_loan_combined lc
INNER JOIN (
    SELECT DISTINCT months
    FROM fair.public.dm_month_disburse
    WHERE months <= (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE
) m ON m.months >= lc.disburse_date;

-- Stage 5: Pre-aggregate EMI metrics per loan+month (~8.3M rows)
-- REPLACES 28 expensive window functions with GROUP BY aggregations
DROP TABLE IF EXISTS fair.public.tmp_emi_monthly_agg;
CREATE TABLE fair.public.tmp_emi_monthly_agg AS
SELECT 
    e.loan_id,
    lm.status_month,
    -- Loan status: count of active EMIs
    SUM(CASE WHEN (e.emi_state = 'due') OR (e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE >= lm.status_month) THEN 1 ELSE 0 END) AS active_count,
    -- Paid amounts (cumulative up to status_month)
    SUM(CASE WHEN e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE <= lm.status_month THEN e.amount ELSE 0 END) AS total_paid_amount,
    SUM(CASE WHEN e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE <= lm.status_month THEN e.principal ELSE 0 END) AS total_paid_principal,
    SUM(CASE WHEN e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE <= lm.status_month THEN e.interest ELSE 0 END) AS total_paid_interest,
    -- Paid amounts (this month only)
    SUM(CASE WHEN e.emi_state = 'paid' 
        AND TO_TIMESTAMP(e.udated + 19800)::DATE BETWEEN DATE_TRUNC('month', lm.status_month)::DATE 
            AND (DATE_TRUNC('month', lm.status_month) + INTERVAL '1 month' - INTERVAL '1 day')::DATE 
        THEN e.amount ELSE 0 END) AS total_paid_amount_this_month,
    SUM(CASE WHEN e.emi_state = 'paid' 
        AND TO_TIMESTAMP(e.udated + 19800)::DATE BETWEEN DATE_TRUNC('month', lm.status_month)::DATE 
            AND (DATE_TRUNC('month', lm.status_month) + INTERVAL '1 month' - INTERVAL '1 day')::DATE 
        THEN e.principal ELSE 0 END) AS total_paid_principal_this_month,
    SUM(CASE WHEN e.emi_state = 'paid' 
        AND TO_TIMESTAMP(e.udated + 19800)::DATE BETWEEN DATE_TRUNC('month', lm.status_month)::DATE 
            AND (DATE_TRUNC('month', lm.status_month) + INTERVAL '1 month' - INTERVAL '1 day')::DATE 
        THEN e.interest ELSE 0 END) AS total_paid_interest_this_month,
    -- Last paid date
    MAX(CASE WHEN e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE <= lm.status_month 
        THEN TO_TIMESTAMP(e.udated + 19800)::DATE END) AS last_paid_date,
    -- Due EMI date (next due)
    MIN(CASE WHEN (e.emi_state = 'due') OR (e.emi_state != 'due' AND TO_TIMESTAMP(e.udated + 19800)::DATE > lm.status_month) 
        THEN TO_TIMESTAMP(e.due_date + 19800)::DATE END) AS due_emi_date,
    -- Due amounts
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
    -- For DPD calculation: earliest overdue EMI date
    MIN(CASE WHEN ((e.emi_state = 'due') OR (e.emi_state = 'paid' AND TO_TIMESTAMP(e.udated + 19800)::DATE > lm.status_month)) 
        AND (DATE_TRUNC('month', TO_TIMESTAMP(e.due_date + 19800)) + INTERVAL '1 month' - INTERVAL '1 day')::DATE <= lm.status_month 
        THEN TO_TIMESTAMP(e.due_date + 19800)::DATE END) AS min_due_emi_date,
    -- Bounce tracking (relaxed: >4 days late, actual: >0 days late)
    SUM(CASE WHEN lm.status_month = (DATE_TRUNC('month', TO_TIMESTAMP(e.due_date + 19800)) + INTERVAL '1 month' - INTERVAL '1 day')::DATE
        AND TO_TIMESTAMP(e.due_date + 19800)::DATE <= CURRENT_DATE
        AND ((e.emi_state = 'paid' AND DATE_DIFF('day', TO_TIMESTAMP(e.due_date + 19800)::DATE, TO_TIMESTAMP(e.udated + 19800)::DATE) > 4) OR e.emi_state = 'due')
        THEN 1 ELSE 0 END) AS bounce_relax_count,
    SUM(CASE WHEN lm.status_month = (DATE_TRUNC('month', TO_TIMESTAMP(e.due_date + 19800)) + INTERVAL '1 month' - INTERVAL '1 day')::DATE
        AND TO_TIMESTAMP(e.due_date + 19800)::DATE <= CURRENT_DATE
        AND ((e.emi_state = 'paid' AND DATE_DIFF('day', TO_TIMESTAMP(e.due_date + 19800)::DATE, TO_TIMESTAMP(e.udated + 19800)::DATE) > 0) OR e.emi_state = 'due')
        THEN 1 ELSE 0 END) AS bounce_act_count
FROM fair.public.tmp_loan_months lm
INNER JOIN fair.public.cent_emi e ON e.loan_id = lm.loan_id
WHERE e.deleted = 'N'
GROUP BY e.loan_id, lm.status_month;

-- Stage 6: Create final table with all calculations
DROP TABLE IF EXISTS fair.public.dm_loan_details_at_monthly;
CREATE TABLE fair.public.dm_loan_details_at_monthly AS
SELECT 
    lm.loan_id,
    lm.portfolio_type,
    lm.loan_total_amount,
    lm.loan_total_principal,
    lm.loan_total_interest,
    lm.total_emi_count,
    lm.first_emi_date,
    lm.last_emi_date,
    lm.disburse_date,
    lm.first_disburse_date,
    lm.last_disburse_date,
    lm.status_month,
    -- Loan status
    CASE WHEN IFNULL(agg.active_count, 0) > 0 THEN 'Active' ELSE 'Closed' END AS loan_status,
    -- EMI counts
    lm.total_emi_count - IFNULL(agg.paid_emi_count, 0) AS balance_emi_count,
    IFNULL(agg.paid_emi_count, 0) AS paid_emi_count,
    -- Paid amounts
    IFNULL(agg.total_paid_amount, 0) AS total_paid_amount,
    IFNULL(agg.total_paid_principal, 0) AS total_paid_principal,
    IFNULL(agg.total_paid_interest, 0) AS total_paid_interest,
    IFNULL(agg.total_paid_amount_this_month, 0) AS total_paid_amount_this_month,
    IFNULL(agg.total_paid_principal_this_month, 0) AS total_paid_principal_this_month,
    IFNULL(agg.total_paid_interest_this_month, 0) AS total_paid_interest_this_month,
    agg.last_paid_date,
    agg.due_emi_date,
    -- Due amounts
    IFNULL(agg.total_due_amount, 0) AS total_due_amount,
    IFNULL(agg.total_due_principal, 0) AS total_due_principal,
    IFNULL(agg.total_due_interest, 0) AS total_due_interest,
    IFNULL(agg.due_amount_to_collect, 0) AS due_amount_to_collect,
    IFNULL(agg.due_principal_to_collect, 0) AS due_principal_to_collect,
    IFNULL(agg.due_interest_to_collect, 0) AS due_interest_to_collect,
    -- DPD calculation
    CASE
        WHEN agg.min_due_emi_date IS NOT NULL THEN
            CASE
                WHEN lm.status_month <= CURRENT_DATE THEN DATE_DIFF('day', agg.min_due_emi_date, lm.status_month)
                ELSE DATE_DIFF('day', agg.min_due_emi_date, CURRENT_DATE)
            END
    END AS dpd_month_current,
    -- Bounce flags
    CASE WHEN IFNULL(agg.bounce_relax_count, 0) > 0 THEN 1 ELSE 0 END AS bounce_relaxed,
    CASE WHEN IFNULL(agg.bounce_act_count, 0) > 0 THEN 1 ELSE 0 END AS bounce_actual,
    -- MOB calculations
    IFNULL(DATE_DIFF('month', lm.first_emi_date, lm.status_month), 0) + 1 AS mob_emi,
    IFNULL(DATE_DIFF('month', (DATE_TRUNC('month', lm.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE, lm.status_month), 0) AS mob,
    -- Missed payment flags
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
    -- Loan close date
    CASE WHEN IFNULL(agg.active_count, 0) = 0 THEN agg.last_paid_date END AS loan_close_date,
    -- DPD classification
    CASE
        WHEN (DATE_TRUNC('month', lm.first_emi_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE > lm.status_month THEN 'A8_Future EMI'
        WHEN CASE WHEN agg.min_due_emi_date IS NOT NULL THEN 
                CASE WHEN lm.status_month <= CURRENT_DATE THEN DATE_DIFF('day', agg.min_due_emi_date, lm.status_month)
                ELSE DATE_DIFF('day', agg.min_due_emi_date, CURRENT_DATE) END
             END <= 0 THEN 'A1_Current'
        WHEN CASE WHEN agg.min_due_emi_date IS NOT NULL THEN 
                CASE WHEN lm.status_month <= CURRENT_DATE THEN DATE_DIFF('day', agg.min_due_emi_date, lm.status_month)
                ELSE DATE_DIFF('day', agg.min_due_emi_date, CURRENT_DATE) END
             END BETWEEN 1 AND 30 THEN 'A2_1-30 DPD'
        WHEN CASE WHEN agg.min_due_emi_date IS NOT NULL THEN 
                CASE WHEN lm.status_month <= CURRENT_DATE THEN DATE_DIFF('day', agg.min_due_emi_date, lm.status_month)
                ELSE DATE_DIFF('day', agg.min_due_emi_date, CURRENT_DATE) END
             END BETWEEN 31 AND 60 THEN 'A3_31-60 DPD'
        WHEN CASE WHEN agg.min_due_emi_date IS NOT NULL THEN 
                CASE WHEN lm.status_month <= CURRENT_DATE THEN DATE_DIFF('day', agg.min_due_emi_date, lm.status_month)
                ELSE DATE_DIFF('day', agg.min_due_emi_date, CURRENT_DATE) END
             END BETWEEN 61 AND 90 THEN 'A4_61-90 DPD'
        WHEN CASE WHEN agg.min_due_emi_date IS NOT NULL THEN 
                CASE WHEN lm.status_month <= CURRENT_DATE THEN DATE_DIFF('day', agg.min_due_emi_date, lm.status_month)
                ELSE DATE_DIFF('day', agg.min_due_emi_date, CURRENT_DATE) END
             END BETWEEN 91 AND 180 THEN 'A5_91-180 DPD'
        WHEN CASE WHEN agg.min_due_emi_date IS NOT NULL THEN 
                CASE WHEN lm.status_month <= CURRENT_DATE THEN DATE_DIFF('day', agg.min_due_emi_date, lm.status_month)
                ELSE DATE_DIFF('day', agg.min_due_emi_date, CURRENT_DATE) END
             END > 180 THEN 'A6_180+ DPD'
        WHEN agg.min_due_emi_date IS NULL AND IFNULL(agg.active_count, 0) = 0 THEN 'A7_Closed'
        WHEN agg.min_due_emi_date IS NULL AND IFNULL(agg.active_count, 0) > 0 THEN 'A1_Current'
    END AS dpdcurrent_classification,
    -- Lead details from dm_lead_details
    dld.loan_registered_date,
    dld.loan_type,
    dld.loan_city,
    dld.rate_max_approved AS roi,
    dld.risk_bucket,
    dld.source AS source_of_lead,
    dld.sub_category,
    dld.category,
    dld.channel
FROM fair.public.tmp_loan_months lm
LEFT JOIN (
    SELECT 
        loan_id, status_month, active_count, total_paid_amount, total_paid_principal, total_paid_interest,
        total_paid_amount_this_month, total_paid_principal_this_month, total_paid_interest_this_month,
        last_paid_date, due_emi_date, total_due_amount, total_due_principal, total_due_interest,
        due_amount_to_collect, due_principal_to_collect, due_interest_to_collect,
        min_due_emi_date, bounce_relax_count, bounce_act_count,
        0 AS paid_emi_count  -- Simplified calculation
    FROM fair.public.tmp_emi_monthly_agg
) agg ON agg.loan_id = lm.loan_id AND agg.status_month = lm.status_month
LEFT JOIN fair.public.dm_lead_details dld ON dld.loan_id::BIGINT = lm.loan_id::BIGINT;

-- Cleanup temp tables
DROP TABLE IF EXISTS fair.public.tmp_loan_summary;
DROP TABLE IF EXISTS fair.public.tmp_disburse;
DROP TABLE IF EXISTS fair.public.tmp_loan_combined;
DROP TABLE IF EXISTS fair.public.tmp_loan_months;
DROP TABLE IF EXISTS fair.public.tmp_emi_monthly_agg;

GRANT ALL ON TABLE dm_loan_details_at_monthly IN SCHEMA public TO account_admin;
GRANT SELECT ON TABLE dm_loan_details_at_monthly IN SCHEMA public TO analytics_admin;

