-- Stage 1: Build EMI base with window functions
DROP TABLE IF EXISTS fair.public.dm_collection_master_stg_emi;
CREATE TABLE fair.public.dm_collection_master_stg_emi AS (
    SELECT DISTINCT
        *,
        CASE
            WHEN SUM(CASE WHEN uid_l = 1983451 THEN 1 ELSE 0 END) OVER (PARTITION BY loan_id) > 0 THEN 'FD'
            WHEN SUM(CASE WHEN uid_l = 4297499 THEN 1 ELSE 0 END) OVER (PARTITION BY loan_id) > 0 THEN 'INDMoney'
            ELSE 'Non-FD'
        END AS portfolio_type,
        TO_TIMESTAMP(due_date + 19800)::DATE AS emi_date,
        DATE_TRUNC('month', TO_TIMESTAMP(due_date + 19800))::DATE AS emi_month,
        TO_TIMESTAMP(created + 19800)::DATE AS emi_createddate,
        TO_TIMESTAMP(udated + 19800)::DATE AS emi_updateddate,
        MIN(TO_TIMESTAMP(created + 19800)::DATE) OVER (PARTITION BY loan_id, uid_l) AS prop_created_date,
        CASE WHEN MIN(TO_TIMESTAMP(created + 19800)::DATE) OVER (PARTITION BY loan_id, uid_l) = TO_TIMESTAMP(created + 19800)::DATE THEN 0 ELSE 1 END AS emi_foreclosure,
        DENSE_RANK() OVER (
            PARTITION BY loan_id
            ORDER BY DATE_TRUNC('month', TO_TIMESTAMP(due_date + 19800))
        ) AS emi_no,
        DENSE_RANK() OVER (
            PARTITION BY loan_id
            ORDER BY TO_TIMESTAMP(due_date + 19800)::DATE
        ) AS emi_no_act,
        CASE
            WHEN SUM(CASE WHEN emi_state = 'due' THEN 1 ELSE 0 END) OVER (
                PARTITION BY loan_id,
                DATE_TRUNC('month', TO_TIMESTAMP(due_date + 19800))
            ) > 0 THEN 'due'
            ELSE 'paid'
        END AS emi_status,
        DENSE_RANK() OVER (
            PARTITION BY loan_id,
            CASE
                WHEN emi_state IN ('paid', 'closed') THEN 'paid'
                ELSE 'due'
            END
            ORDER BY DATE_TRUNC('month', TO_TIMESTAMP(due_date + 19800))
        ) AS emistate_no,
        CASE
            WHEN emi_state = 'due' THEN 
                DENSE_RANK() OVER (
                    PARTITION BY loan_id,
                    CASE
                        WHEN emi_state IN ('paid', 'closed') THEN 'paid'
                        ELSE 'due'
                    END
                    ORDER BY DATE_TRUNC('month', TO_TIMESTAMP(due_date + 19800))
                )
            ELSE 0
        END AS emidue_no,
        CASE
            WHEN emi_state IN ('paid', 'closed') THEN 
                DENSE_RANK() OVER (
                    PARTITION BY loan_id,
                    CASE
                        WHEN emi_state IN ('paid', 'closed') THEN 'paid'
                        ELSE 'due'
                    END
                    ORDER BY DATE_TRUNC('month', TO_TIMESTAMP(due_date + 19800))
                )
            ELSE 0
        END AS emipaid_no,
        DENSE_RANK() OVER (
            PARTITION BY loan_id,
            TO_TIMESTAMP(created + 19800)::DATE
            ORDER BY DATE_TRUNC('month', TO_TIMESTAMP(due_date + 19800))
        ) AS created_no
    FROM fair.public.cent_emi
    WHERE deleted = 'N'
);

-- Stage 2: Aggregate EMI data per loan
DROP TABLE IF EXISTS fair.public.dm_collection_master_stg_base;
CREATE TABLE fair.public.dm_collection_master_stg_base AS (
    SELECT DISTINCT
        loan_id,
        portfolio_type,
        CASE
            WHEN SUM(CASE WHEN emi_state = 'due' THEN 1 ELSE 0 END) > 0 THEN 'Active'
            ELSE 'closed'
        END AS loan_status,
        MAX(emi_no) AS total_emi_count,
        IFNULL(MAX(CASE WHEN emi_status = 'paid' THEN emipaid_no ELSE 0 END), 0) AS paid_emi_count,
        IFNULL(MAX(CASE WHEN emi_status = 'due' THEN emidue_no ELSE 0 END), 0) AS balance_emi_count,
        SUM(CASE WHEN emi_no = 1 THEN amount ELSE 0 END) AS first_emi_amount,
        SUM(CASE WHEN emi_no = 2 THEN amount ELSE 0 END) AS emi_amount,
        SUM(CASE WHEN emi_state != 'closed' THEN amount ELSE 0 END) AS loan_total_amount,
        SUM(CASE WHEN emi_state != 'closed' THEN principal ELSE 0 END) AS loan_total_principal,
        SUM(CASE WHEN emi_state != 'closed' THEN interest ELSE 0 END) AS loan_total_interest,
        MIN(emi_date) AS first_emi_date,
        MAX(emi_date) AS last_emi_date,
        SUM(CASE WHEN emi_state = 'paid' THEN amount ELSE 0 END) AS total_paid_amount,
        SUM(CASE WHEN emi_state = 'paid' THEN principal ELSE 0 END) AS total_paid_principal,
        SUM(CASE WHEN emi_state = 'paid' THEN interest ELSE 0 END) AS total_paid_interest,
        SUM(CASE WHEN emi_state = 'paid' AND emi_updateddate BETWEEN DATE_TRUNC('month', CURRENT_DATE)::DATE AND CURRENT_DATE THEN amount ELSE 0 END) AS total_paid_amount_this_month,
        SUM(CASE WHEN emi_state = 'paid' AND emi_updateddate BETWEEN DATE_TRUNC('month', CURRENT_DATE)::DATE AND CURRENT_DATE THEN principal ELSE 0 END) AS total_paid_principal_this_month,
        SUM(CASE WHEN emi_state = 'paid' AND emi_updateddate BETWEEN DATE_TRUNC('month', CURRENT_DATE)::DATE AND CURRENT_DATE THEN interest ELSE 0 END) AS total_paid_interest_this_month,
        SUM(CASE WHEN emi_state = 'paid' AND emi_updateddate BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE AND (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN amount ELSE 0 END) AS total_paid_amount_previous_month,
        SUM(CASE WHEN emi_state = 'paid' AND emi_updateddate BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE AND (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN principal ELSE 0 END) AS total_paid_principal_previous_month,
        SUM(CASE WHEN emi_state = 'paid' AND emi_updateddate BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE AND (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN interest ELSE 0 END) AS total_paid_interest_previous_month,
        MAX(CASE WHEN emi_state = 'paid' THEN emi_updateddate END) AS last_paid_date,
        MAX(CASE WHEN emi_status = 'paid' THEN emi_updateddate::DATE END) AS last_paid_date_full_emi,
        SUM(CASE WHEN emi_state = 'due' THEN amount ELSE 0 END) AS total_due_amount,
        SUM(CASE WHEN emi_state = 'due' THEN principal ELSE 0 END) AS total_due_principal,
        SUM(CASE WHEN emi_state = 'due' THEN interest ELSE 0 END) AS total_due_interest,
        SUM(CASE WHEN emi_createddate < DATE_TRUNC('month', CURRENT_DATE)::DATE AND ((emi_state = 'due') OR (emi_state != 'due' AND emi_updateddate >= DATE_TRUNC('month', CURRENT_DATE)::DATE)) THEN amount ELSE 0 END) AS total_due_amount_start_month,
        SUM(CASE WHEN (emi_state = 'due') OR (emi_state = 'paid' AND emi_updateddate >= DATE_TRUNC('month', CURRENT_DATE)::DATE) THEN principal ELSE 0 END) AS total_due_principal_start_month,
        SUM(CASE WHEN emi_createddate < DATE_TRUNC('month', CURRENT_DATE)::DATE AND ((emi_state = 'due') OR (emi_state != 'due' AND emi_updateddate >= DATE_TRUNC('month', CURRENT_DATE)::DATE)) THEN interest ELSE 0 END) AS total_due_interest_start_month,
        SUM(CASE WHEN emi_state = 'due' AND emi_month <= (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN amount ELSE 0 END) AS due_amount_to_collect,
        SUM(CASE WHEN emi_state = 'due' AND emi_month <= (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN principal ELSE 0 END) AS due_principal_to_collect,
        SUM(CASE WHEN emi_state = 'due' AND emi_month <= (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN interest ELSE 0 END) AS due_interest_to_collect,
        SUM(CASE WHEN emi_state = 'due' AND emi_month = DATE_TRUNC('month', CURRENT_DATE)::DATE THEN amount ELSE 0 END) AS due_amount_current_month_emi,
        SUM(CASE WHEN emi_state = 'due' AND emi_month = DATE_TRUNC('month', CURRENT_DATE)::DATE THEN principal ELSE 0 END) AS due_principal_current_month_emi,
        SUM(CASE WHEN emi_state = 'due' AND emi_month = DATE_TRUNC('month', CURRENT_DATE)::DATE THEN interest ELSE 0 END) AS due_interest_current_month_emi,
        SUM(CASE WHEN emi_createddate < DATE_TRUNC('month', CURRENT_DATE)::DATE AND ((emi_state = 'due' AND emi_month <= (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) OR (emi_state != 'due' AND emi_month <= (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AND emi_updateddate BETWEEN DATE_TRUNC('month', CURRENT_DATE)::DATE AND (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE)) THEN amount ELSE 0 END) AS due_amount_to_collect_month_start,
        SUM(CASE WHEN emi_createddate < DATE_TRUNC('month', CURRENT_DATE)::DATE AND ((emi_state = 'due' AND emi_month <= (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) OR (emi_state != 'due' AND emi_month <= (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AND emi_updateddate BETWEEN DATE_TRUNC('month', CURRENT_DATE)::DATE AND (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE)) THEN principal ELSE 0 END) AS due_principal_to_collect_month_start,
        SUM(CASE WHEN emi_createddate < DATE_TRUNC('month', CURRENT_DATE)::DATE AND ((emi_state = 'due' AND emi_month <= (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) OR (emi_state != 'due' AND emi_month <= (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AND emi_updateddate BETWEEN DATE_TRUNC('month', CURRENT_DATE)::DATE AND (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE)) THEN interest ELSE 0 END) AS due_interest_to_collect_month_start,
        SUM(CASE WHEN emi_createddate < DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE AND ((emi_state = 'due' AND emi_month <= (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::DATE) OR (emi_state != 'due' AND emi_month <= (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::DATE AND emi_updateddate BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE AND CURRENT_DATE)) THEN amount ELSE 0 END) AS due_amount_to_collect_previous_month_start,
        SUM(CASE WHEN emi_createddate < DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE AND ((emi_state = 'due' AND emi_month <= (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::DATE) OR (emi_state != 'due' AND emi_month <= (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::DATE AND emi_updateddate BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE AND CURRENT_DATE)) THEN principal ELSE 0 END) AS due_principal_to_collect_previous_month_start,
        SUM(CASE WHEN emi_createddate < DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE AND ((emi_state = 'due' AND emi_month <= (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::DATE) OR (emi_state != 'due' AND emi_month <= (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::DATE AND emi_updateddate BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE AND CURRENT_DATE)) THEN interest ELSE 0 END) AS due_interest_to_collect_previous_month_start,
        SUM(CASE WHEN emi_createddate < DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE AND ((emi_state = 'due') OR (emi_state != 'due' AND emi_updateddate BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE AND (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE)) THEN principal ELSE 0 END) AS pos_previous_month,
        MAX(CASE WHEN emi_state = 'due' THEN emi_no ELSE 0 END) AS count_of_emi_total_due,
        MAX(CASE WHEN emi_state = 'due' AND emi_date <= (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN emi_no ELSE 0 END) AS count_of_emi_due,
        MAX(CASE WHEN emi_createddate < DATE_TRUNC('month', CURRENT_DATE)::DATE AND ((emi_state = 'due' AND emi_month <= DATE_TRUNC('month', CURRENT_DATE)::DATE) OR (emi_state != 'due' AND emi_updateddate >= DATE_TRUNC('month', CURRENT_DATE)::DATE AND emi_month <= DATE_TRUNC('month', CURRENT_DATE)::DATE)) THEN emi_no ELSE 0 END) AS due_emi_count_start_month,
        MIN(CASE WHEN emi_state = 'due' THEN emi_date END) AS due_emi_date,
        MIN(CASE WHEN emi_createddate < DATE_TRUNC('month', CURRENT_DATE)::DATE AND ((emi_state = 'due') OR (emi_state != 'due' AND emi_updateddate >= DATE_TRUNC('month', CURRENT_DATE)::DATE)) THEN emi_date END) AS due_emi_date_start_month,
        CASE WHEN MIN(CASE WHEN emi_state = 'due' THEN emi_month END) <= DATE_TRUNC('month', CURRENT_DATE)::DATE THEN DATE_DIFF('day', MIN(CASE WHEN emi_state = 'due' THEN emi_date END), CURRENT_DATE) END AS dpd_current,
        CASE WHEN MIN(CASE WHEN emi_createddate < DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE AND ((emi_state = 'due') OR (emi_state != 'due' AND emi_updateddate >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE)) THEN emi_month END) <= (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN DATE_DIFF('day', MIN(CASE WHEN emi_createddate < DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE AND ((emi_state = 'due') OR (emi_state != 'due' AND emi_updateddate >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE)) THEN emi_date END), DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE) END AS dpd_previous_month_start,
        CASE WHEN MIN(CASE WHEN emi_createddate < DATE_TRUNC('month', CURRENT_DATE)::DATE AND ((emi_state = 'due') OR (emi_state != 'due' AND emi_updateddate >= DATE_TRUNC('month', CURRENT_DATE)::DATE)) THEN emi_month END) <= DATE_TRUNC('month', CURRENT_DATE)::DATE THEN DATE_DIFF('day', MIN(CASE WHEN emi_createddate < DATE_TRUNC('month', CURRENT_DATE)::DATE AND ((emi_state = 'due') OR (emi_state != 'due' AND emi_updateddate >= DATE_TRUNC('month', CURRENT_DATE)::DATE)) THEN emi_date END), DATE_TRUNC('month', CURRENT_DATE)::DATE) END AS dpd_month_start
    FROM fair.public.dm_collection_master_stg_emi
    GROUP BY ALL
);

-- Stage 3: Add classifications and build final table
DROP TABLE IF EXISTS fair.public.dm_collection_master;
CREATE TABLE fair.public.dm_collection_master AS (
    SELECT
        CURRENT_TIMESTAMP AS report_datetime,
        b.*,
        CASE
            WHEN b.first_emi_date > CURRENT_DATE THEN 'A8_Future EMI'
            WHEN b.dpd_current <= 0 THEN 'A1_Current'
            WHEN b.dpd_current BETWEEN 1 AND 30 THEN 'A2_1-30'
            WHEN b.dpd_current BETWEEN 31 AND 60 THEN 'A3_31-60'
            WHEN b.dpd_current BETWEEN 61 AND 90 THEN 'A4_61-90'
            WHEN b.dpd_current BETWEEN 91 AND 180 THEN 'A5_91-180'
            WHEN b.dpd_current > 180 THEN 'A6_180+'
            WHEN b.dpd_current IS NULL AND b.total_due_principal <= 0 THEN 'A7_Closed'
            WHEN b.dpd_current IS NULL AND b.total_due_principal > 0 THEN 'A1_Current'
        END AS dpdcurrent_classification,
        CASE
            WHEN b.first_emi_date > CURRENT_DATE THEN 'A9_Future EMI'
            WHEN b.dpd_current <= 4 THEN 'A1_0-4'
            WHEN b.dpd_current BETWEEN 5 AND 30 THEN 'A2_5-30'
            WHEN b.dpd_current BETWEEN 31 AND 60 THEN 'A3_31-60'
            WHEN b.dpd_current BETWEEN 61 AND 90 THEN 'A4_61-90'
            WHEN b.dpd_current BETWEEN 91 AND 180 THEN 'A5_91-180'
            WHEN b.dpd_current BETWEEN 181 AND 360 THEN 'A6_181-360'
            WHEN b.dpd_current > 360 THEN 'A7_360+'
            WHEN b.dpd_current IS NULL AND b.total_due_principal <= 0 THEN 'A8_Closed'
            WHEN b.dpd_current IS NULL AND b.total_due_principal > 0 THEN 'A1_0-4'
        END AS dpdcurrent_classification_finance,
        CASE
            WHEN b.first_emi_date BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE AND (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN 'A0_1st EMI'
            WHEN b.first_emi_date > (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN 'A8_Future EMI'
            WHEN b.pos_previous_month <= 0 THEN 'A7_Closed'
            WHEN b.dpd_previous_month_start <= 0 THEN 'A1_Current'
            WHEN b.dpd_previous_month_start BETWEEN 1 AND 30 THEN 'A2_1-30'
            WHEN b.dpd_previous_month_start BETWEEN 31 AND 60 THEN 'A3_31-60'
            WHEN b.dpd_previous_month_start BETWEEN 61 AND 90 THEN 'A4_61-90'
            WHEN b.dpd_previous_month_start BETWEEN 91 AND 180 THEN 'A5_91-180'
            WHEN b.dpd_previous_month_start > 180 THEN 'A6_180+'
            WHEN b.dpd_previous_month_start IS NULL AND b.first_emi_date > (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN 'A8_Future EMI'
            WHEN b.dpd_previous_month_start IS NULL THEN 'A1_Current'
        END AS dpd_previous_month_day_classification,
        CASE
            WHEN b.first_emi_date BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::DATE AND (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN 'A0_1st EMI'
            WHEN b.first_emi_date > (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN 'A9_Future EMI'
            WHEN b.pos_previous_month <= 0 THEN 'A8_Closed'
            WHEN b.dpd_previous_month_start <= 4 THEN 'A1_0-4'
            WHEN b.dpd_previous_month_start BETWEEN 5 AND 30 THEN 'A2_5-30'
            WHEN b.dpd_previous_month_start BETWEEN 31 AND 60 THEN 'A3_31-60'
            WHEN b.dpd_previous_month_start BETWEEN 61 AND 90 THEN 'A4_61-90'
            WHEN b.dpd_previous_month_start BETWEEN 91 AND 180 THEN 'A5_91-180'
            WHEN b.dpd_previous_month_start BETWEEN 181 AND 360 THEN 'A6_181-360'
            WHEN b.dpd_previous_month_start > 360 THEN 'A7_360+'
            WHEN b.dpd_previous_month_start IS NULL AND b.first_emi_date > (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN 'A9_Future EMI'
            WHEN b.dpd_previous_month_start IS NULL THEN 'A1_0-4'
        END AS dpd_previous_month_day_classification_finance,
        CASE
            WHEN b.first_emi_date BETWEEN DATE_TRUNC('month', CURRENT_DATE)::DATE AND (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN 'A0_1st EMI'
            WHEN b.first_emi_date > (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN 'A8_Future EMI'
            WHEN b.total_due_principal_start_month <= 0 THEN 'A7_Closed'
            WHEN b.dpd_month_start <= 0 THEN 'A1_Current'
            WHEN b.dpd_month_start BETWEEN 1 AND 30 THEN 'A2_1-30'
            WHEN b.dpd_month_start BETWEEN 31 AND 60 THEN 'A3_31-60'
            WHEN b.dpd_month_start BETWEEN 61 AND 90 THEN 'A4_61-90'
            WHEN b.dpd_month_start BETWEEN 91 AND 180 THEN 'A5_91-180'
            WHEN b.dpd_month_start > 180 THEN 'A6_180+'
            WHEN b.dpd_month_start IS NULL AND b.first_emi_date > (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN 'A8_Future EMI'
            WHEN b.dpd_month_start IS NULL THEN 'A1_Current'
        END AS dpdstart_classification,
        CASE
            WHEN b.first_emi_date BETWEEN DATE_TRUNC('month', CURRENT_DATE)::DATE AND (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN 'A0_1st EMI'
            WHEN b.first_emi_date > (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN 'A9_Future EMI'
            WHEN b.total_due_principal_start_month <= 0 THEN 'A8_Closed'
            WHEN b.dpd_month_start <= 4 THEN 'A1_0-4'
            WHEN b.dpd_month_start BETWEEN 5 AND 30 THEN 'A2_5-30'
            WHEN b.dpd_month_start BETWEEN 31 AND 60 THEN 'A3_31-60'
            WHEN b.dpd_month_start BETWEEN 61 AND 90 THEN 'A4_61-90'
            WHEN b.dpd_month_start BETWEEN 91 AND 180 THEN 'A5_91-180'
            WHEN b.dpd_month_start BETWEEN 181 AND 360 THEN 'A5_181-360'
            WHEN b.dpd_month_start > 360 THEN 'A6_360+'
            WHEN b.dpd_month_start IS NULL AND b.first_emi_date > (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN 'A9_Future EMI'
            WHEN b.dpd_month_start IS NULL THEN 'A1_0-4'
        END AS dpdstart_classification_finance,
        CASE WHEN b.loan_status = 'Closed' THEN b.last_paid_date END AS loan_close_date
    FROM fair.public.dm_collection_master_stg_base b
    LEFT JOIN fair.public.dm_lead_details dld ON b.loan_id::numeric(38,0) = dld.loan_id::numeric(38,0)
    WHERE (dld.first_disburse_date < DATE_TRUNC('month', CURRENT_DATE)::DATE 
        OR (dld.first_disburse_date IS NULL AND b.first_emi_date <= CURRENT_DATE))
        AND dld.plus <> 420
);

-- Stage 4: Cleanup staging tables
DROP TABLE IF EXISTS fair.public.dm_collection_master_stg_emi;
DROP TABLE IF EXISTS fair.public.dm_collection_master_stg_base;

-- Stage 5: Grant permissions
GRANT ALL ON TABLE dm_collection_master IN SCHEMA public TO account_admin;
