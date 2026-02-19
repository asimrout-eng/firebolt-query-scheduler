drop table if exists fair.public.dm_loan_details;
create table fair.public.dm_loan_details as (
WITH tb_emi AS (
SELECT
    DISTINCT *,
    CASE
        WHEN SUM(CASE WHEN uid_l = 1983451 THEN 1 ELSE 0 END) OVER (PARTITION BY loan_id) > 0 THEN 'FD'
        WHEN SUM(CASE WHEN uid_l = 4297499 THEN 1 ELSE 0 END) OVER (PARTITION BY loan_id) > 0 THEN 'INDMoney'
        WHEN SUM(CASE WHEN uid_l = 5046222 THEN 1 ELSE 0 END) OVER (PARTITION BY loan_id) > 0 THEN 'MLP'
        ELSE 'Non-FD'
    END AS fd_product,
    CASE
        WHEN emi_state = 'paid' THEN TO_DATE(TO_TIMESTAMP(udated + 19800))
        ELSE DATE '1970-01-01'
    END AS emi_paiddate,
    TO_DATE(TO_TIMESTAMP(due_date + 19800)) AS emi_date,
    DATE_TRUNC('month', TO_DATE(TO_TIMESTAMP(due_date + 19800))) AS emi_month,
    TO_DATE(TO_TIMESTAMP(created + 19800)) AS emi_createddate,
    TO_DATE(TO_TIMESTAMP(udated + 19800)) AS emi_updateddate,
    DENSE_RANK() OVER (PARTITION BY loan_id ORDER BY DATE_TRUNC('month', TO_DATE(TO_TIMESTAMP(due_date + 19800)))) AS emi_no,
    DENSE_RANK() OVER (PARTITION BY loan_id, emi_state ORDER BY DATE_TRUNC('month', TO_DATE(TO_TIMESTAMP(due_date + 19800)))) AS emi_state_no
FROM
    fair.public.cent_emi
WHERE
    deleted = 'N'
),
tb_emidue AS (
SELECT
    DISTINCT loan_id AS loanid_emidue,
    MIN(emi_no) OVER (PARTITION BY loan_id) - 1 AS paid_emicount,
    SUM(amount) OVER (PARTITION BY loan_id) AS current_totaldueamount,
    SUM(principal) OVER (PARTITION BY loan_id) AS current_totaldueprincipal,
    SUM(interest) OVER (PARTITION BY loan_id) AS current_totaldueinterest,
    SUM(CASE WHEN emi_date <= CURRENT_DATE THEN amount ELSE 0 END) OVER (PARTITION BY loan_id) AS current_dueamount,
    SUM(CASE WHEN emi_date <= CURRENT_DATE THEN principal ELSE 0 END) OVER (PARTITION BY loan_id) AS current_dueprincipal,
    SUM(CASE WHEN emi_date <= CURRENT_DATE THEN interest ELSE 0 END) OVER (PARTITION BY loan_id) AS current_dueinterest,
    SUM(CASE WHEN uid_l = 1983451 THEN amount ELSE 0 END) OVER (PARTITION BY loan_id) AS current_fddueamount,
    SUM(CASE WHEN uid_l = 1983451 THEN principal ELSE 0 END) OVER (PARTITION BY loan_id) AS current_fddueprincipal,
    SUM(CASE WHEN uid_l = 1983451 THEN interest ELSE 0 END) OVER (PARTITION BY loan_id) AS current_fddueinterest,
    MIN(emi_date) OVER (PARTITION BY loan_id) AS emi_duedate,
    CASE
        WHEN MIN(emi_date) OVER (PARTITION BY loan_id) <= CURRENT_DATE THEN
            DATE_DIFF('day', MIN(emi_date) OVER (PARTITION BY loan_id), CURRENT_DATE)
    END AS dpd_current,
    MAX(emi_no) OVER (PARTITION BY loan_id) - MIN(emi_no) OVER (PARTITION BY loan_id) + 1 AS count_of_emi_total_due,
    MIN(CASE WHEN emi_date <= CURRENT_DATE THEN emi_date END) OVER (PARTITION BY loan_id) AS emi_duedate_first,
    MAX(CASE WHEN emi_date <= CURRENT_DATE THEN emi_date END) OVER (PARTITION BY loan_id) AS emi_duedate_last,
    DATE_DIFF('month', MIN(CASE WHEN emi_date <= CURRENT_DATE THEN emi_date END) OVER (PARTITION BY loan_id), MAX(CASE WHEN emi_date <= CURRENT_DATE THEN emi_date END) OVER (PARTITION BY loan_id)) + 1 AS count_of_emi_due,
    CASE 
        WHEN EXTRACT(DAY FROM CURRENT_DATE) < 25 THEN SUM(CASE WHEN emi_date <= DATE_ADD('day', -1, DATE_ADD('month', 1, DATE_TRUNC('month', CURRENT_DATE))) THEN amount ELSE 0 END) OVER (PARTITION BY loan_id)
        ELSE SUM(CASE WHEN emi_date <= DATE_ADD('day', -1, DATE_ADD('month', 2, DATE_TRUNC('month', CURRENT_DATE))) THEN amount ELSE 0 END) OVER (PARTITION BY loan_id)
    END AS pt_current_dueamount,
    CASE 
        WHEN EXTRACT(DAY FROM CURRENT_DATE) < 25 THEN SUM(CASE WHEN emi_date <= DATE_ADD('day', -1, DATE_ADD('month', 1, DATE_TRUNC('month', CURRENT_DATE))) THEN principal ELSE 0 END) OVER (PARTITION BY loan_id)
        ELSE SUM(CASE WHEN emi_date <= DATE_ADD('day', -1, DATE_ADD('month', 2, DATE_TRUNC('month', CURRENT_DATE))) THEN principal ELSE 0 END) OVER (PARTITION BY loan_id)
    END AS pt_current_dueprincipal,
    CASE 
        WHEN EXTRACT(DAY FROM CURRENT_DATE) < 25 THEN MIN(CASE WHEN emi_date <= DATE_ADD('day', -1, DATE_ADD('month', 1, DATE_TRUNC('month', CURRENT_DATE))) THEN emi_date END) OVER (PARTITION BY loan_id) 
        ELSE MIN(CASE WHEN emi_date <= DATE_ADD('day', -1, DATE_ADD('month', 2, DATE_TRUNC('month', CURRENT_DATE))) THEN emi_date END) OVER (PARTITION BY loan_id) 
    END AS pt_due_date_since,
    CASE 
        WHEN EXTRACT(DAY FROM CURRENT_DATE) < 25 THEN MAX(CASE WHEN emi_date <= DATE_ADD('day', -1, DATE_ADD('month', 1, DATE_TRUNC('month', CURRENT_DATE))) THEN emi_date END) OVER (PARTITION BY loan_id) 
        ELSE MAX(CASE WHEN emi_date <= DATE_ADD('day', -1, DATE_ADD('month', 2, DATE_TRUNC('month', CURRENT_DATE))) THEN emi_date END) OVER (PARTITION BY loan_id) 
    END AS pt_last_due_date,
    DATE_DIFF('month', 
        CASE 
            WHEN EXTRACT(DAY FROM CURRENT_DATE) < 25 THEN MIN(CASE WHEN emi_date <= DATE_ADD('day', -1, DATE_ADD('month', 1, DATE_TRUNC('month', CURRENT_DATE))) THEN emi_date END) OVER (PARTITION BY loan_id) 
            ELSE MIN(CASE WHEN emi_date <= DATE_ADD('day', -1, DATE_ADD('month', 2, DATE_TRUNC('month', CURRENT_DATE))) THEN emi_date END) OVER (PARTITION BY loan_id) 
        END,
        CASE 
            WHEN EXTRACT(DAY FROM CURRENT_DATE) < 25 THEN MAX(CASE WHEN emi_date <= DATE_ADD('day', -1, DATE_ADD('month', 1, DATE_TRUNC('month', CURRENT_DATE))) THEN emi_date END) OVER (PARTITION BY loan_id) 
            ELSE MAX(CASE WHEN emi_date <= DATE_ADD('day', -1, DATE_ADD('month', 2, DATE_TRUNC('month', CURRENT_DATE))) THEN emi_date END) OVER (PARTITION BY loan_id) 
        END
    ) + 1 AS pt_count_of_emi_due
FROM
    tb_emi
WHERE
    emi_state = 'due'
),
tb_emifinal AS (
SELECT
    DISTINCT loan_id AS loanid_emi,
    fd_product AS portfolio_type,
    MAX(emi_no) OVER (PARTITION BY loan_id) AS count_total_emi,
    COALESCE(paid_emicount, MAX(emi_no) OVER (PARTITION BY loan_id)) AS count_paid_emi,
    COALESCE(MAX(emi_no) OVER (PARTITION BY loan_id) - paid_emicount, 0) AS count_balance_emi,
    SUM(CASE WHEN emi_no = 1 THEN amount ELSE 0 END) OVER (PARTITION BY loan_id) AS first_emi_amount,
    CASE 
        WHEN SUM(CASE WHEN emi_no = 2 THEN amount ELSE 0 END) OVER (PARTITION BY loan_id) IS NULL THEN SUM(CASE WHEN emi_no = 1 THEN amount ELSE 0 END) OVER (PARTITION BY loan_id)
        WHEN SUM(CASE WHEN emi_no = 3 THEN amount ELSE 0 END) OVER (PARTITION BY loan_id) IS NULL THEN SUM(CASE WHEN emi_no = 2 THEN amount ELSE 0 END) OVER (PARTITION BY loan_id)
        WHEN SUM(CASE WHEN emi_no = 2 THEN amount ELSE 0 END) OVER (PARTITION BY loan_id) <= SUM(CASE WHEN emi_no = 3 THEN amount ELSE 0 END) OVER (PARTITION BY loan_id) THEN 
            SUM(CASE WHEN emi_no = 2 THEN amount ELSE 0 END) OVER (PARTITION BY loan_id)
        ELSE SUM(CASE WHEN emi_no = 3 THEN amount ELSE 0 END) OVER (PARTITION BY loan_id)
    END AS loan_emi_amount,
    SUM(CASE WHEN emi_state != 'closed' AND amount >= 0 THEN amount ELSE 0 END) OVER (PARTITION BY loan_id) AS amount_total,
    SUM(CASE WHEN emi_state != 'closed' AND principal >= 0 THEN principal ELSE 0 END) OVER (PARTITION BY loan_id) AS principal_total,
    SUM(CASE WHEN emi_state != 'closed' AND interest > 0 THEN interest ELSE 0 END) OVER (PARTITION BY loan_id) AS interest_total,
    SUM(CASE WHEN emi_state = 'paid' AND amount >= 0 THEN amount ELSE 0 END) OVER (PARTITION BY loan_id) AS emi_total_paid,	
    SUM(CASE WHEN emi_state = 'paid' AND principal >= 0 THEN principal ELSE 0 END) OVER (PARTITION BY loan_id) AS principal_total_paid,	
    SUM(CASE WHEN emi_state = 'paid' AND interest >= 0 THEN interest ELSE 0 END) OVER (PARTITION BY loan_id) AS interest_total_paid,
    MIN(emi_date) OVER (PARTITION BY loan_id) AS first_emi_date,
    MAX(emi_date) OVER (PARTITION BY loan_id) AS last_emi_date,
    CASE
        WHEN MAX(emi_paiddate) OVER (PARTITION BY loan_id) != DATE '1970-01-01' THEN
            MAX(emi_paiddate) OVER (PARTITION BY loan_id)
    END AS last_paid_date,
    COALESCE(current_totaldueamount, 0) AS current_total_due_amount,
    COALESCE(current_totaldueprincipal, 0) AS current_total_due_principal,
    COALESCE(current_totaldueinterest, 0) AS current_total_due_interest,
    COALESCE(current_dueamount, 0) AS current_due_amount,
    COALESCE(current_dueprincipal, 0) AS current_due_principal,
    COALESCE(current_dueinterest, 0) AS current_due_interest,
    emi_duedate AS emi_due_date,
    dpd_current,
    COALESCE(count_of_emi_total_due, 0) AS count_of_emi_total_due,
    emi_duedate_first,
    emi_duedate_last,
    COALESCE(count_of_emi_due, 0) AS count_of_emi_due,
    COALESCE(pt_current_dueamount, 0) AS pt_current_dueamount,
    COALESCE(pt_current_dueprincipal, 0) AS pt_current_dueprincipal,
    pt_due_date_since,
    pt_last_due_date,
    pt_count_of_emi_due,
    MAX(CASE WHEN emi_no = 1 THEN CASE WHEN emi_state != 'due' THEN emi_paiddate ELSE DATE '1900-01-02' END ELSE DATE '1900-01-01' END) OVER (PARTITION BY loan_id) AS first_emi_paid_date
FROM
    tb_emi
LEFT JOIN tb_emidue ON loan_id = loanid_emidue
),
base AS (
    SELECT
        loanid_emi AS loan_id,
        tbe.portfolio_type,
        CASE
            WHEN current_total_due_principal = 0 THEN 'Closed'
            ELSE 'Active'
        END AS loan_status,
        current_total_due_principal AS loan_statusss,
        loan_type,
        loan_city,
        loan_registered_date,
        actual_live_date AS live_date,
        first_disburse_date AS disburse_date,
        DATE_ADD('day', -1, DATE_ADD('month', 1, DATE_TRUNC('month', TO_DATE(first_disburse_date)))) AS lastday_disburse_date,
        first_disburse_month AS disburse_month,
        first_emi_date,
        last_emi_date,
        first_emi_paid_date,
        CASE
            WHEN current_total_due_principal = 0 THEN last_paid_date
        END AS loan_close_date,
        last_paid_date,
        emi_due_date AS due_emi_date,
        count_total_emi AS tenure,
        CONCAT(rate_max_approved, ' %') AS roi,
        risk_bucket,
        first_emi_amount,
        CASE WHEN loan_emi_amount > 0 THEN loan_emi_amount ELSE first_emi_amount END AS loan_emi_amount,
        loan_amount_expected,
        max_amount_approved,
        disburse_amount,
        principal_total AS loan_amount,
        count_paid_emi,
        count_balance_emi,
        interest_total AS loan_total_interest,
        amount_total AS loan_total_amount,
        principal_total_paid,
        interest_total_paid,
        emi_total_paid AS amount_total_paid,
        current_total_due_principal AS total_due_principal,
        current_total_due_interest AS total_due_interest,
        current_total_due_amount AS total_due_amount,
        current_due_amount,
        current_due_principal,
        current_due_interest,
        count_of_emi_total_due,
        emi_duedate_first,
        emi_duedate_last,
        count_of_emi_due,
        dpd_current,
        pt_current_dueamount,
        pt_current_dueprincipal,
        pt_due_date_since,
        pt_last_due_date,
        pt_count_of_emi_due,
        dld.source        AS source_of_lead,
        dld.sub_category  AS sub_category,
        dld.category      AS category,
        dld.channel       AS channel,
        dld.cibil_score,
        dld.crif_score,
        ct.registration_Fees,
        ct.processing_Fees
    FROM tb_emifinal tbe
    LEFT JOIN dm_lead_details dld
        ON CAST(dld.loan_id AS NUMERIC(38,0)) = CAST(tbe.loanid_emi AS NUMERIC(38,0))
    LEFT JOIN (
        SELECT DISTINCT
            loan_id,
            ROUND(SUM(CASE WHEN txn_type = 1358     THEN txn_amount ELSE 0 END) OVER (PARTITION BY loan_id), 0) AS registration_Fees,
            ROUND(SUM(CASE WHEN txn_type = 90133286 THEN txn_amount ELSE 0 END) OVER (PARTITION BY loan_id), 0) AS processing_Fees
        FROM cent_transaction
        WHERE deleted = 'N' AND txn_state = 200
        ORDER BY loan_id DESC
    ) ct ON ct.loan_id = tbe.loanid_emi
)

SELECT
    base.*,
    CASE
        WHEN dpd_current <= 0 THEN 'A1_Current'
        WHEN dpd_current BETWEEN 1  AND 30  THEN 'A2_1-30'
        WHEN dpd_current BETWEEN 31 AND 60  THEN 'A3_31-60 DPD'
        WHEN dpd_current BETWEEN 61 AND 90  THEN 'A4_61-90 DPD'
        WHEN dpd_current BETWEEN 91 AND 180 THEN 'A5_91-180 DPD'
        WHEN dpd_current > 180           THEN 'A6_180+ DPD'
        WHEN dpd_current IS NULL AND loan_status = 'Closed'
            THEN 'A7_Closed'
        WHEN dpd_current IS NULL AND first_emi_date > CURRENT_DATE
            THEN 'A8_Future EMI'
        WHEN dpd_current IS NULL
             AND first_emi_date <= CURRENT_DATE
             AND loan_status <> 'Closed'
            THEN 'A1_Current'
    END AS dpd_classification
FROM base);

GRANT ALL ON TABLE dm_loan_details in schema public to account_admin;
GRANT SELECT ON TABLE dm_loan_details in schema public to analytics_admin;
