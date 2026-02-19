drop table if exists fair.public.dm_disburse_master;
create table fair.public.dm_disburse_master as (
WITH loan_disburse AS (
    SELECT *
    FROM (
        SELECT
            *,
            amount AS cp_amount,
            TO_TIMESTAMP(created)::DATE AS cp_created,
            TO_TIMESTAMP(udated)::DATE AS cp_updated
        FROM fair.public.cent_proposal
        WHERE deleted = 'N'
            AND proposal_state = '13000'
            AND is_collected = 'Y'
    ) cp
    INNER JOIN (
        SELECT
            *,
            amount AS cpc_amount,
            TO_TIMESTAMP(created)::DATE AS cpc_created,
            TO_TIMESTAMP(udated)::DATE AS cpc_updated
        FROM fair.public.cent_proposal_collection
        WHERE deleted = 'N'
            AND proposal_state = '14000'
            AND is_declined = '0'
    ) cpc ON cp.id::NUMERIC(38,0) = cpc.proposal_id::NUMERIC(38,0)
    INNER JOIN (
        SELECT
            *,
            id AS loan_mid,
            TO_TIMESTAMP(created)::DATE AS cl_created,
            TO_TIMESTAMP(udated)::DATE AS cl_updated
        FROM fair.public.cent_loan cl_a
        LEFT JOIN (
            SELECT
                cnd_name,
                cnd_code
            FROM fair.public.cent_cnd
            WHERE cnd_group = 'LOAN_STATE'
                AND deleted = 'N'
        ) cnd ON cnd_code::text = loan_state::text
        WHERE deleted = 'N'
    ) cl ON cl.loan_mid = cp.loan_id
    LEFT JOIN (
        SELECT
            REPLACE(cnd_name, '%', '')::DECIMAL(10, 2) AS rate,
            id AS irate_n
        FROM fair.public.cent_cnd
        WHERE cnd_group = 'INTREST_RATE'
            AND deleted = 'N'
    ) cr ON cl.max_irate_approved = cr.irate_n
    LEFT JOIN (
        SELECT
            tenure_code,
            tenure_months,
            tenure AS tenure_m
        FROM fair.public.dm_tenure_master
    ) dtm ON cl.tenure = dtm.tenure_code
    LEFT JOIN (
        SELECT DISTINCT
            loan_id,
            FIRST_VALUE(emi_amount) OVER (
                PARTITION BY loan_id
                ORDER BY due_date DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS emiamount
        FROM (
            SELECT DISTINCT
                loan_id,
                DATE_TRUNC('month', TO_TIMESTAMP(due_date + 19800))::DATE AS due_date,
                SUM(amount) OVER (
                    PARTITION BY loan_id,
                    DATE_TRUNC('month', TO_TIMESTAMP(due_date + 19800))
                ) AS emi_amount
            FROM fair.public.cent_emi
            WHERE deleted = 'N'
        ) ce
    ) ce ON cl.loan_mid = ce.loan_id
)
SELECT DISTINCT
    loan_mid AS loan_id,
    cl_created AS loan_created,
    MIN(cpc_updated) OVER (PARTITION BY loan_mid) AS first_disburse_date,
    TO_CHAR(MIN(cpc_updated) OVER (PARTITION BY loan_mid), 'Mon-YY') AS first_disburse_month,
    MAX(cpc_updated) OVER (PARTITION BY loan_mid) AS last_disburse_date,
    TO_CHAR(MAX(cpc_updated) OVER (PARTITION BY loan_mid), 'Mon-YY') AS last_disburse_month,
    SUM(cp_amount) OVER (PARTITION BY loan_mid) AS disburse_amount,
    IFNULL(rate, 0) || '%' AS roi,
    emiamount AS emi,
    IFNULL(tenure_m, 0) AS tenure_months
FROM loan_disburse
ORDER BY last_disburse_date DESC, loan_id DESC
  );
GRANT ALL ON TABLE  dm_disburse_master in schema public TO  account_admin;