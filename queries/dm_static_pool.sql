drop table if exists fair.public.dm_static_pool;
CREATE TABLE fair.public.dm_static_pool AS
WITH tb_main AS (
    SELECT *
    FROM fair.public.dm_loan_details_at_monthly
),
tb_lead_details AS (
    SELECT *
    FROM fair.public.dm_lead_details
),
tb_top_up_details AS (
    SELECT
        dld1.loan_id,
        user_pan,
        disburse_date,
        loan_close_date,
        ROW_NUMBER() OVER (PARTITION BY dld2.user_pan ORDER BY dld1.disburse_date) AS pan_count,
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY dld2.user_pan ORDER BY dld1.disburse_date) > 1 THEN 1 ELSE 0 END AS top_up,
        LAG(dld2.sub_category) OVER (PARTITION BY dld2.user_pan ORDER BY dld1.disburse_date) AS old_sub_category,
        LAG(dld2.category) OVER (PARTITION BY dld2.user_pan ORDER BY dld1.disburse_date) AS old_category,
        LAG(dld2.channel) OVER (PARTITION BY dld2.user_pan ORDER BY dld1.disburse_date) AS old_channel,
        LAG(last_emi_date) OVER (PARTITION BY dld2.user_pan ORDER BY dld1.disburse_date) AS old_loan_last_emi_date,
        LAG(loan_close_date) OVER (PARTITION BY dld2.user_pan ORDER BY dld1.disburse_date) AS old_loan_close_date,
        CASE
            WHEN CASE WHEN ROW_NUMBER() OVER (PARTITION BY dld2.user_pan ORDER BY dld1.disburse_date) > 1 THEN 1 ELSE 0 END = 1 
                 AND LAG(loan_close_date) OVER (PARTITION BY dld2.user_pan ORDER BY dld1.disburse_date) IS NOT NULL 
            THEN DATE_DIFF('day', disburse_date, LAG(loan_close_date) OVER (PARTITION BY dld2.user_pan ORDER BY dld1.disburse_date))
            WHEN CASE WHEN ROW_NUMBER() OVER (PARTITION BY dld2.user_pan ORDER BY dld1.disburse_date) > 1 THEN 1 ELSE 0 END = 1 
                 AND LAG(loan_close_date) OVER (PARTITION BY dld2.user_pan ORDER BY dld1.disburse_date) IS NULL 
            THEN 30
            ELSE 0
        END AS top_date_diff
    FROM tb_main dld1
    INNER JOIN fair.public.dm_lead_details dld2
        ON dld1.loan_id::DECIMAL(38,0) = dld2.loan_id::DECIMAL(38,0)
        AND status_month >= CURRENT_DATE 
        AND dld2.channel != 'Alliances' 
        AND dld2.user_pan != ''
),
tb_mandate AS (
    SELECT DISTINCT 
        loan_id,
        unique_reference_no
    FROM fair.public.cent_nach_collection_register
    WHERE deleted = 'N'
        AND ledger_type NOT IN ('PENALTY')
        AND TO_TIMESTAMP(created::BIGINT + 19800)::DATE <= DATE_TRUNC('month', CURRENT_DATE)::DATE
),
  tb_with_crif AS (
SELECT DISTINCT
    dldam.loan_id,
    dldam.portfolio_type,
    dldam.loan_total_principal AS loan_amount,
    (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS disburse_month,
    'CY ' || EXTRACT(YEAR FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE)::TEXT AS cy,
    'CY ' || EXTRACT(YEAR FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE)::TEXT || '-' ||
    CASE
        WHEN EXTRACT(MONTH FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) BETWEEN 1 AND 3 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) BETWEEN 4 AND 6 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) BETWEEN 7 AND 9 THEN 'Q3'
        WHEN EXTRACT(MONTH FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) BETWEEN 10 AND 12 THEN 'Q4'
    END AS cy_qtr,
    CASE
        WHEN EXTRACT(MONTH FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) <= 3 
        THEN 'FY ' || (EXTRACT(YEAR FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) - 1)::TEXT || '-' || 
             SUBSTRING((EXTRACT(YEAR FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE))::TEXT, 3, 2)
        ELSE 'FY ' || EXTRACT(YEAR FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE)::TEXT || '-' || 
             SUBSTRING((EXTRACT(YEAR FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) + 1)::TEXT, 3, 2)
    END AS fy,
    CASE
        WHEN EXTRACT(MONTH FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) <= 3 
        THEN 'FY ' || (EXTRACT(YEAR FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) - 1)::TEXT || '-' || 
             SUBSTRING((EXTRACT(YEAR FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE))::TEXT, 3, 2) || '-' ||
             CASE
                 WHEN EXTRACT(MONTH FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) BETWEEN 4 AND 6 THEN 'Q1'
                 WHEN EXTRACT(MONTH FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) BETWEEN 7 AND 9 THEN 'Q2'
                 WHEN EXTRACT(MONTH FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) BETWEEN 10 AND 12 THEN 'Q3'
                 WHEN EXTRACT(MONTH FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) BETWEEN 1 AND 3 THEN 'Q4'
             END
        ELSE 'FY ' || EXTRACT(YEAR FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE)::TEXT || '-' || 
             SUBSTRING((EXTRACT(YEAR FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) + 1)::TEXT, 3, 2) || '-' ||
             CASE
                 WHEN EXTRACT(MONTH FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) BETWEEN 4 AND 6 THEN 'Q1'
                 WHEN EXTRACT(MONTH FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) BETWEEN 7 AND 9 THEN 'Q2'
                 WHEN EXTRACT(MONTH FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) BETWEEN 10 AND 12 THEN 'Q3'
                 WHEN EXTRACT(MONTH FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) BETWEEN 1 AND 3 THEN 'Q4'
             END
    END AS fy_qtr,
    CASE
        WHEN EXTRACT(MONTH FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) <= 3 
        THEN EXTRACT(MONTH FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) + 9
        ELSE EXTRACT(MONTH FROM (DATE_TRUNC('month', dldam.disburse_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE) - 3
    END AS fy_month,
    dld.gender,
    dld.generations,
    dld.user_pin AS pincode,
    dld.user_city AS borrower_city,
    dld.user_district AS borrower_district,
    dld.user_state AS borrower_state,
    dld.user_zone AS borrower_zone,
    dld.residence_type,
    dld.employement_type,
    '' AS cirf_score,
    CASE
        WHEN dld.source = 'IMB' AND dld.parent_uid <> 0 THEN
            (SELECT CASE WHEN dld3.crif_score IS NOT NULL AND dld3.crif_score != '' THEN dld3.crif_score::INT END 
             FROM fair.public.dm_lead_details dld3 
             WHERE dld3.user_id = dld.parent_uid AND dld3.loan_id NOT IN (1005216551, 1005249730, 1005249824))
        ELSE
            CASE WHEN dld.crif_score IS NOT NULL AND dld.crif_score != '' THEN dld.crif_score::INT END
    END AS crif_score_original,
    dld.loan_type,
    dld.loan_city,
    dld.tenure_months AS tenure,
    CASE
        WHEN dld.tenure_monthsno <= 11 THEN 'A1 <12'
        WHEN dld.tenure_monthsno BETWEEN 12 AND 15 THEN 'A2 12-15'
        WHEN dld.tenure_monthsno BETWEEN 16 AND 18 THEN 'A3 16-18'
        WHEN dld.tenure_monthsno BETWEEN 19 AND 24 THEN 'A4 19-24'
        WHEN dld.tenure_monthsno BETWEEN 25 AND 30 THEN 'A5 25-30'
        WHEN dld.tenure_monthsno BETWEEN 31 AND 36 THEN 'A6 31-36'
    END AS tenure_group,
    dld.rate_max_approved AS roi,
    dld.risk_bucket,
    dld.sub_category,
    dld.source,
    dld.category,
    CASE WHEN dld.channel != 'Alliances' AND dldam.loan_total_principal < 25001 THEN 'Pocket Loan'
         ELSE CASE WHEN dld.channel = 'Marketing' THEN 'Organic' ELSE dld.channel END
    END AS channel,
    dld.channel AS channel_old,
    CASE WHEN dldam.loan_status = 'Active' THEN 1 ELSE 0 END AS loan_status,
    dldam.total_due_principal AS pos,
    CASE
        WHEN dldam.roi < 20 THEN 'A1 <20%'
        WHEN dldam.roi <= 25 THEN 'A2 20%-25%'
        WHEN dldam.roi <= 30 THEN 'A3 26%-30%'
        WHEN dldam.roi <= 36 THEN 'A4 31%-36%'
        ELSE 'A5 36%+'
    END AS roi_group,
    CASE
        WHEN dld.age_loanregister < 25 THEN 'A1 <25'
        WHEN dld.age_loanregister < 31 THEN 'A2 25-30'
        WHEN dld.age_loanregister < 36 THEN 'A3 31-35'
        WHEN dld.age_loanregister < 41 THEN 'A4 36-40'
        WHEN dld.age_loanregister < 46 THEN 'A5 41-45'
        WHEN dld.age_loanregister < 51 THEN 'A6 46-50'
        WHEN dld.age_loanregister < 56 THEN 'A7 51-55'
        ELSE 'A8 55+'
    END AS age_group,
    CASE WHEN dld.income IS NOT NULL AND dld.income != '' THEN
        REGEXP_EXTRACT(dld.income, '[0-9]+.[0-9]+')::DECIMAL(10,2)
    END AS income,
    CASE WHEN dld.employement_type = 'Salaried Employee' THEN
            CASE
                WHEN dld.income IS NULL OR dld.income = '' THEN 'SA_NA'
                WHEN REGEXP_EXTRACT(dld.income, '[0-9]+.[0-9]+')::DECIMAL(10,2) < 26000 THEN 'SA01 <26K'
                WHEN REGEXP_EXTRACT(dld.income, '[0-9]+.[0-9]+')::DECIMAL(10,2) < 28000 THEN 'SA02 26K-28K'
                WHEN REGEXP_EXTRACT(dld.income, '[0-9]+.[0-9]+')::DECIMAL(10,2) < 30000 THEN 'SA03 28K-30K'
                WHEN REGEXP_EXTRACT(dld.income, '[0-9]+.[0-9]+')::DECIMAL(10,2) < 33000 THEN 'SA04 30K-33K'
                WHEN REGEXP_EXTRACT(dld.income, '[0-9]+.[0-9]+')::DECIMAL(10,2) < 44000 THEN 'SA05 33K-44K'
                ELSE 'SA06 44K+'
            END
        ELSE
            CASE
                WHEN dld.income IS NULL OR dld.income = '' THEN 'SE_NA'
                WHEN REGEXP_EXTRACT(dld.income, '[0-9]+.[0-9]+')::DECIMAL(10,2) < 44000 THEN 'SE01 <44K'
                WHEN REGEXP_EXTRACT(dld.income, '[0-9]+.[0-9]+')::DECIMAL(10,2) < 51000 THEN 'SE02 44K-51K'
                WHEN REGEXP_EXTRACT(dld.income, '[0-9]+.[0-9]+')::DECIMAL(10,2) < 55000 THEN 'SE03 51K-55K'
                WHEN REGEXP_EXTRACT(dld.income, '[0-9]+.[0-9]+')::DECIMAL(10,2) < 58000 THEN 'SE04 55K-58K'
                WHEN REGEXP_EXTRACT(dld.income, '[0-9]+.[0-9]+')::DECIMAL(10,2) < 62000 THEN 'SE05 58K-62K'
                WHEN REGEXP_EXTRACT(dld.income, '[0-9]+.[0-9]+')::DECIMAL(10,2) < 90000 THEN 'SE06 62K-90K'
                ELSE 'SE07 90K+'
            END
    END AS income_group,
    CASE
        WHEN dldam.loan_total_principal < 25001 THEN 'A01 <=25K'
        WHEN dldam.loan_total_principal < 50001 THEN 'A02 25K-50K'
        WHEN dldam.loan_total_principal < 100001 THEN 'A03 50K-1L'
        WHEN dldam.loan_total_principal < 150001 THEN 'A04 1L-1.5L'
        WHEN dldam.loan_total_principal < 200001 THEN 'A05 1.5L-2L'
        WHEN dldam.loan_total_principal < 250001 THEN 'A06 2L-2.5L'
        WHEN dldam.loan_total_principal < 300001 THEN 'A07 2.5L-3L'
        WHEN dldam.loan_total_principal < 500001 THEN 'A08 3L-5L'
        WHEN dldam.loan_total_principal < 700001 THEN 'A09 5L-7L'
        ELSE 'A10 7L+'
    END AS loan_amount_group_new,
    CASE WHEN dld.foir_before IS NOT NULL AND dld.foir_before != '' THEN
        dld.foir_before::DECIMAL(10,3)
    END AS foir_before,
    CASE WHEN dld.employement_type = 'Salaried Employee' THEN
            CASE
                WHEN dld.foir_before IS NULL THEN 'SA_NA'
                WHEN dld.foir_before = '' THEN 'SA_NA'
                WHEN dld.foir_before::DECIMAL(10,3) < 18 THEN 'SA01 <18'
                WHEN dld.foir_before::DECIMAL(10,3) < 25 THEN 'SA02 18-25'
                WHEN dld.foir_before::DECIMAL(10,3) < 32 THEN 'SA03 25-32'
                WHEN dld.foir_before::DECIMAL(10,3) < 37 THEN 'SA04 32-37'
                WHEN dld.foir_before::DECIMAL(10,3) < 42 THEN 'SA05 37-42'
                WHEN dld.foir_before::DECIMAL(10,3) < 48 THEN 'SA06 42-48'
                WHEN dld.foir_before::DECIMAL(10,3) < 54 THEN 'SA07 48-54'
                ELSE 'SA08 54+'
            END
        ELSE
            CASE
                WHEN dld.foir_before IS NULL THEN 'SE_NA'
                WHEN dld.foir_before = '' THEN 'SE_NA'
                WHEN dld.foir_before::DECIMAL(10,3) < 19 THEN 'SE01 <19'
                WHEN dld.foir_before::DECIMAL(10,3) < 25 THEN 'SE02 19-25'
                WHEN dld.foir_before::DECIMAL(10,3) < 32 THEN 'SE03 25-32'
                WHEN dld.foir_before::DECIMAL(10,3) < 37 THEN 'SE04 32-37'
                WHEN dld.foir_before::DECIMAL(10,3) < 42 THEN 'SE05 37-42'
                WHEN dld.foir_before::DECIMAL(10,3) < 48 THEN 'SE06 42-48'
                WHEN dld.foir_before::DECIMAL(10,3) < 55 THEN 'SE07 48-55'
                WHEN dld.foir_before::DECIMAL(10,3) < 66 THEN 'SE08 55-66'
                ELSE 'SE09 66+'
            END
    END AS foir_before_group,
    CASE WHEN dld.foir_after IS NOT NULL AND dld.foir_after != '' THEN
        dld.foir_after::DECIMAL(10,3)
    END AS foir_after,
    CASE WHEN dld.employement_type = 'Salaried Employee' THEN
            CASE
                WHEN dld.foir_after IS NULL THEN 'SA_NA'
                WHEN dld.foir_after = '' THEN 'SA_NA'
                WHEN dld.foir_after::DECIMAL(10,3) < 40 THEN 'SA01 <40'
                WHEN dld.foir_after::DECIMAL(10,3) < 49 THEN 'SA02 40-48'
                WHEN dld.foir_after::DECIMAL(10,3) < 56 THEN 'SA03 49-55'
                WHEN dld.foir_after::DECIMAL(10,3) < 61 THEN 'SA04 56-60'
                WHEN dld.foir_after::DECIMAL(10,3) < 67 THEN 'SA05 61-66'
                WHEN dld.foir_after::DECIMAL(10,3) < 71 THEN 'SA06 67-70'
                WHEN dld.foir_after::DECIMAL(10,3) < 76 THEN 'SA07 71-75'
                WHEN dld.foir_after::DECIMAL(10,3) < 82 THEN 'SA08 76-81'
                ELSE 'SA09 81+'
            END
        ELSE
            CASE
                WHEN dld.foir_after IS NULL THEN 'SE_NA'
                WHEN dld.foir_after = '' THEN 'SE_NA'
                WHEN dld.foir_after::DECIMAL(10,3) < 34 THEN 'SE01 <34'
                WHEN dld.foir_after::DECIMAL(10,3) < 41 THEN 'SE02 34-40'
                WHEN dld.foir_after::DECIMAL(10,3) < 49 THEN 'SE03 41-48'
                WHEN dld.foir_after::DECIMAL(10,3) < 56 THEN 'SE04 49-55'
                WHEN dld.foir_after::DECIMAL(10,3) < 63 THEN 'SE05 56-62'
                WHEN dld.foir_after::DECIMAL(10,3) < 71 THEN 'SE06 63-70'
                WHEN dld.foir_after::DECIMAL(10,3) < 79 THEN 'SE07 71-78'
                WHEN dld.foir_after::DECIMAL(10,3) < 85 THEN 'SE08 79-84'
                ELSE 'SE09 85+'
            END
    END AS foir_after_group,
    '' AS crif_group,
    /*CASE
        WHEN crif_score_original IS NULL THEN 'CNF'
        WHEN crif_score_original::INT <= 550 THEN 'A01 <=550'
        WHEN crif_score_original::INT <= 575 THEN 'A02 551-575'
        WHEN crif_score_original::INT <= 600 THEN 'A03 576-600'
        WHEN crif_score_original::INT <= 625 THEN 'A04 601-625'
        WHEN crif_score_original::INT <= 650 THEN 'A05 626-650'
        WHEN crif_score_original::INT <= 675 THEN 'A06 651-675'
        WHEN crif_score_original::INT <= 700 THEN 'A07 675-700'
        WHEN crif_score_original::INT <= 725 THEN 'A08 701-725'
        WHEN crif_score_original::INT <= 750 THEN 'A09 726-750'
        WHEN crif_score_original::INT <= 775 THEN 'A10 751-775'
        WHEN crif_score_original::INT <= 800 THEN 'A11 775-800'
        WHEN crif_score_original::INT <= 850 THEN 'A12 801-850'
        ELSE 'A13 850+'
    END AS crif_group_original,*/
    DATE_DIFF('day', dld.loan_registered_date, dld.first_disburse_date) AS tat,
    CASE
        WHEN DATE_DIFF('day', dld.loan_registered_date, dld.first_disburse_date) <= 1 THEN 'A1 Same Day'
        WHEN DATE_DIFF('day', dld.loan_registered_date, dld.first_disburse_date) <= 4 THEN 'A2 2-4 Days'
        WHEN DATE_DIFF('day', dld.loan_registered_date, dld.first_disburse_date) <= 8 THEN 'A3 a Week'
        WHEN DATE_DIFF('day', dld.loan_registered_date, dld.first_disburse_date) <= 15 THEN 'A4 a Fortnight'
        WHEN DATE_DIFF('day', dld.loan_registered_date, dld.first_disburse_date) <= 31 THEN 'A5 a Month'
        WHEN DATE_DIFF('day', dld.loan_registered_date, dld.first_disburse_date) <= 60 THEN 'A6 2 Month'
        WHEN DATE_DIFF('day', dld.loan_registered_date, dld.first_disburse_date) IS NOT NULL THEN 'A7 2+ Month'
        ELSE 'NA'
    END AS tat_grp,
    DATE_DIFF('day', dld.uw_live_date::DATE, dld.first_disburse_date) AS tat_live_disburse,
    CASE
        WHEN DATE_DIFF('day', dld.uw_live_date::DATE, dld.first_disburse_date) <= 1 THEN 'A1 Same Day'
        WHEN DATE_DIFF('day', dld.uw_live_date::DATE, dld.first_disburse_date) <= 4 THEN 'A2 2-4 Days'
        WHEN DATE_DIFF('day', dld.uw_live_date::DATE, dld.first_disburse_date) <= 8 THEN 'A3 a Week'
        WHEN DATE_DIFF('day', dld.uw_live_date::DATE, dld.first_disburse_date) <= 15 THEN 'A4 a Fortnight'
        WHEN DATE_DIFF('day', dld.uw_live_date::DATE, dld.first_disburse_date) <= 31 THEN 'A5 a Month'
        WHEN DATE_DIFF('day', dld.uw_live_date::DATE, dld.first_disburse_date) <= 60 THEN 'A6 2 Month'
        WHEN DATE_DIFF('day', dld.uw_live_date::DATE, dld.first_disburse_date) IS NOT NULL THEN 'A7 2+ Month'
        ELSE 'NA'
    END AS tat_grp_live_disburse,
    DATE_DIFF('day', dld.loan_registered_date, dld.uw_live_date::DATE) AS tat_loan_live,
    CASE
        WHEN DATE_DIFF('day', dld.loan_registered_date, dld.uw_live_date::DATE) <= 1 THEN 'A1 Same Day'
        WHEN DATE_DIFF('day', dld.loan_registered_date, dld.uw_live_date::DATE) <= 4 THEN 'A2 2-4 Days'
        WHEN DATE_DIFF('day', dld.loan_registered_date, dld.uw_live_date::DATE) <= 8 THEN 'A3 a Week'
        WHEN DATE_DIFF('day', dld.loan_registered_date, dld.uw_live_date::DATE) <= 15 THEN 'A4 a Fortnight'
        WHEN DATE_DIFF('day', dld.loan_registered_date, dld.uw_live_date::DATE) <= 31 THEN 'A5 a Month'
        WHEN DATE_DIFF('day', dld.loan_registered_date, dld.uw_live_date::DATE) <= 60 THEN 'A6 2 Month'
        WHEN DATE_DIFF('day', dld.loan_registered_date, dld.uw_live_date::DATE) IS NOT NULL THEN 'A7 2+ Month'
        ELSE 'NA'
    END AS tat_grp_loan_live,
    dld.dev_exists,
    dld.total_dev,
    dld.bank_name,
    CASE WHEN dld.nach_source IS NULL THEN 'NA' ELSE dld.nach_source END AS nach_type,
    CASE WHEN dld.nach_status IS NULL THEN 'NA' ELSE dld.nach_status END AS nach_status,
    CASE WHEN dld.nach_reason IS NULL THEN 'NA' ELSE dld.nach_reason END AS nach_reason,
    CASE WHEN dld.payment_registration_method IS NULL THEN 'NA' ELSE dld.payment_registration_method END AS payment_registration_method,
    CASE WHEN cncr.loan_id IS NULL THEN 'Others' ELSE 'NACH' END AS payment_presentation_method,
    dld.cheque_total,
    dld.cheque_available,
    CASE WHEN dld.cheque_available > 0 THEN 'Yes' ELSE 'No' END AS cheque_available_flag,
    CASE WHEN dldam.first_emi_date <= status_month - 4 THEN dldam.loan_total_principal ELSE 0 END AS la_1stemipresented,
    CASE WHEN dldam.loan_total_principal = dldam.total_due_principal AND dldam.first_emi_date <= status_month - 4 THEN 'Yes' ELSE 'No' END AS non_starter_flag,
    CASE WHEN dldam.loan_total_principal = dldam.total_due_principal AND dldam.first_emi_date <= status_month - 4 THEN 1 ELSE 0 END AS non_starter,
    CASE WHEN dldam.loan_total_principal = dldam.total_due_principal AND dldam.first_emi_date <= status_month - 4 THEN dldam.total_due_principal ELSE 0 END AS non_starter_pos,
    dldam.bounce_relaxed AS bounce_mob,
    CASE WHEN dld.dev_exists = 'Y' THEN 1 ELSE 0 END AS deviation_exists,
    total_dev AS total_deviation,
    dld.loan_registered_date,
    dld.accepted_date::DATE AS accepted_date,
    dld.actual_live_date::DATE AS actual_live_date,
    dld.rfd_date::DATE AS rfd_date,
    dld.first_disburse_date,
    dld.product_type,
    dld.comp_natureofbusiness AS nature_of_business,
    dld.comp_industrytype AS industry_type,
    dld.ex360_npa,
    dldam.status_month,
    mob,
    dldam.loan_status AS loan_status_mob,
    dldam.total_due_principal AS pos_mob,
    dldam.dpdcurrent_classification AS dpd,
    CASE
        WHEN dldam.channel = 'Alliances' THEN 'Alliances'
        WHEN dldam.loan_total_principal < 25001 THEN 'Pocket Loan'
        ELSE dld.uw_name
    END AS uw,
    dld.uw_status_final AS uw_status,
    dld.call_disposition AS call_status,
    dld.ba_name AS borrower_agent,
    dld.ba_login_name_clean AS borrower_agent_name,
    dld.rm AS relation_manager,
    CASE
        WHEN dld.loan_img1 = 'APPROVED' THEN 'PQ Approved'
        WHEN dld.loan_img1 = 'APPROVED_STPQ' THEN 'STPQ Approved'
        WHEN dld.loan_img1 = 'REFER' THEN 'REFER'
        WHEN dld.loan_img1 = 'REJECTED' THEN 'REJECTED'
        WHEN dld.loan_img1 IS NOT NULL THEN 'Unknown'
        ELSE 'NA'
    END AS pq_status,
    CASE
        WHEN dld.loan_img2 = 'YES' THEN 'Yes'
        WHEN dld.loan_img2 = 'NO' THEN 'No'
        WHEN dld.loan_img2 IS NOT NULL THEN 'Unknown'
        ELSE 'NA'
    END AS offer_status,
    CASE WHEN dld.uw_returned_date IS NOT NULL THEN 1 ELSE 0 END AS retuned_flag,
    CASE WHEN dld.uw_forward_to_cs_date IS NOT NULL THEN 1 ELSE 0 END AS forwarded_flag,
    DATE_DIFF('month', dld.comp_startdate, dld.loan_registered_date) AS business_vintage,
    CASE
        WHEN DATE_DIFF('month', dld.comp_startdate, dld.loan_registered_date) <= 6 THEN 'A1 Upto 6M'
        WHEN DATE_DIFF('month', dld.comp_startdate, dld.loan_registered_date) <= 12 THEN 'A2 6-12M'
        WHEN DATE_DIFF('month', dld.comp_startdate, dld.loan_registered_date) <= 24 THEN 'A3 12-24M'
        WHEN DATE_DIFF('month', dld.comp_startdate, dld.loan_registered_date) <= 36 THEN 'A4 24-36M'
        WHEN DATE_DIFF('month', dld.comp_startdate, dld.loan_registered_date) <= 60 THEN 'A5 36-60M'
        WHEN DATE_DIFF('month', dld.comp_startdate, dld.loan_registered_date) IS NOT NULL THEN 'A6 60+ M'
        ELSE 'NA'
    END AS business_vintage_grp,
    CASE WHEN dldam.channel = 'DSA Partner' THEN
        CASE WHEN dld.user_agent_name IS NOT NULL AND dld.user_agent_name != '' THEN dld.user_agent_name END
    END AS dsa,
    dldam.mob_emi AS mob_on_1st_emi,
    pan_count,
    top_up,
    old_category,
    old_sub_category,
    old_channel,
    old_loan_last_emi_date,
    old_loan_close_date,
    top_date_diff,
    CASE WHEN dd.top_date_diff IS NOT NULL AND dd.top_date_diff > 10 THEN 1 ELSE 0 END AS double_disburse,
    CASE WHEN dldam.dpdcurrent_classification IN ('A2_1-30 DPD','A3_31-60 DPD','A4_61-90 DPD','A5_91-180 DPD','A6_180+ DPD') THEN 1 ELSE 0 END AS plus_x_count,
    CASE WHEN dldam.dpdcurrent_classification IN ('A2_1-30 DPD','A3_31-60 DPD','A4_61-90 DPD','A5_91-180 DPD','A6_180+ DPD') THEN dldam.total_due_principal ELSE 0 END AS plus_x,
    CASE WHEN dldam.dpdcurrent_classification IN ('A3_31-60 DPD','A4_61-90 DPD','A5_91-180 DPD','A6_180+ DPD') THEN 1 ELSE 0 END AS plus_30_count,
    CASE WHEN dldam.dpdcurrent_classification IN ('A3_31-60 DPD','A4_61-90 DPD','A5_91-180 DPD','A6_180+ DPD') THEN dldam.total_due_principal ELSE 0 END AS plus_30,
    CASE WHEN dldam.dpdcurrent_classification IN ('A4_61-90 DPD','A5_91-180 DPD','A6_180+ DPD') THEN 1 ELSE 0 END AS plus_60_count,
    CASE WHEN dldam.dpdcurrent_classification IN ('A4_61-90 DPD','A5_91-180 DPD','A6_180+ DPD') THEN dldam.total_due_principal ELSE 0 END AS plus_60,
    CASE WHEN dldam.dpdcurrent_classification IN ('A5_91-180 DPD','A6_180+ DPD') THEN 1 ELSE 0 END AS plus_90_count,
    CASE WHEN dldam.dpdcurrent_classification IN ('A5_91-180 DPD','A6_180+ DPD') THEN dldam.total_due_principal ELSE 0 END AS plus_90,
    dldam.due_principal_to_collect,
    CASE WHEN dldam.dpd_month_current >= 365 THEN 1 ELSE 0 END AS ex365_month
FROM tb_main dldam
LEFT JOIN tb_lead_details dld ON dldam.loan_id::decimal(38,0) = dld.loan_id::decimal(38,0)
LEFT JOIN (
    SELECT
        loan_id,
        pan_count,
        top_up,
        old_category,
        old_sub_category,
        old_channel,
        old_loan_last_emi_date,
        old_loan_close_date,
        top_date_diff
    FROM tb_top_up_details
) dd ON dd.loan_id = dldam.loan_id
LEFT JOIN tb_mandate cncr ON cncr.loan_id = dld.loan_id AND cncr.unique_reference_no = dld.mandate_id
WHERE dld.plus <> 420 AND dld.fraud_flag = 0) select *, CASE
        WHEN crif_score_original IS NULL THEN 'CNF'
        WHEN crif_score_original::INT <= 550 THEN 'A01 <=550'
        WHEN crif_score_original::INT <= 575 THEN 'A02 551-575'
        WHEN crif_score_original::INT <= 600 THEN 'A03 576-600'
        WHEN crif_score_original::INT <= 625 THEN 'A04 601-625'
        WHEN crif_score_original::INT <= 650 THEN 'A05 626-650'
        WHEN crif_score_original::INT <= 675 THEN 'A06 651-675'
        WHEN crif_score_original::INT <= 700 THEN 'A07 675-700'
        WHEN crif_score_original::INT <= 725 THEN 'A08 701-725'
        WHEN crif_score_original::INT <= 750 THEN 'A09 726-750'
        WHEN crif_score_original::INT <= 775 THEN 'A10 751-775'
        WHEN crif_score_original::INT <= 800 THEN 'A11 775-800'
        WHEN crif_score_original::INT <= 850 THEN 'A12 801-850'
        ELSE 'A13 850+'
    END AS crif_group_original from tb_with_crif;


grant select on table dm_static_pool in schema public to analytics_admin;
grant all on table dm_static_pool in schema public to  account_admin;