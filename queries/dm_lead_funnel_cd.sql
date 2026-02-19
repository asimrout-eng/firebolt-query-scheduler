drop table if exists fair.public.dm_lead_funnel_cd;
create table fair.public.dm_lead_funnel_cd as (
with tb_campaign as (
select
	los_app_id as camp_id,
	min(camp_date) as campaign_date
from (
	select
		los_app_id,
		to_date(campaign_date,'yyyy-mm-dd') as camp_date
	from
		fair.public.dm_pro_campaign_exeution
	union all 
	select
		cast(los_app_id as bigint) as los_app_id,
		--min(to_date(timestamp 'epoch' + (cast(campaign_date as bigint)-19800)* interval '1 second','yyyy-mm-dd')) as camp_date
  		MIN(TO_TIMESTAMP(campaign_date::BIGINT - 19800)::DATE) AS camp_date
	from
		fair.public.cent_rm_wise_faircent_pro_performance
	where
		deleted = 'N'
		and id >2
	group by cast(los_app_id as bigint))
group by los_app_id
)
--select to_date(case when campaign_date='0025-08-29' then '2025-08-29' else campaign_date end ,'yyyy-mm-01'),count(camp_id) from tb_campaign group by to_date(case when campaign_date='0025-08-29' then '2025-08-29' else campaign_date end ,'yyyy-mm-01')
,
tb_campaign_final as (
select
	los_app_id as los_app_id,
	applicant_name as pros_name,
	user_dob as pros_dob,
	user_age as pros_age,
	pan as pros_pan,
	mobile as pros_mobile,
	user_date as pros_registration_date,
	user_vintage as pros_vintage,
	user_pin as pros_pin,
	registration_type as pros_registration_type,
	employement_type as pros_employement_type,
	employment_sub_type as pros_employment_sub_type,
	score_date as pros_score_date,
	bureau_score as pros_score,
	score_description as pros_score_description,
	cs_score_date as pros_cs_score_date,
	cs_score as pros_cs_score,
	propensity as pros_propensity,
	campaign_date,
	case 
		when pan is not null and pan!='' then 
			row_number() over (partition by pan order by case when campaign_date is null then to_date('2099-12-31','yyyy-mm-dd') else campaign_date end) 
	end as pros_pan_rowno
from 
	fair.public.dm_pro_base tb_enq
left join tb_campaign on
	tb_enq.los_app_id = tb_campaign.camp_id
),
tb_leads as (
select
	loan_id,
	loan_registered_date as lead_registration_date,
	--to_date(loan_registered_date,'yyyy-mm-01') as lead_registration_month,
  	DATE_TRUNC('month', loan_registered_date)::DATE AS lead_registration_month,
	loan_currentstate as lead_stage,
	user_name as lead_name,
	user_mobile as lead_mobile,
	bor_mobile as lead_mobile_clean,
	user_pan as lead_pan,
	user_dob as lead_dob,
	age_loanregister as lead_registration_age,
	age_current as lead_current_age,
	user_pin as lead_pin,
	user_city as lead_city,
	user_final_city as lead_final_city,
	user_state as lead_state,
	user_final_state as lead_final_state,
	source as lead_source,
	user_agent_name as lead_dsa,
	user_agents_name as lead_sub_dsa,
	sub_category as lead_sub_category,
	category as lead_category,
	channel as lead_channel,
	rm as lead_rm,
	employement_type as lead_employement
from
	fair.public.dm_lead_details
where
	loan_registered_date >= '2024-01-01'
),
tb_bre as (
select
	bre_loan_id,
	lender_name,
	rule_date,
	bre_pass
from
	(select
		ftpl.loan_id as bre_loan_id,
		case 
			when action_type='ABFL_PL_RULE_EXECUTION' then 'ABFL_PL'
			when action_type='ABFL_RULE_EXECUTION' then 'ABFL_BL'
			when action_type='AXIS_RULE_EXECUTION' then 'AXIS_BL'
			when action_type='BAJAJ_BL_RULE_EXECUTION' then 'BAJAJ_BL'
			when action_type='DMI_RULE_EXECUTION' then 'DMI_BL'
			when action_type='FATAKPAY_RULE_EXECUTION' then 'FATAKPAY_PL'
			when action_type='FINNABLE_PL_RULE_EXECUTION' then 'FINNABLE_PL'
			when action_type='FLEXILOANS_RULE_EXECUTION' then 'FLEXI_BL'
			when action_type='IDFC_RULE_EXECUTION' then 'IDFC'
			when action_type='INCRED_RULE_EXECUTION' then 'INCRED_PL'
			when action_type='INDIFI_RULE_EXECUTION' then 'INDIFI_BL'
			when action_type='LENDINGKART_RULE_EXECUTION' then 'LK_BL'
			when action_type='MUTHOOT_BL_RULE_EXECUTION' then 'MUTHOOT_BL'
			when action_type='MUTHOOT_RULE_EXECUTION' then 'MUTHOOT_PL'
			when action_type='TATA_PL_RULE_EXECUTION' then 'TATA_PL'
			when action_type='TATA_RULE_EXECUTION' then 'TATA_BL'
			when action_type='UNITY_BIL_RULE_EXECUTION' then 'UNITY_BL'
			when action_type='UNITY_SBL_RULE_EXECUTION' then 'UNITY_SBL'
			when action_type='CREDITSAISON_RULE_EXECUTION' then 'CREDITSAISON'
		end as lender_name,
		--to_date(timestamp 'epoch' + (created)* interval '1 second', 'yyyy-mm-dd') as rule_date,
  		TO_TIMESTAMP(created)::DATE AS rule_date,
		case when final_value='1000' then 1 else 0 end as bre_pass,
		row_number () over (partition by ftpl.loan_id,action_type order by created desc) as rule_latest
	from
		fair.public.ftpl_activity_log ftpl
	inner join tb_leads on tb_leads.loan_id=ftpl.loan_id
	where
		deleted = 'N' 
--		and final_value=1000
--		and to_date(timestamp 'epoch' + (created)* interval '1 second', 'yyyy-mm-dd')>='2025-06-25'
		and action_type in ('ABFL_PL_RULE_EXECUTION','ABFL_RULE_EXECUTION','AXIS_RULE_EXECUTION','BAJAJ_BL_RULE_EXECUTION','CREDITSAISON_RULE_EXECUTION','DMI_RULE_EXECUTION','FATAKPAY_RULE_EXECUTION','FINNABLE_PL_RULE_EXECUTION','FLEXILOANS_RULE_EXECUTION','IDFC_RULE_EXECUTION','INCRED_RULE_EXECUTION','INDIFI_RULE_EXECUTION','LENDINGKART_RULE_EXECUTION','MUTHOOT_BL_RULE_EXECUTION','MUTHOOT_RULE_EXECUTION','TATA_PL_RULE_EXECUTION','TATA_RULE_EXECUTION','UNITY_BIL_RULE_EXECUTION','UNITY_SBL_RULE_EXECUTION')
	)
where rule_latest=1
),
tb_bre_product as (
select
	bre_loan_id,
	min(rule_date) as bre_date,
	sum(case when lender_name='ABFL_PL' then bre_pass else 0 end) as abfl_pl_bre_eligible,
	sum(case when lender_name='ABFL_BL' then bre_pass end) as abfl_bl_bre_eligible,
	sum(case when lender_name='AXIS_BL' then bre_pass end) as axis_bl_bre_eligible,
	sum(case when lender_name='BAJAJ_BL' then bre_pass end) as bajaj_bl_bre_eligible,
	sum(case when lender_name='CREDITSAISON' then bre_pass end) as cs_bre_eligible,
	sum(case when lender_name='DMI_BL' then bre_pass end) as dmi_bl_bre_eligible,
	sum(case when lender_name='FATAKPAY_PL' then bre_pass end) as fatakpay_pl_bre_eligible,
	sum(case when lender_name='FINNABLE_PL' then bre_pass end) as finnable_pl_bre_eligible,
	sum(case when lender_name='FLEXI_BL' then bre_pass end) as flexi_bl_bre_eligible,
	sum(case when lender_name='IDFC' then bre_pass end) as idfc_bre_eligible,
	sum(case when lender_name='INCRED_PL' then bre_pass end) as incred_pl_bre_eligible,
	sum(case when lender_name='INDIFI_BL' then bre_pass end) as indifi_bl_bre_eligible,
	sum(case when lender_name='LK_BL' then bre_pass end) as lk_bl_bre_eligible,
	sum(case when lender_name='MUTHOOT_BL' then bre_pass end) as muthoot_bl_bre_eligible,
	sum(case when lender_name='MUTHOOT_PL' then bre_pass end) as muthoot_pl_bre_eligible,
	sum(case when lender_name='TATA_PL' then bre_pass end) as tata_pl_bre_eligible,
	sum(case when lender_name='TATA_BL' then bre_pass end) as tata_bl_bre_eligible,
	sum(case when lender_name='UNITY_BL' then bre_pass end) as unity_bl_bre_eligible,
	sum(case when lender_name='UNITY_SBL' then bre_pass end) as unity_sbl_bre_eligible
from
	tb_bre
group by bre_loan_id
),
tb_accept as (
SELECT
    DISTINCT CASE WHEN LENGTH(REGEXP_REPLACE(logged_entity_id, '[.'' ]', '')) <= 10 THEN
        CAST(REGEXP_REPLACE(logged_entity_id, '[.'' ]', '') AS BIGINT)
    END AS accept_loan_id,
    TO_TIMESTAMP(created)::DATE AS accept_date
FROM
    fair.public.cent_state_log csl
INNER JOIN tb_leads ON tb_leads.loan_id = CAST(REGEXP_REPLACE(csl.logged_entity_id, '[.'' ]', '') AS BIGINT)
WHERE
    deleted = 'N'
    AND new_state = 1400
    AND LENGTH(REGEXP_REPLACE(logged_entity_id, '[.'' ]', '')) <= 10
),
tb_disburse as (
select
	flle.loan_id as disb_loan_id,
	product_tag,
	disburse_amount,
	disburse_date
from
	fair.public.ftpl_loan_lender_eligibility flle 
inner join tb_leads on tb_leads.loan_id=flle.loan_id
where
	deleted = 'N'
	and disburse_status = 'Y'
),
tb_lead_funnel as (
select
	distinct tb_leads.*,
	bre_date,
	--to_date(bre_date,'yyyy-mm-01') as bre_month,
  	DATE_TRUNC('month', bre_date)::DATE AS bre_month,
	case when accept_loan_id is not null then 1 else 0 end as accepted,
	acc_date,
	--to_date(acc_date,'yyyy-mm-01') as acc_month,
  	DATE_TRUNC('month', acc_date)::DATE AS acc_month,
	abfl_pl_bre_eligible,
	abfl_bl_bre_eligible,
	axis_bl_bre_eligible,
	bajaj_bl_bre_eligible,
	cs_bre_eligible,
	dmi_bl_bre_eligible,
	fatakpay_pl_bre_eligible,
	finnable_pl_bre_eligible,
	flexi_bl_bre_eligible,
	idfc_bre_eligible,
	incred_pl_bre_eligible,
	indifi_bl_bre_eligible,
	lk_bl_bre_eligible,
	muthoot_bl_bre_eligible,
	muthoot_pl_bre_eligible,
	tata_pl_bre_eligible,
	tata_bl_bre_eligible,
	unity_bl_bre_eligible,
	unity_sbl_bre_eligible,
	product_tag,
	disburse_amount,
	disburse_date,
	--to_date(disburse_date,'yyyy-mm-01') as disb_month
  	DATE_TRUNC('month', disburse_date)::DATE AS disb_month
from
	tb_leads
left join (select accept_loan_id,max(accept_date) as acc_date from tb_accept group by accept_loan_id) acc on acc.accept_loan_id=tb_leads.loan_id
left join tb_bre_product on tb_bre_product.bre_loan_id=tb_leads.loan_id
left join tb_disburse on tb_disburse.disb_loan_id=tb_leads.loan_id
),
tb_lead_camp_mob as (
select
	*
from
	tb_lead_funnel
	left join tb_campaign_final on tb_campaign_final.pros_mobile::text=tb_lead_funnel.lead_mobile_clean::text 
),
tb_lead_camp_pan as (
select
	*
from
	tb_lead_funnel
	left join tb_campaign_final on tb_campaign_final.pros_pan=tb_lead_funnel.lead_pan and tb_campaign_final.pros_pan_rowno =1
),
tb_lead_camp_mobile as (
select
	*,
	row_number () over (partition by loan_id order by case when campaign_date is null then to_date('2099-12-31','yyyy-mm-dd') else campaign_date end) as lead_rowno
from
	(select
		*
	from
		tb_lead_camp_mob
	union all
	select
		*
	from
		tb_lead_camp_pan
	)
),
tb_all as (
select
	*,
	case
		when DATE_DIFF('day',campaign_date,lead_registration_date)>=0 and DATE_DIFF('day',campaign_date,lead_registration_date)<=90 then 1
--			and (lead_source is null or lead_source not in ('wl')) then 1
	else 0
	end as campaign_lead,
	DATE_DIFF('day',campaign_date,lead_registration_date) as camp_lead_diff,
	IFNULL(abfl_pl_bre_eligible,0)+	IFNULL(abfl_bl_bre_eligible,0)+IFNULL(axis_bl_bre_eligible,0)+IFNULL(bajaj_bl_bre_eligible,0)+
	IFNULL(cs_bre_eligible,0)+IFNULL(dmi_bl_bre_eligible,0)+IFNULL(fatakpay_pl_bre_eligible,0)+IFNULL(finnable_pl_bre_eligible,0)+
	IFNULL(flexi_bl_bre_eligible,0)+IFNULL(idfc_bre_eligible,0)+IFNULL(incred_pl_bre_eligible,0)+IFNULL(indifi_bl_bre_eligible,0)+
	IFNULL(lk_bl_bre_eligible,0)+IFNULL(muthoot_bl_bre_eligible,0)+IFNULL(muthoot_pl_bre_eligible,0)+IFNULL(tata_pl_bre_eligible,0)+
	IFNULL(tata_bl_bre_eligible,0)+IFNULL(unity_bl_bre_eligible,0)+IFNULL(unity_sbl_bre_eligible,0) as eligible_lender,
	case when IFNULL(tata_bl_bre_eligible,0)+IFNULL(unity_bl_bre_eligible,0)+IFNULL(unity_sbl_bre_eligible,0)>0 then 1 else 0 end as bre_approved,
	case when disburse_amount>0 or disburse_date is not null then 1 else 0 end as disbursed
from
	tb_lead_camp_mobile
),
tb_final as (
select
	*
from
	tb_all
where
	1 = 1
	and lead_rowno = 1
),
tb_lead_all_mob as (
select
	tb_final.*,
	dld.user_id as uid,
	dld.loan_registered_date,
	--to_date(dld.loan_registered_date,'yyyy-mm-01') as loan_registered_month
  	DATE_TRUNC('month', dld.loan_registered_date)::DATE AS loan_registered_month
from
	tb_final
	left join fair.public.dm_lead_details dld on tb_final.lead_mobile_clean=dld.bor_mobile
),
tb_lead_all_pan as (
select
	tb_final.*,
	dld.user_id as uid,
	dld.loan_registered_date,
	--to_date(dld.loan_registered_date,'yyyy-mm-01') as loan_registered_month
  	DATE_TRUNC('month', dld.loan_registered_date)::DATE AS loan_registered_month
from
	tb_final
	left join fair.public.dm_lead_details dld on dld.user_pan=tb_final.lead_pan and dld.user_pan!='' and tb_final.lead_pan!=''
),
tb_lead_all_mobile as (
select
	*,
	row_number () over (partition by loan_id order by loan_registered_date) as loan_rowno
from
	(select
		*
	from
		tb_lead_all_mob
	union all
	select
		*
	from
		tb_lead_all_pan
	)
)
select
	*,
	DATE_DIFF('day',loan_registered_date,lead_registration_date) as loan_lead_diff
from
	tb_lead_all_mobile
where
	loan_rowno = 1

);
grant select on table dm_lead_funnel_cd in schema public to analytics_admin;
grant all on table dm_lead_funnel_cd in schema public to  account_admin;