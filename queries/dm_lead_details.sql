
drop table dm_lead_details;
create table fair.public.dm_lead_details as (
with 
tb_report as (
select CURRENT_DATE() as report_datetime
),
cnd as (
select
	id,
	cnd_name,
	cnd_code,
	cnd_group
from
	fair.public.cent_cnd
where
	deleted = 'N'),
tb_disburse as (
select
	loan_id as disburse_loanid,
	MIN(TO_TIMESTAMP(payment_instrument_date::BIGINT + 19800)::DATE) AS first_disburse_date,
    MAX(TO_TIMESTAMP(payment_instrument_date::BIGINT + 19800)::DATE) AS last_disburse_date,
	sum(instrument_amount) as disburse_amount
from
	fair.public.cent_disbursment_payment_register cdpr
where
	pay_user_id not in (8888888888, 9999999999, 7777777777, 6666666666, 5555555555, 222222221)
	and payment_status in ('Paid','Pending','pending') --,'Pending','pending'
	and deleted = 'N'
group by
	loan_id
),
tb_proposal as (
select
	distinct cp.loan_id as proposal_loanid,
	min(cpc_updated) over (partition by cp.loan_id) as first_proposal_date,
	max(cpc_updated) over (partition by cp.loan_id) as last_proposal_date,
	sum(cp_amount) over (partition by cp.loan_id) as proposal_amount,
	case
		when sum(case when cp.lender_uid = 1983451 then 1 else 0 end) over (partition by cp.loan_id)>0 then 'FD'
		when sum(case when cp.lender_uid = 4297499 then 1 else 0 end) over (partition by cp.loan_id)>0 then 'INDMoney'
		when sum(case when cp.lender_uid = 5046222 then 1 else 0 end) over (partition by cp.loan_id)>0 then 'MLP'
	else 'Non-FD'
	end as portfolio_type
from
	(
	select
		*,
		amount as cp_amount,
		TO_TIMESTAMP(created)::DATE AS cp_created,
        TO_TIMESTAMP(udated)::DATE AS cp_updated
	from
		fair.public.cent_proposal
	where
		deleted = 'N'
		and proposal_state = '13000'
		and is_collected = 'Y') cp
inner join 
	(
	select
		*,
		amount as cpc_amount,
		TO_TIMESTAMP(created)::DATE AS cpc_created,
        TO_TIMESTAMP(udated)::DATE AS cpc_updated
	from
		fair.public.cent_proposal_collection
	where
		deleted = 'N'
		and proposal_state = '14000'
		and is_declined = '0') cpc on
	CAST(cp.id AS NUMERIC(38,0))= CAST(cpc.proposal_id AS NUMERIC(38,0))
),
tb_cibil as (
select
	distinct cibil_loanid,
	case 
		when first_disburse_date is not null then
			first_value(cibil_scores) over (partition by cibil_loanid
	order by
		disburse_date_diff rows between unbounded preceding and unbounded following)
		else
		first_value(cibil_scores) over (partition by cibil_loanid
	order by
		cibil_created
			desc rows between unbounded preceding and unbounded following)
	end as cibil_score,
	case 
		when first_disburse_date is not null then
			first_value(cibil_created) over (partition by cibil_loanid
	order by
		disburse_date_diff rows between unbounded preceding and unbounded following)
		else
		first_value(cibil_created) over (partition by cibil_loanid
	order by
		cibil_created
			desc rows between unbounded preceding and unbounded following)
	end as cibil_score_date
from
	(
	select
		distinct loanid as cibil_loanid,
		case
			when cibil_score like '%0-1%' then '1'
			when cibil_score = '000' then '0'
			when cibil_score = '001' then '1'
			when cibil_score = '002' then '2'
			when cibil_score = '003' then '3'
			when cibil_score = '004' then '4'
			when cibil_score = '005' then '5'
			else cibil_score
		end as cibil_scores,
		--to_date(timestamp 'epoch' + (created)* interval '1 second') as cibil_created,
  		cibil_created,
		first_disburse_date,
		DATE_DIFF('day',
		cibil_created,
		first_disburse_date) as disburse_date_diff
	from
		(
		select
  			loanid,
  			cibil_score,
  			TO_TIMESTAMP(created)::DATE as cibil_created
  			
		from
			fair.public.cent_tu_credit_information
		where
			loanid is not null
			and cibil_score is not null
			and cibil_score <> '') ctci
	left join tb_disburse on
		tb_disburse.disburse_loanid = ctci.loanid
	order by
		cibil_loanid,
		cibil_created desc)
where
	disburse_date_diff >= 0
	or disburse_date_diff is null),
tb_crif as (
select
	distinct crif_loanid,
	case
		when first_disburse_date is not null then
			first_value(crif_scores) over (partition by crif_loanid
	order by
		disburse_date_diff rows between unbounded preceding and unbounded following)
		else
		first_value(crif_scores) over (partition by crif_loanid
	order by
		crif_created
			desc rows between unbounded preceding and unbounded following)
	end as crif_score,
	case
		when first_disburse_date is not null then
			first_value(crif_created) over (partition by crif_loanid
	order by
		disburse_date_diff rows between unbounded preceding and unbounded following)
		else
		first_value(crif_created) over (partition by crif_loanid
	order by
		crif_created
			desc rows between unbounded preceding and unbounded following)
	end as crif_score_date
from
	(
	select
		distinct loanid as crif_loanid,
		case
			when score like '%0-1%'	or score = '' then '0'
			else score
		end as crif_scores,
		--to_date(timestamp 'epoch' + (created+19800)* interval '1 second') as crif_created,
  		crif_created,
		first_disburse_date,
		DATE_DIFF('day',
		crif_created,
		first_disburse_date) as disburse_date_diff
	from
		(
		select
			loanid, score, 
  			--to_date(timestamp 'epoch' + (created+19800)* interval '1 second') as crif_created
            TO_TIMESTAMP(created + 19800)::DATE AS crif_created
		from
			fair.public.cent_crif_log_data
		where
			loanid is not null
			and score is not null
			and score <> '') ccld
	left join tb_disburse on
		tb_disburse.disburse_loanid = ccld.loanid
	order by
		crif_loanid,
		crif_created desc)
where
	disburse_date_diff >= 0
	or disburse_date_diff is null),
tb_adhaar as (
SELECT
    adh_main.loan_id AS adhaar_loanid,

    IFNULL(
        ICU_NORMALIZE(
            TRIM(REGEXP_REPLACE(name, 'null', '')),
            'Any-Title'
        ),
        ''
    ) AS adhaar_name,

    dob AS adhaar_dob,
    gender AS adhaar_gender,

    REGEXP_REPLACE(
        ICU_NORMALIZE(
            TRIM(
                IFNULL(TRIM(REGEXP_REPLACE("House_number", 'null', '')), '') || ' ' ||
                IFNULL(TRIM(REGEXP_REPLACE(locality, 'null', '')), '') || ' ' ||
                IFNULL(TRIM(REGEXP_REPLACE(vtc, 'null', '')), '') || ' ' ||
                IFNULL(TRIM(REGEXP_REPLACE(sub_district, 'null', '')), '') || ' ' ||
                IFNULL(TRIM(REGEXP_REPLACE(district, 'null', '')), '') || ' ' ||
                IFNULL(TRIM(REGEXP_REPLACE(state, 'null', '')), '')
            ),
            'Any-Title'
        ),
        '[\n|\r]+',
        ','
    ) AS adhaar_fulladdress,

    IFNULL(
        ICU_NORMALIZE(
            TRIM(REGEXP_REPLACE(state, 'null', '')),
            'Any-Title'
        ),
        ''
    ) AS adhaar_state,

    IFNULL(
        ICU_NORMALIZE(
            TRIM(REGEXP_REPLACE(district, 'null', '')),
            'Any-Title'
        ),
        ''
    ) AS adhaar_district,

    IFNULL(
        ICU_NORMALIZE(
            TRIM(REGEXP_REPLACE(sub_district, 'null', '')),
            'Any-Title'
        ),
        ''
    ) AS adhaar_sub_district,

    IFNULL(
        ICU_NORMALIZE(
            TRIM(REGEXP_REPLACE(street, 'null', '')),
            'Any-Title'
        ),
        ''
    ) AS adhaar_street,

    IFNULL(
        ICU_NORMALIZE(
            TRIM(REGEXP_REPLACE(vtc, 'null', '')),
            'Any-Title'
        ),
        ''
    ) AS adhaar_vtc,

    IFNULL(
        ICU_NORMALIZE(
            TRIM(REGEXP_REPLACE("House_number", 'null', '')),
            'Any-Title'
        ),
        ''
    ) AS adhaar_house_number,

    IFNULL(
        ICU_NORMALIZE(
            TRIM(REGEXP_REPLACE(locality, 'null', '')),
            'Any-Title'
        ),
        ''
    ) AS adhaar_locality,

    IFNULL(
        UPPER(TRIM(REGEXP_REPLACE(adhaar, 'null', ''))),
        ''
    ) AS adhaar_adhaar

FROM fair.public.cent_aadhar_address_data adh_main
inner join 
	(
	select
		distinct loan_id,
		max(id) as id
	from
		fair.public.cent_aadhar_address_data
	where
		deleted = 'N'
	group by
		loan_id) adh_max on
	adh_max.id = adh_main.id
where
	deleted = 'N'),
user_details as (
SELECT DISTINCT
    user_email.uid AS uid_user,

    ICU_NORMALIZE(
        TRIM(
            CONCAT(
                IFNULL(user_det.fname, ''),
                CONCAT(' ', IFNULL(user_det.lname, ''))
            )
        ),
        'Any-Title'
    ) AS name_user,

    user_det.mobile   AS mobile_user,
    user_det.landline AS landline_user,

    user_email.name AS namec_org_user,

    ICU_NORMALIZE(
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    LOWER(user_email.name),
                    'fc_invalid1_lockdown_invalid_lockdown_invalid_|fcinvalid_invalid-|\
|fc_invalid3_invalid_faircent_|fc_invalid_1|fc_invalid1_lockdown_invalid_|fc_invalid3_lockdown_invalid_|lockdown_invalid_fc_invalid1_|\
|fc_invalid1_invalid_faircent_|fc_invalid_lockdown_invalid_|fc_invalid3_closed_loan_|fc_invalid1_invalid_fc_|invalid_faircent_|\
|lockdown_invalid_|invalid_admin_|fc_invalid1_|fc_invalid3_|fc_invalid2_|fc_invalid_2|fc_invalid_|fc_invalid1|fc_ invalid|fcinvalid_|\
|invalid_|invalid-|fc_invalid|fc_closed_2|fc_closed_1|fc_closed_|fc_close_|fc_closed|closed_loan_|closed_|\
|@faircent.com1|@faircent.com',
                    ''
                ),
                '\\.',
                ' '
            )
        ),
        'Any-Title'
    ) AS namec_user,

    user_email.mail AS email_org_user,

    LOWER(
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    LOWER(user_email.mail),
                    'fc_invalid1_lockdown_invalid_lockdown_invalid_|fcinvalid_invalid-|\
|fc_invalid3_invalid_faircent_|fc_invalid_1|fc_invalid1_lockdown_invalid_|fc_invalid3_lockdown_invalid_|lockdown_invalid_fc_invalid1_|\
|fc_invalid1_invalid_faircent_|fc_invalid_lockdown_invalid_|fc_invalid3_closed_loan_|fc_invalid1_invalid_fc_|invalid_faircent_|\
|lockdown_invalid_|invalid_admin_|fc_invalid1_|fc_invalid3_|fc_invalid2_|fc_invalid_2|fc_invalid_|fc_invalid1|fc_ invalid|fcinvalid_|\
|invalid_|invalid-|fc_invalid|fc_closed_2|fc_closed_1|fc_closed_|fc_close_|fc_closed|closed_loan_|closed_',
                    ''
                ),
                '@faircent.com1',
                '@faircent.com'
            )
        )
    ) AS email_user

FROM fair.public.users user_email
left join fair.public.cent_user user_det on
	user_det.uid = user_email.uid
where
	user_det.deleted = 'N'
	or user_det.deleted is null),
tb_state_log as (
select
	id,
	CASE WHEN LENGTH(REGEXP_REPLACE(logged_entity_id, '\\.|''| ', '')) <= 10 THEN
		cast(regexp_replace(logged_entity_id, '[.'' ]', '') as bigint)
	end as logged_entity_id,
	logged_entity_type,
	old_state,
	new_state,
	updated_by,
	--cast(to_timestamp(timestamp 'epoch' + (created + 19800)* interval '1 second' , 'yyyy-mm-dd hh24:mi:ss') as timestamp) as created_datetime
    TO_TIMESTAMP(created + 19800)::TIMESTAMP AS created_datetime
from
	fair.public.cent_state_log
where
	deleted = 'N'
	and regexp_replace(logged_entity_id, '[.'' ]', '') != ''
	and logged_entity_type in ('cent_loan','cent_vendor_borrower_varification')),
tb_borrower_agent as (
select
	ba_loan_id,
	ba_uid,
	name_user as ba_name,
	mobile_user as ba_mobile,
	namec_org_user as ba_login_name,
	namec_user as ba_login_name_clean,
	email_org_user as ba_email_original,
	email_user as ba_name_from_email
from
	(
	select
		distinct logged_entity_id as ba_loan_id,
		old_state,
		new_state,
		updated_by as ba_uid,
		created_datetime,
		row_number () over (partition by logged_entity_id order by created_datetime desc,id desc) as row_ba
	from
		tb_state_log
	where
		logged_entity_type = 'cent_loan'
		and new_state in (1050)
		and updated_by is not null
		and updated_by not in (0, 1)
	order by ba_loan_id,row_ba
	) ba
inner join user_details on user_details.uid_user=ba.ba_uid and row_ba=1
),
tb_preuw_agent as (
select
	preuw_loan_id,
	preuw_uid,
	name_user as preuw_name,
	mobile_user as preuw_mobile,
	namec_org_user as preuw_login_name,
	namec_user as preuw_login_name_clean,
	email_org_user as preuw_email_original,
	email_user as preuw_name_from_email
from
	(
	select
		distinct logged_entity_id as preuw_loan_id,
		old_state,
		new_state,
		updated_by as preuw_uid,
		created_datetime,
		row_number () over (partition by logged_entity_id order by created_datetime desc,id desc) as row_preuw
	from
		tb_state_log
	where
		logged_entity_type = 'cent_loan'
		and ((old_state in (1050) and new_state in (1200)) or (old_state in (1200) and new_state in (1400,1100)))
		and updated_by is not null
		and updated_by not in (0, 1)
	order by preuw_loan_id,row_preuw
	) preuw
inner join user_details on user_details.uid_user=preuw.preuw_uid and row_preuw=1
),
tb_uw_csl as (
select
	distinct logged_entity_id as uw_loan_id,
	old_state,
	new_state,
	updated_by as uw_id,
	created_datetime,
	row_number () over (partition by logged_entity_id
order by
	created_datetime desc,
	id desc) as row_uw
from
	tb_state_log
where
	old_state in (1400, 1500, 1700)
	and new_state in (-1000, 1100, 1500, 1600, 2500, 2700)
	and logged_entity_type='cent_loan'
	and updated_by is not null
	and updated_by not in (0, 1)
	and updated_by in (
	select
		distinct uw_id
	from
		fair.public.dm_uw_master)
--order by
		--created_datetime,
		--id
),
tb_uw_state as (
select
	a.*,
	uw_name
from
	(
	select
		distinct uw_loan_id,
		new_state,
		first_value (uw_id) over (partition by uw_loan_id,
		new_state
	order by
		row_uw rows between unbounded preceding and unbounded following) as uw_id,
		max (created_datetime) over (partition by uw_loan_id,new_state) as uw_created
	from
		tb_uw_csl) a
left join fair.public.dm_uw_master dum on
	dum.uw_id = a.uw_id
),
tb_rule_pass as (
select
	distinct loan_id as loan_id_pass,
	created as rule_pass_date
from
	(
	select
		*,
		row_number () over (partition by loan_id,rule_status
	order by
		created) as rowno
	from
		(
		select
			distinct loan_id,
			TO_TIMESTAMP(created + 19800)::DATE as created,
			case
				when sum(case when status in ('REJECTED', 'Reject','REJECT') then 1 else 0 end) over (partition by loan_id,
				TO_TIMESTAMP(created + 19800)::DATE) >0 then 'Rejected'
				else 'Approved'
			end as rule_status
		from
			fair.public.cent_loan_auto_execution_status where deleted ='N' 
			and (action_type is null or action_type in ('Covid_3rd_wave_rule','FAIRCENT_RULE','FC_PQ_RULE_EXECUTION','FC_RULE_EXECUTION','FC_RULE_EXECUTION_STPQ','RULE_EXECUTION'))
		order by
			loan_id desc
  		))
where
	rowno = 1
	and rule_status = 'Approved'
),
tb_rule_reject as (
select
	distinct loan_id as loan_id_reject,
	created as rule_reject_date
from
	(
	select
		*,
		row_number () over (partition by loan_id,rule_status
	order by
		created) as rowno
	from
		(
		select
			distinct loan_id,
			TO_TIMESTAMP(created + 19800)::DATE as created,
			case
				when sum(case when status in ('REJECTED', 'Reject','REJECT') then 1 else 0 end) over (partition by loan_id,
				TO_TIMESTAMP(created + 19800)::DATE )>0 then 'Rejected'
				else 'Approved'
			end as rule_status
		from
			fair.public.cent_loan_auto_execution_status where deleted ='N'
			and (action_type is null or action_type in ('Covid_3rd_wave_rule','FAIRCENT_RULE','FC_PQ_RULE_EXECUTION','FC_RULE_EXECUTION','FC_RULE_EXECUTION_STPQ','RULE_EXECUTION'))
		order by
			loan_id desc
  		))
where
	rowno = 1
	and rule_status = 'Rejected'
),
tb_uwa_csl as (
select
	logged_entity_id as uwac_loan_id,
	created_datetime as uwac_created
from
	(
	select
		*,
		row_number () over (partition by logged_entity_id
	order by
		created_datetime,
		id desc) as rowno_uw
	from
		tb_state_log
	where
		new_state in (1400)
		and	logged_entity_type='cent_loan'
		order by
			created_datetime,
			id)
where
	rowno_uw = 1
),
tb_uwa_system as (
select
	a.*,
	uw_name
from
	(
	select
		uw_loan_id as uwas_loan_id,
		cast(uw_id as bigint) as uwas_id,
		uw_assigned_date as uwas_assigned_date
	from
		(
		select
			distinct
		cl.id as uw_loan_id,
			ccmcl.id as ccmcl_id,
			regexp_replace(regexp_replace(ccmcl.remark, 'agent- ', ''), ' .*', '') as uw_id,
			--to_timestamp(timestamp 'epoch' + (ccmcl.created + 19800)* interval '1 second', 'yyyy-mm-dd hh24:mi:ss') as uw_assigned_date
            TO_TIMESTAMP(ccmcl.created + 19800)::TIMESTAMP AS uw_assigned_date
		from
			fair.public.cent_cron_master_log ccml
		left join fair.public.cent_cron_master_child_log ccmcl on
			ccmcl.cron_id = ccml.cron_id
		left join fair.public.cent_loan cl on
			cl.uid::text = ccmcl.communication_id::text
		where
			ccml.deleted = 'N'
			and ccmcl.deleted = 'N'
			and ccml.cron_type in ('automatic_allocation_of_cases_to_underwriters.sh', 'automatic_allocation_of_cases_to_underwriters_new.sh')
				and ccmcl.status = 'success') uw
	inner join (
		select
			distinct
		cl1.id as loanid,
			max(ccmcl1.id) over (partition by cl1.id) as max_ccmclid
		from
			fair.public.cent_cron_master_log ccml1
		left join fair.public.cent_cron_master_child_log ccmcl1 on
			ccmcl1.cron_id = ccml1.cron_id
		left join fair.public.cent_loan cl1 on
			cl1.uid::text = ccmcl1.communication_id::text
		where
			ccml1.deleted = 'N'
			and ccmcl1.deleted = 'N'
			and ccml1.cron_type in ('automatic_allocation_of_cases_to_underwriters.sh', 'automatic_allocation_of_cases_to_underwriters_new.sh')
				and ccmcl1.status = 'success') ccmclm on
		ccmclm.max_ccmclid = uw.ccmcl_id
		and ccmclm.loanid = uw.uw_loan_id) a
left join fair.public.dm_uw_master dum on
	dum.uw_id = a.uwas_id
),
tb_uw_data as (
/*select
	distinct tb_loan.*,
	IFNULL(tb_uwa_system.uwas_id,
	tb_uw_agn.uw_id) as uw_assigned_id,
	IFNULL(tb_uwa_system.uw_name,
	tb_uw_agn.uw_name) as uw_assigned_name,
	to_timestamp(IFNULL(uwas_assigned_date, IFNULL(uwac_created, created_datetime)), 'yyyy-mm-dd hh24:mi:ss') as uw_assigned_date,
	last_day(to_date(IFNULL(uwas_assigned_date, IFNULL(uwac_created, created_datetime)), 'yyyy-mm-dd')) as uw_lastday_assigned_date,
	uw_cancelled.uw_name as uw_cancelled,
	to_timestamp(uw_cancelled.uw_created, 'yyyy-mm-dd hh24:mi:ss') as uw_cancelled_date,
	uw_returned.uw_name as uw_returned,
	to_timestamp(uw_returned.uw_created, 'yyyy-mm-dd hh24:mi:ss') as uw_returned_date,
	uw_cs.uw_name as uw_forward_to_cs,
	to_timestamp(uw_cs.uw_created, 'yyyy-mm-dd hh24:mi:ss') as uw_forward_to_cs_date,
	uw_live.uw_name as uw_live,
	to_timestamp(uw_live.uw_created, 'yyyy-mm-dd hh24:mi:ss') as uw_live_date*/

  SELECT DISTINCT tb_loan.*,
IFNULL(tb_uwa_system.uwas_id, tb_uw_agn.uw_id) AS uw_assigned_id,
IFNULL(tb_uwa_system.uw_name, tb_uw_agn.uw_name) AS uw_assigned_name,
IFNULL(uwas_assigned_date, IFNULL(uwac_created, created_datetime)) AS uw_assigned_date,
(DATE_TRUNC('month', IFNULL(uwas_assigned_date, IFNULL(uwac_created, created_datetime))::DATE) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS uw_lastday_assigned_date,
uw_cancelled.uw_name AS uw_cancelled,
uw_cancelled.uw_created AS uw_cancelled_date,
uw_returned.uw_name AS uw_returned,
uw_returned.uw_created AS uw_returned_date,
uw_cs.uw_name AS uw_forward_to_cs,
uw_cs.uw_created AS uw_forward_to_cs_date,
uw_live.uw_name AS uw_live,
uw_live.uw_created AS uw_live_date
	from
		(select id as uwm_loan_id from fair.public.cent_loan_dedup) tb_loan
	left join tb_uwa_system on
		uwas_loan_id = uwm_loan_id
	left join (
		select
			aa.*,
			dum2.uw_name
		from
			(
			select
				*
			from
				tb_uw_csl
			where
				row_uw = 1) aa
		left join fair.public.dm_uw_master dum2 on
			dum2.uw_id = aa.uw_id) tb_uw_agn on
		tb_uw_agn.uw_loan_id = uwm_loan_id
	left join tb_uwa_csl on
		tb_uwa_csl.uwac_loan_id = uwm_loan_id
	left join tb_uw_state uw_cancelled on
		uw_cancelled.uw_loan_id = uwm_loan_id
		and uw_cancelled.new_state =-1000
	left join tb_uw_state uw_returned on
		uw_returned.uw_loan_id = uwm_loan_id
		and uw_returned.new_state = 1100
	left join tb_uw_state uw_wip on
		uw_wip.uw_loan_id = uwm_loan_id
		and uw_wip.new_state = 1500
	left join tb_uw_state uw_cs on
		uw_cs.uw_loan_id = uwm_loan_id
		and uw_cs.new_state = 1600
	left join tb_uw_state uw_live on
		uw_live.uw_loan_id = uwm_loan_id
		and uw_live.new_state = 2500
	where
		IFNULL(tb_uwa_system.uwas_id, tb_uw_agn.uw_id) is not null),
tb_ops_remark as (
select
	main.loan_id as ops_loan_id,
	ops_remark,
	ops_reason
from
	(
	select
		distinct loan_id
	from
		fair.public.cent_user_additional_info
	inner join fair.public.users on
		users.uid::TEXT = created_by::TEXT
	where
		deleted = 'N'
		and action_type like 'loan_state_remark%') main
left join (
	select
		loan_id,
		--listagg(distinct value1,
		--'|') as ops_remark
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT value1), '|') AS ops_remark
	from
		fair.public.cent_user_additional_info
	where
		action_type = 'loan_state_remark'
		and deleted = 'N'
		and value1 is not null
	group by
		loan_id) remark on
		main.loan_id = remark.loan_id
left join (
	select
		loan_id,
		--listagg(distinct IFNULL(cnd_name, value1),'|') as ops_reason
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT IFNULL(cnd_name, value1)), '|') AS ops_reason
	from
		fair.public.cent_user_additional_info cuai
	left join fair.public.cent_cnd on
		cent_cnd.id::text = value1::text
	where
		action_type = 'loan_state_remark_reason'
		and cuai.deleted = 'N'
		and value1 is not null
	group by
		loan_id) reason on
		main.loan_id = reason.loan_id
order by
	main.loan_id
),
tb_dsa_rm as (
select
	distinct value1 as partner_uid,
	--initcap(first_value(regexp_replace(agent_name, '\\.', ' ')) over (partition by value1 order by Id desc rows between unbounded preceding and unbounded following)) as rm_dsa
  	ICU_NORMALIZE(
  FIRST_VALUE(
    REGEXP_REPLACE(agent_name::text, '\\.', ' ')
  )
  OVER (
    PARTITION BY value1
    ORDER BY id DESC
  ),
  'Any-Title'
) AS rm_dsa
from
	fair.public.cent_lead_assign_list clal
where
	deleted = 'N'
	and value1 is not null
	and value1 <> 0
),
tb_other_rm as (
select
	sources_name,
	IFNULL(cast(rm_other_id as bigint),0) as rm_other_id,
	IFNULL(namec_user,'') as rm_others
from
	(
	select
		distinct lower(old_source) as sources_name,
		--initcap(first_value(assigned_to) over (partition by old_source order by id desc rows between unbounded preceding and unbounded following)) as rm_other_id
        ICU_NORMALIZE(
    		FIRST_VALUE(assigned_to::text)
  			OVER (
    		PARTITION BY old_source
    		ORDER BY id DESC
    		ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  			),
  				'Any-Title'
			) AS rm_other_id
	from
		fair.public.cent_product_source_name cpsn
	where
		deleted = 'N'
		and action_type = 'BORROWER_RM_ASSIGNED'
	) rm_other
	left join user_details on rm_other.rm_other_id::text=user_details.uid_user::text
),
tb_ref as (
select
	*,
	row_number() over (partition by ref_loan_id
order by
	ref_loan_id,ref_name,ref_contact) as rowcount
from
	(
	select
			distinct loan_id as ref_loan_id,
			--initcap(IFNULL(value1, '')) as ref_name,
			--initcap(IFNULL(value2, '')) as ref_contact
  			ICU_NORMALIZE(IFNULL(value1, ''), 'Any-Title') AS ref_name,
			ICU_NORMALIZE(IFNULL(value2, ''), 'Any-Title') AS ref_contact
	from
			fair.public.cent_user_additional_info cuai2
	where
			action_type = 'REFERENCE_DETAILS'
		and deleted = 'N'
		order by ref_loan_id,ref_name,ref_contact)
),
tb_ref1 as (
select * from tb_ref where rowcount = 1
),
tb_ref2 as (
select * from tb_ref where rowcount = 2
),
tb_ref3 as (
select * from tb_ref where rowcount = 3
),
tb_ref4 as (
select * from tb_ref where rowcount = 4
),
tb_ref5 as (
select * from tb_ref where rowcount = 5
),
tb_ref6 as (
select * from tb_ref where rowcount = 6
),
tb_ref7 as (
select * from tb_ref where rowcount = 7
),
tb_ref8 as (
select * from tb_ref where rowcount = 8
),
tb_ref_contact as (
select
	distinct tb_ref.ref_loan_id,
	tb_ref1.ref_name as ref_name,
	tb_ref1.ref_contact as ref_contact,
	tb_ref2.ref_name as ref_name_1,
	tb_ref2.ref_contact as ref_contact_1,
	tb_ref3.ref_name as ref_name_2,
	tb_ref3.ref_contact as ref_contact_2,
	tb_ref4.ref_name as ref_name_3,
	tb_ref4.ref_contact as ref_contact_3,
	tb_ref5.ref_name as ref_name_4,
	tb_ref5.ref_contact as ref_contact_4,
	tb_ref6.ref_name as ref_name_5,
	tb_ref6.ref_contact as ref_contact_5,
	tb_ref7.ref_name as ref_name_6,
	tb_ref7.ref_contact as ref_contact_6,
	tb_ref8.ref_name as ref_name_7,
	tb_ref8.ref_contact as ref_contact_7
from
	tb_ref
left join tb_ref1 on
	tb_ref1.ref_loan_id = tb_ref.ref_loan_id
left join tb_ref2 on
	tb_ref2.ref_loan_id = tb_ref.ref_loan_id
left join tb_ref3 on
	tb_ref3.ref_loan_id = tb_ref.ref_loan_id
left join tb_ref4 on
	tb_ref4.ref_loan_id = tb_ref.ref_loan_id
left join tb_ref5 on
	tb_ref5.ref_loan_id = tb_ref.ref_loan_id
left join tb_ref6 on
	tb_ref6.ref_loan_id = tb_ref.ref_loan_id
left join tb_ref7 on
	tb_ref7.ref_loan_id = tb_ref.ref_loan_id
left join tb_ref8 on
	tb_ref8.ref_loan_id = tb_ref.ref_loan_id
),
tb_permanent as (
select
	*,
	row_number() over (partition by perm_loan_id
order by
	perm_loan_id,perm_address,perm_contact1,perm_contact2) as rowcount
from
	(
	select
		distinct loan_id as perm_loan_id,
		--initcap(IFNULL(value1, '')) as perm_address,
		--initcap(IFNULL(value2, '')) as perm_contact1,
		--initcap(IFNULL(value3, '')) as perm_contact2
  		ICU_NORMALIZE(IFNULL(value1, ''), 'Any-Title') AS perm_address,
        ICU_NORMALIZE(IFNULL(value2, ''), 'Any-Title') AS perm_contact1,
        ICU_NORMALIZE(IFNULL(value3, ''), 'Any-Title') AS perm_contact2
	from
		fair.public.cent_user_additional_info cuai2
	where
		action_type = 'USER_PERMANENT_INFO')
),
tb_perm_contact as (
select
	distinct tb_permanent.perm_loan_id,
	perm1.perm_address as permannent_address,
	perm1.perm_contact1 as permannent_contact,
	perm1.perm_contact2 as permannent_contact1,
	perm2.perm_address as permannent_address_1,
	perm2.perm_contact1 as permannent_contact_1,
	perm2.perm_contact2 as permannent_contact1_1
from
	tb_permanent
left join 
	(
	select
		*
	from
		tb_permanent
	where
		rowcount = 1
	) perm1 on
	perm1.perm_loan_id = tb_permanent.perm_loan_id
left join (
	select
		*
	from
		tb_permanent
	where
		rowcount = 2
	) perm2 on
	perm2.perm_loan_id = tb_permanent.perm_loan_id
),
loan_main as (
select
	distinct *,
	case
		when rate_max_approved<12 then '1.Premium'
	when rate_max_approved >= 12
		and rate_max_approved <= 14 then '2.Minimal'
		when rate_max_approved > 14
		and rate_max_approved <= 18 then '3.Low'
		when rate_max_approved > 18
		and rate_max_approved <= 22 then '4.Medium'
		when rate_max_approved > 22
		and rate_max_approved <= 26 then '5.High'
		when rate_max_approved > 26
		and rate_max_approved <= 30 then '6.Very High'
		else 'Unrated'
	end as risk_bucket,
	case 
		when loan_sub_state =1 then 'Not Contactable/Not reachable'
		when loan_sub_state =714 then 'FINAL APPROVE'
		when loan_sub_state =715 then 'FINAL REJECT'
		when loan_sub_state =15000 then 'Cooling-Off Period - Closed'
	end as loan_sub_staus
from
	(
	select
		id,
		loan_title,
		loan_amount_expected,
		tenure,
		loan_type_cnd,
		loan_state,
		loan_sub_state,
		loan_desc,
		loan_img1,
		loan_img2,
		loan_irate_expected,
		uid as uid_loan,
		max_amount_approved,
		min_irate_apporved,
		max_irate_approved,
		max_days_approved,
		verified_personal,
		verfied_professional,
		deleted as deleted_loan,
		created as created_loan,
		udated as udated_loan,
		updated_by as updatedby_loan,
		case
			when (
			select
				cnd_name
			from
				cnd
			where
			--cast(id as varchar)= regexp_substr(cl1.loan_city,
				--'[0-9]+')
				--and (cnd_group in ('LOAN_CITY', 'STATE_PIN', 'STATE_INDIA', 'CORONA_CITY')
					--or cnd_code = 'INDIA_CITY')
				--and deleted = 'N') is not null then
  			CAST(id AS TEXT) = REGEXP_EXTRACT(cl1.loan_city, '[0-9]+')
            AND (cnd_group IN ('LOAN_CITY', 'STATE_PIN', 'STATE_INDIA', 'CORONA_CITY')
            OR cnd_code = 'INDIA_CITY')
            AND deleted = 'N') IS NOT NULL THEN
			(
			select
				cnd_name
			from
				cnd
			where
				--cast(id as varchar)= regexp_substr(cl1.loan_city,
				--'[0-9]+')
				--and (cnd_group in ('LOAN_CITY', 'STATE_PIN', 'STATE_INDIA', 'CORONA_CITY')
					--or cnd_code = 'INDIA_CITY')
				--and deleted = 'N')
  			CAST(id AS TEXT) = REGEXP_EXTRACT(cl1.loan_city, '[0-9]+')
            AND (cnd_group IN ('LOAN_CITY', 'STATE_PIN', 'STATE_INDIA', 'CORONA_CITY')
            OR cnd_code = 'INDIA_CITY')
            AND deleted = 'N')
		else
			cl1.loan_city
		end as loan_city,
		verified_professional,
		percent_fund,
		editor_comment,
		document_pending,
		underwriting_pending,
		doc_verify,
		delisting,
		emiday,
		reference_id as referenceid_loan,
		risk,
		live_date,
		(
		select
			max(created_datetime)
		from
			tb_state_log
		where
			logged_entity_id::text = cast(cl1.id as varchar)
			and	logged_entity_type='cent_loan'
			and new_state in (2500)) as livec_date,
		(
		select
			max(created_datetime)
		from
			tb_state_log
		where
			logged_entity_id::text = cast(cl1.id as varchar)
			and	logged_entity_type='cent_vendor_borrower_varification'
			and new_state = 90135409) as rfd_date,
		(
		select
			max(created_datetime)
		from
			tb_state_log
		where
			logged_entity_id::text = cast(cl1.id as varchar)
			and	logged_entity_type='cent_loan'
			and new_state in (1050,1200)) as allocated_date,
		(
		select
			max(created_datetime)
		from
			tb_state_log
		where
			logged_entity_id::text = cast(cl1.id as varchar)
			and	logged_entity_type='cent_loan'
			and new_state = 1400) as accepted_date,
		original_loan_amt,
		processing_fee,
		remark,
		loan_sub_type_cnd,
		product_type,
		total_settlement_amount,
		settlement_term,
		partial_setlement_amount,
		loan_sub_status_settlement,
		broken_intrest,
		insurance_fees,
		health_insurance_fees,
		bureau_code,
		thirdparty_ref_id,
		other_loan_type_cnd,
		is_exp_pass,
		nbfc_eligible,
		product_tag,
		emi_month,
		emi_year,
		--to_date(timestamp 'epoch' + (created + 19800)* interval '1 second', 'yyyy-mm-dd') as cl_created,
		--to_date(timestamp 'epoch' + (udated + 19800)* interval '1 second', 'yyyy-mm-dd') as cl_updated,
  		TO_TIMESTAMP(created + 19800)::DATE AS cl_created,
        TO_TIMESTAMP(udated + 19800)::DATE AS cl_updated,
		--case
			--when live_date != 0 then
				--to_date(timestamp 'epoch' + (live_date + 19800)* interval '1 second', 'yyyy-mm-dd')
		--end as cl_livedate,
  		CASE
        WHEN live_date != 0 THEN
        	TO_TIMESTAMP(live_date + 19800)::DATE
        END AS cl_livedate,
		pd_value,
		pd_status
	from
		fair.public.cent_loan_dedup cl1
--	and id =1003937261
	) cl
left join 
	(
	select
		--initcap(trim(IFNULL(cnd_name, 'unknown'))) as loan_currentstate,
  		ICU_NORMALIZE(TRIM(IFNULL(cnd_name, 'unknown')), 'Any-Title') AS loan_currentstate,
		cnd_code as cndcode_lstate
	from
		cnd
	where
		lower(cnd_group) = 'loan_state'
	) lstate_cnd on
	lstate_cnd.cndcode_lstate = cast(cl.loan_state as varchar)
left join 
	(
	select
		UPPER(trim(IFNULL(cnd_name, 'unknown'))) as other_loan_type,
		id as id_ltypeother
	from
		cnd
	where
		cnd_group in ('NBFC_TYPE', 'FIP_TYPE') 
	) ltypeother_cnd on
	ltypeother_cnd.id_ltypeother = cl.other_loan_type_cnd
left join 
	(
	select
		--initcap(trim(IFNULL(cnd_name, 'unknown'))) as loan_type,
  		ICU_NORMALIZE(TRIM(IFNULL(cnd_name, 'unknown')), 'Any-Title') AS loan_type,
		id as id_ltype
	from
		cnd
	where
		cnd_group in ('CIVIL_LOAN_TYPE', 'CRIF_LOAN_TYPE', 'INDIA_LOAN', 'MAIN_LOAN_TYPE', 'PAISABAZAR_PURPOSE')
	) ltype_cnd on
	ltype_cnd.id_ltype = cl.loan_type_cnd
left join 
	(
	SELECT
    IFNULL(CAST(TRIM(REPLACE(cnd_name, '%', '')) AS DECIMAL(10, 2)), 0) AS rate_expected,
    id AS id_exprate
	FROM
    cnd
	where
		lower(cnd_group) = 'intrest_rate'
	) exprate_cnd on
	exprate_cnd.id_exprate = cl.loan_irate_expected
left join 
	(
	SELECT
    IFNULL(CAST(TRIM(REPLACE(cnd_name, '%', '')) AS DECIMAL(10, 2)), 0) AS rate_min_approved,
    id AS id_minrate
FROM
    cnd
	where
		lower(cnd_group) = 'intrest_rate'
	) minrate_cnd on
	minrate_cnd.id_minrate = cl.min_irate_apporved
left join 
	(
	SELECT
    IFNULL(CAST(TRIM(REGEXP_REPLACE(cnd_name, '%', '')) AS DECIMAL(10, 2)), 0) AS rate_max_approved,
    id AS id_maxrate
FROM
    cnd
WHERE
    LOWER(cnd_group) = 'intrest_rate'
	) maxrate_cnd on
	maxrate_cnd.id_maxrate = cl.max_irate_approved
left join 
	(
	select
		--initcap(trim(IFNULL(cnd_name, 'unknown'))) as loan_subtype,
  		ICU_NORMALIZE(TRIM(IFNULL(cnd_name, 'unknown')), 'Any-Title') as loan_subtype,
		id as id_lsubtype
	from
		cnd
	where
		cnd_group in ('MAIN_LOAN_TYPE', 'FIP_TYPE', 'INDIA_SUB_LOAN','MLP_PLAN_BORROWER')
	) lsubtype_cnd on
	lsubtype_cnd.id_lsubtype = cl.loan_sub_type_cnd
left join 
	(
	select
		name_user as name_lupdatedby,
		namec_user as namec_lupdatedby,
		mobile_user as mobile_lupdatedby,
		landline_user as landline_lupdatedby,
		email_user as email_lupdatedby,
		uid_user as uid_lupdatedby
	from
		user_details
	) lupdatedby_user on
	lupdatedby_user.uid_lupdatedby = cl.updatedby_loan
left join tb_ops_remark on
	ops_loan_id = cl.id
left join tb_crif on
	crif_loanid = cl.id
left join tb_cibil on
	cibil_loanid = cl.id
left join tb_perm_contact on
	perm_loan_id = cl.id
left join tb_ref_contact on
	tb_ref_contact.ref_loan_id = cl.id
),
user_main as (
select
	*,
CASE
    WHEN dob <= 2147463847 THEN 
        TO_TIMESTAMP(dob + 19800)::DATE
    WHEN dob > 2147463847 AND dob <= 2147483647 THEN 
        DATE '2038-01-19'
END AS cu_dob,
TO_TIMESTAMP(created + 19800)::DATE AS cu_created,
TO_TIMESTAMP(udated + 19800)::DATE AS cu_updated
from
	fair.public.cent_user cent_user
left join 
	(
	select
		uid_user as uid_agent,
		name_user as name_agent,
		namec_user as namec_agent,
		namec_org_user as namec_org_agent,
		mobile_user as mobile_agent,
		landline_user as landline_agent,
		email_user as email_agent,
		email_org_user as email_org_agent
	from
		user_details
	) agent_user
on
	agent_user.uid_agent = cent_user.agent_uid
left join 
	(
	select
		uid_user as uid_sub_agent,
		name_user as name_sub_agent,
		namec_user as namec_sub_agent,
		namec_org_user as namec_org_sub_agent,
		mobile_user as mobile_sub_agent,
		landline_user as landline_sub_agent,
		email_user as email_sub_agent,
		email_org_user as email_org_sub_agent
	from
		user_details
	) sub_agent_user
on
	sub_agent_user.uid_sub_agent = cent_user.sub_agent_uid
left join 
	(
	select
		uid_user as uid_jobassigned,
		name_user as name_jobassigned,
		namec_user as namec_jobassigned,
		mobile_user as mobile_jobassigned,
		landline_user as landline_jobassigned,
		email_user as email_jobassigned
	from
		user_details
	) job_user
on
	job_user.uid_jobassigned = cent_user.job_assigning
left join 
	(
	select
		uid_user as uid_portfolio_manager,
		name_user as name_portfolio_manager,
		namec_user as namec_portfolio_manager,
		mobile_user as mobile_portfolio_manager,
		landline_user as landline_portfolio_manager,
		email_user as email_portfolio_manager
	from
		user_details
	) portfolio_user
on
	portfolio_user.uid_portfolio_manager = cent_user.portfolio_manager
left join 
	(
	select
		uid_user as uid_user,
		email_user as email_user,
		email_org_user
	from
		user_details
	) user_user
on
	user_user.uid_user = cent_user.uid
left join 
	(
	select
		--initcap(trim(IFNULL(cnd_name, 'unknown'))) as religion,
  		ICU_NORMALIZE(TRIM(IFNULL(cnd_name, 'unknown')), 'Any-Title') as religion,
		id as id_religion
	from
		cnd
	where
		cnd_group = 'INDIA_RELIGION'
	) religion_cnd on
	religion_cnd.id_religion = cent_user.religion_cnd
left join 
	(
	select
		--initcap(trim(IFNULL(cnd_name, 'unknown'))) as caste,
  		ICU_NORMALIZE(TRIM(IFNULL(cnd_name, 'unknown')), 'Any-Title') as caste,
		id as id_caste
	from
		cnd
	where
		cnd_group like 'RELIGION%'
	) caste_cnd on
	caste_cnd.id_caste = cent_user.caste_cnd
left join 
	(
	select
		case
			when UPPER(trim(cnd_name))= upper('Andaman and Nicobar Islands') then upper('ANDAMAN & NICOBAR ISLANDS')
			when UPPER(trim(cnd_name))= upper('Chhattisgarh') then upper('Chattisgarh')
			when UPPER(trim(cnd_name))= upper('Jammu') then upper('Jammu & Kashmir')
			when UPPER(trim(cnd_name))= upper('Madhyapradesh') then upper('Madhya Pradesh')
			when UPPER(trim(cnd_name))= upper('Puducherry') then upper('Pondicherry')
			when UPPER(trim(cnd_name))= upper('Tamilnadu') then upper('Tamil Nadu')
			when UPPER(trim(cnd_name))= upper('Up') then upper('Uttar Pradesh')
			when UPPER(trim(cnd_name))= upper('Uttarpradesh') then upper('Uttar Pradesh')
			when UPPER(trim(cnd_name))= upper('Uttaranchal') then upper('Uttarakhand')
		else 
			UPPER(trim(IFNULL(cnd_name, 'unknown')))
		end as origion_state,
		id as id_origion_state
	from
		cnd
	where
		cnd_group = 'STATE_INDIA'
		or cnd_group = 'LOAN_CITY'
		or cnd_group = 'STATE_JAMMU_AND_KASHMIR'
		or cnd_group = 'STATE_DELHI'
	) origion_state_cnd on
	origion_state_cnd.id_origion_state = cent_user.origin_state_cnd
left join 
	(
	select
		case
			when UPPER(trim(cnd_name))= upper('Andaman and Nicobar Islands') then upper('ANDAMAN & NICOBAR ISLANDS')
			when UPPER(trim(cnd_name))= upper('Chhattisgarh') then upper('Chattisgarh')
			when UPPER(trim(cnd_name))= upper('Jammu') then upper('Jammu & Kashmir')
			when UPPER(trim(cnd_name))= upper('Madhyapradesh') then upper('Madhya Pradesh')
			when UPPER(trim(cnd_name))= upper('Puducherry') then upper('Pondicherry')
			when UPPER(trim(cnd_name))= upper('Tamilnadu') then upper('Tamil Nadu')
			when UPPER(trim(cnd_name))= upper('Up') then upper('Uttar Pradesh')
			when UPPER(trim(cnd_name))= upper('Uttarpradesh') then upper('Uttar Pradesh')
			when UPPER(trim(cnd_name))= upper('Uttaranchal') then upper('Uttarakhand')
		else 
			UPPER(trim(IFNULL(cnd_name, 'unknown')))
		end as input_state,
		id as id_state
	from
		cnd
	where
		cnd_group = 'STATE_INDIA'
		or cnd_group = 'LOAN_CITY'
		or cnd_group = 'STATE_JAMMU_AND_KASHMIR'
		or cnd_group = 'STATE_DELHI'
	) state_cnd on
	state_cnd.id_state = cent_user.state_cnd
left join 
	(
	select
		--initcap(trim(IFNULL(cnd_name, 'unknown'))) as mothertounge,
  		ICU_NORMALIZE(TRIM(IFNULL(cnd_name, 'unknown')), 'Any-Title') as mothertounge,
		id as id_mothertounge
	from
		cnd
	where
		cnd_group = 'INDIA_MOTHERTONGUE'
	) mothertounge_cnd on
	mothertounge_cnd.id_mothertounge = cent_user.mothertounge_cnd
),
tb_banking as MATERIALIZED (
SELECT
    cudb.id,
    cudb.loanid,
    POSITION(ICU_NORMALIZE(cudb.month, 'Any-Title') IN 'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec') / 4 + 1 AS month_bank,
    cl_created,
    REGEXP_EXTRACT(date_cnd, '[0-9]+') AS day_bank,
    CAST(TO_CHAR(cl_created, 'MM') AS INT) AS month_loan,
    CAST(
        CASE 
            WHEN POSITION(ICU_NORMALIZE(cudb.month, 'Any-Title') IN 'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec') / 4 + 1 <= CAST(TO_CHAR(cl_created, 'MM') AS INT) 
            THEN TO_CHAR(cl_created, 'YYYY') || '-' || (POSITION(ICU_NORMALIZE(cudb.month, 'Any-Title') IN 'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec') / 4 + 1) || '-' || REGEXP_EXTRACT(date_cnd, '[0-9]+')
            ELSE (CAST(TO_CHAR(cl_created, 'YYYY') AS INT) - 1) || '-' || (POSITION(ICU_NORMALIZE(cudb.month, 'Any-Title') IN 'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec') / 4 + 1) || '-' || REGEXP_EXTRACT(date_cnd, '[0-9]+')
        END 
    AS DATE) AS date_bank,
    monthly_amount
FROM
    fair.public.cent_user_detail_banking cudb
inner join loan_main on
	cudb.loanid = loan_main.id
where
	cudb.deleted = 'N'
	and action_type = 'detailed_banking_info'
	and monthly_amount is not null),
tb_banking_calc as (
SELECT
    loanid,
    DATE_TRUNC('month', date_bank)::DATE AS banking_month,
    AVG(monthly_amount) AS monthly_avg_balance,
    ROW_NUMBER() OVER (
        PARTITION BY loanid
        ORDER BY DATE_TRUNC('month', date_bank) DESC
    ) AS banking_row
FROM
    tb_banking
GROUP BY
    loanid,
    DATE_TRUNC('month', date_bank)
ORDER BY
    loanid,
    DATE_TRUNC('month', date_bank) DESC
),
loan_monthlybalance as (
select
	distinct tb_banking_calc.loanid as balance_loan_id,
	avg(monthly_avg_balance) over (partition by tb_banking_calc.loanid) as monthly_average_balance
from
	tb_banking_calc
left join tb_banking on
	tb_banking.loanid = tb_banking_calc.loanid
where
	banking_row <= 3
order by
	balance_loan_id),
loan_details as (
select
	distinct cld.loan_id as loan_detailsid,
	child,
	dependent,
	total_exp,
	sibling,
	cibil,
	equinox,
	gross_salary_m,
	take_home_salary_m,
	deductions_m,
	case
		when cld.residence_type_cnd = 1 then 'Rented'
		when cld.residence_type_cnd = 2 then 'Self Owned'
		when cld.residence_type_cnd = 3 then 'Owned By Parent Or Sibling'
		when cld.residence_type_cnd = 4 then 'Relatives'
		--else initcap(IFNULL(cnd_name, 'Unknown'))
  		else ICU_NORMALIZE(IFNULL(cnd_name, 'unknown'), 'Any-Title')
	end as residence_type,
	--to_date(timestamp 'epoch' + (cast(residence_shifting_date as bigINT)+ 19800)* interval '1 second', 'yyyy-mm-dd') as residence_shifting_date,
  	TO_TIMESTAMP(CAST(residence_shifting_date AS BIGINT) + 19800)::DATE AS residence_shifting_date,
	field1,
	dept,
	business_reg_proof,
	business_owner_proof,
	itr_gstin_proof,
	itr_ack_num,
	itr_valid_message,
	itr_valid_status,
	refer_product,
	is_nbfc_pass 
from
	fair.public.cent_loan_detail cld
left join cnd on
	cnd.id = cld.residence_type_cnd
	and cnd_group = 'RESIDENCE_TYPE'
inner join 
	(
	select
		loan_id,
		max(id) as loandet_id
	from
		fair.public.cent_loan_detail
	group by
		loan_id) loan_detailmaxid on
	cld.id = loan_detailmaxid.loandet_id
where
	cld.deleted = 'N'),
loan_employment as MATERIALIZED(
select
	distinct uid_employmentid,
	--initcap(trim(IFNULL(employement_type, 'unknown'))) as employement_type,
  	ICU_NORMALIZE(TRIM(IFNULL(employement_type, 'unknown')), 'Any-Title') AS employement_type,
	is_current,
	--initcap(trim(IFNULL(comp_designation, ''))) as comp_designation,
  	ICU_NORMALIZE(TRIM(IFNULL(comp_designation, 'unknown')), 'Any-Title') as comp_designation,
	--initcap(trim(IFNULL(comp_name, ''))) as comp_name,
  	ICU_NORMALIZE(TRIM(IFNULL(comp_name, 'unknown')), 'Any-Title') as comp_name,
	--regexp_replace(initcap(trim(IFNULL(comp_address, ''))), '[\n\r]+', ',') as comp_address,
  	REGEXP_REPLACE(
  	ICU_NORMALIZE(TRIM(IFNULL(comp_address, '')), 'Any-Title'),
  	'[\n\r]+',
  	','
		) AS comp_address,
	--initcap(trim(IFNULL(comp_city, ''))) as comp_city,
  	ICU_NORMALIZE(TRIM(IFNULL(comp_city, 'unknown')), 'Any-Title') as comp_city,
	--initcap(trim(IFNULL(comp_state, ''))) as comp_state,
  	ICU_NORMALIZE(TRIM(IFNULL(comp_state, 'unknown')), 'Any-Title') as comp_state,
	comp_tenure,
	comp_pin,
	comp_phone,
	lower(trim(IFNULL(comp_email, ''))) as comp_email,
	comp_start_date as comp_startdate,
	comp_end_date as com_enddate,
	CASE
  WHEN ICU_NORMALIZE(TRIM(IFNULL(comp_business_type, '')), 'Any-Title') = '90000116' THEN ''
  ELSE ICU_NORMALIZE(TRIM(IFNULL(comp_business_type, '')), 'Any-Title')
END AS comp_businesstype,
ICU_NORMALIZE(TRIM(IFNULL(comp_industry, '')), 'Any-Title') AS comp_industry,
company_profit AS comp_profit,
company_depreciation AS comp_depreciation,
company_salary_paid AS comp_salarypaid,
ICU_NORMALIZE(TRIM(IFNULL(company_offc_ownership, '')), 'Any-Title') AS comp_officeownership,
is_verified,
ICU_NORMALIZE(TRIM(IFNULL(designation_level, '')), 'Any-Title') AS designation_level,
ICU_NORMALIZE(TRIM(IFNULL(profession, '')), 'Any-Title') AS comp_profession,
other_salaries_paid AS other_salariespaid,
work_exp,
prev_takehome_sal AS previous_takehomesalary,
ICU_NORMALIZE(TRIM(IFNULL(business_type, '')), 'Any-Title') AS business_type,
CASE
  WHEN ICU_NORMALIZE(TRIM(IFNULL(office_type, '')), 'Any-Title') = 'Null' THEN ''
  ELSE ICU_NORMALIZE(TRIM(IFNULL(office_type, '')), 'Any-Title')
END AS comp_officetype,

no_of_employee AS comp_noofemployee,
company_turnover AS comp_turnover,
comp_registration_no AS comp_registrationno,
contact2 AS comp_contactno2,

ICU_NORMALIZE(TRIM(IFNULL(nature_of_business, '')), 'Any-Title') AS comp_natureofbusiness,

CASE
  WHEN ICU_NORMALIZE(TRIM(IFNULL(industry_type, '')), 'Any-Title') = '60' THEN 'Other'
  ELSE ICU_NORMALIZE(TRIM(IFNULL(industry_type, '')), 'Any-Title')
END AS comp_industrytype,

ICU_NORMALIZE(TRIM(IFNULL(sub_industry_type, '')), 'Any-Title') AS comp_subindustrytype,

caution_profile,
business_cash_component AS comp_cashcomponent

from
	(
	select
		employment.id,
		uid_employmentid,
		is_current,
		comp_designation,
		comp_name,
		comp_address,
		comp_tenure,
		comp_pin,
		dpm.city_pin as comp_city,
		dpm.state as comp_state,
		comp_phone,
		comp_email,
		comp_website,
		comp_start_date,
		comp_end_date,
		company_profit,
		company_depreciation,
		company_emi_loan,
		company_salary_paid,
		is_verified,
		other_salaries_paid,
		work_exp,
		prev_takehome_sal,
		business_type,
		office_type,
		no_of_employee,
		company_turnover,
		comp_registration_no,
		contact2,
		IFNULL(cndcaution.cnd_name,
		caution_profile) as caution_profile,
		business_cash_component,
		IFNULL(cndemployementtype.cnd_name,
		cast(employment_type_cnd as varchar)) as employement_type,
		IFNULL(cndbusinesstype.cnd_name,
		cast(comp_business_type_cnd as varchar)) as comp_business_type,
		IFNULL(cndindustry.cnd_name,
		cast(comp_industry_cnd as varchar)) as comp_industry,
		IFNULL(cndofficeowner.cnd_name,
		cast(company_offc_ownership_cnd as varchar)) as company_offc_ownership,
		IFNULL(cnddesignationlevel.cnd_name,
		cast(desig_level_cnd as varchar)) as designation_level,
		IFNULL(cndprofession.cnd_name,
		cast(profession_cnd as varchar)) as profession,
		IFNULL(cndbusinessnature.cnd_name,
		cast(nature_of_business as varchar)) as nature_of_business,
		IFNULL(cndindustrytype.cnd_name,
		cast(industry_type as varchar)) as industry_type,
		IFNULL(cndsubindustrytype.cnd_name,
		cast(sub_industry_type as varchar)) as sub_industry_type
	from
		(
		select
			*
		from
			(
			select
				distinct 
				id,
				uid as uid_employmentid,
				is_current,
				comp_designation,
				comp_name,
				comp_address,
				comp_tenure,
				--regexp_substr(comp_pin,
				--'[0-9]+') as comp_pin,
  				REGEXP_EXTRACT(comp_pin, '[0-9]+') AS comp_pin,
				comp_phone,
				comp_email,
				comp_website,
				--to_date(timestamp 'epoch' + (case when comp_start = '' or lower(comp_start)='nan' or lower(comp_start)='null' then null when comp_start = '6309528114600' then 1577491200 else cast(comp_start as bigint) end + 19800)* interval '1 second', 'yyyy-mm-dd') as comp_start_date,
				--to_date(timestamp 'epoch' + (case when comp_end = '' then null else cast(comp_end as bigint) end + 19800)* interval '1 second', 'yyyy-mm-dd') as comp_end_date ,
  				TO_TIMESTAMP(
   					 CASE 
        				WHEN comp_start = '' OR LOWER(comp_start) = 'nan' OR LOWER(comp_start) = 'null' THEN NULL 
        				WHEN comp_start = '6309528114600' THEN 1577491200 
        				ELSE comp_start::BIGINT 
    					END + 19800
						)::DATE AS comp_start_date,
				TO_TIMESTAMP(
    			CASE 
        		WHEN comp_end = '' THEN NULL 
        		ELSE comp_end::BIGINT 
    			END + 19800
					)::DATE AS comp_end_date,
				company_profit,
				company_depreciation,
				company_emi_loan,
				company_salary_paid,
				is_verified,
				other_salaries_paid,
				work_exp,
				prev_takehome_sal,
				business_type,
				case
					when office_type = '1' then 'Owned'
					when office_type = '0' then 'Rented'
				else office_type
				end as office_type,
				no_of_employee,
				company_turnover,
				comp_registration_no,
				contact2,
				caution_profile,
				business_cash_component,
				industry_type,
				employment_type_cnd,
				comp_business_type_cnd,
				comp_industry_cnd,
				company_offc_ownership_cnd,
				desig_level_cnd,
				profession_cnd,
				nature_of_business,
				sub_industry_type
			from
				fair.public.cent_employment
			where
				deleted = 'N') user_employment
		inner join 
			(
			select
				uid,
				max(id) as max_employmentid
			from
					fair.public.cent_employment
			where
					deleted = 'N'
			group by
				uid) b on
			user_employment.id = b.max_employmentid) employment
	left join fair.public.dm_pincode_master dpm on
		dpm.pincode::text = employment.comp_pin::text
	left join cnd cndemployementtype on
		employment.employment_type_cnd::text = cast(cndemployementtype.id as varchar)
		and cndemployementtype.cnd_group = 'EMPLOYMENT_TYPE'
	left join cnd cndbusinesstype on
		employment.comp_business_type_cnd::text = cast(cndbusinesstype.id as varchar)
		and cndbusinesstype.cnd_group = 'BUSINESS_TYPE'
	left join cnd cndindustry on
		employment.comp_industry_cnd::text = cast(cndindustry.id as varchar)
		and cndindustry.cnd_group = 'INDIA_PROFESSION'
	left join cnd cndprofession on
		employment.profession_cnd::text = cast(cndprofession.id as varchar)
		and cndprofession.cnd_group = 'INDIA_PROFESSION'
	left join cnd cndofficeowner on
		employment.company_offc_ownership_cnd::text = cast(cndofficeowner.id as varchar)
		and cndofficeowner.cnd_group = 'OFFICE_OWNERSHIP'
	left join cnd cnddesignationlevel on
		employment.desig_level_cnd::text = cast(cnddesignationlevel.id as varchar)
		and cnddesignationlevel.cnd_group = 'DESIGNATION_LEVEL'
	left join cnd cndbusinessnature on
		employment.nature_of_business = cast(cndbusinessnature.id as varchar)
		and cndbusinessnature.cnd_group = 'nature_of_business_borrower'
	left join cnd cndsubindustrytype on
		employment.sub_industry_type = cast(cndsubindustrytype.id as varchar)
		and cndsubindustrytype.cnd_group = 'business_subindustry_type_new'
	left join cnd cndindustrytype on
		employment.industry_type = cast(cndindustrytype.id as varchar)
		and cndindustrytype.cnd_group = 'business_industry_type_new'
	left join cnd cndcaution on
		employment.caution_profile = cast(cndcaution.id as varchar)
		and cndindustrytype.cnd_group = 'borrower_caution_profile'
	)
),
tb_nach AS (
    SELECT * FROM (
        SELECT
            base.*,
            FIRST_VALUE(unique_reference_no) OVER (
                PARTITION BY loan_id
                ORDER BY nach_priority DESC, unique_reference_no DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS nach_ref,
            ROW_NUMBER() OVER (
                PARTITION BY loan_id
                ORDER BY nach_priority DESC, unique_reference_no
            ) AS nach_loanentry
        FROM (
            SELECT
                DISTINCT
                loan_id,
                loan_id AS loanid_nach,
                applicant_name AS nach_applicant,
                email_id AS nach_emailid,
                mobile_no AS nach_mobile,
                bank_holder_name2 AS nach_bankholder2,
                bank_holder_name3 AS nach_bankholder3,
                bank_holder_name AS nach_bankholder,
                bank_name AS nach_bank,
                branch_name AS nach_branch,
                account_no AS nach_accountno,
                unique_reference_no,
                CASE
    WHEN account_type = '10' THEN 'Saving'
    ELSE 'Current'
END AS nach_account_type,
CASE  
    -- Specific bad data cases FIRST
    WHEN startdate = '050102017' THEN TO_DATE('05102017', 'DDMMYYYY')
    
    -- Filter out empty or invalid patterns
    WHEN REGEXP_REPLACE(startdate, '[^0-9]', '') = '' THEN NULL
    WHEN REGEXP_REPLACE(startdate, '[^0-9]', '') IS NULL THEN NULL
    WHEN REGEXP_REPLACE(startdate, '[^0-9]', '') LIKE '00%' THEN NULL  -- Invalid day 00
    
    -- Length 7 (DMMYYYY format)
    WHEN LENGTH(REGEXP_REPLACE(startdate, '[^0-9]', '')) = 7 THEN 
        CASE
            WHEN SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 3, 2) != ''
                AND SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 3, 2)::INT BETWEEN 1 AND 12
                AND SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 1, 2) != ''
                AND SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 1, 2)::INT BETWEEN 1 AND 31
            THEN TO_DATE(REGEXP_REPLACE(startdate, '[^0-9]', ''), 'DMMYYYY')
            ELSE NULL
        END
    
    -- Length 8 (DDMMYYYY format) with comprehensive validation
    WHEN LENGTH(REGEXP_REPLACE(startdate, '[^0-9]', '')) = 8 THEN 
        CASE
            -- Check for empty substrings before casting
            WHEN SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 3, 2) = '' THEN NULL
            WHEN SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 1, 2) = '' THEN NULL
            -- Basic range checks
            WHEN SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 3, 2)::INT NOT BETWEEN 1 AND 12 THEN NULL
            WHEN SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 1, 2)::INT NOT BETWEEN 1 AND 31 THEN NULL
            -- Day 31 in months with only 30 days (Feb, Apr, Jun, Sep, Nov)
            WHEN SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 1, 2)::INT = 31 
                AND SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 3, 2)::INT IN (2, 4, 6, 9, 11) THEN NULL
            -- Day 30 or 31 in February
            WHEN SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 1, 2)::INT >= 30 
                AND SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 3, 2)::INT = 2 THEN NULL
            -- Feb 29 in non-leap years
            WHEN SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 1, 2)::INT = 29 
                AND SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 3, 2)::INT = 2
                AND SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 5, 4)::INT % 4 != 0 THEN NULL
            WHEN SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 1, 2)::INT = 29 
                AND SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 3, 2)::INT = 2
                AND SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 5, 4)::INT % 100 = 0
                AND SUBSTRING(REGEXP_REPLACE(startdate, '[^0-9]', ''), 5, 4)::INT % 400 != 0 THEN NULL
            ELSE TO_DATE(REGEXP_REPLACE(startdate, '[^0-9]', ''), 'DDMMYYYY')
        END
    
    -- Length 10 (Unix epoch)
    WHEN LENGTH(REGEXP_REPLACE(startdate, '[^0-9]', '')) = 10 THEN 
        TO_TIMESTAMP(REGEXP_REPLACE(startdate, '[^0-9]', '')::BIGINT + 19800)::DATE
    
    -- Note: Removed >= 5 AND <= 8 to avoid catching invalid lengths 5, 6
    ELSE NULL
END AS start_date,
CASE  
    -- Specific bad data cases FIRST
    WHEN enddate = '3101201' THEN TO_DATE('31012021', 'DDMMYYYY')
    WHEN enddate = '3107022' THEN TO_DATE('31072022', 'DDMMYYYY')
    WHEN enddate = '3108022' THEN TO_DATE('31082022', 'DDMMYYYY')
    WHEN enddate = '3122018' THEN TO_DATE('03122018', 'DDMMYYYY')
    WHEN enddate = '3009202' THEN TO_DATE('30092022', 'DDMMYYYY')
    
    -- Filter out empty
    WHEN REGEXP_REPLACE(enddate, '-|/', '') = '' THEN NULL
    WHEN REGEXP_REPLACE(enddate, '-|/', '') IS NULL THEN NULL
    
    -- Length 7 (DMMYYYY format) - Pad to 8 digits (DDMMYYYY) since Firebolt doesn't support 'D'
    WHEN LENGTH(REGEXP_REPLACE(enddate, '-|/', '')) = 7 THEN 
        CASE
            WHEN SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 3, 2) != ''
                AND SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 3, 2)::INT BETWEEN 1 AND 12
                AND SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 1, 2) != ''
                AND SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 1, 2)::INT BETWEEN 1 AND 31
            THEN TO_DATE('0' || REGEXP_REPLACE(enddate, '-|/', ''), 'DDMMYYYY')  -- Pad with leading zero
            ELSE NULL
        END
    
    -- Length 8 (DDMMYYYY format) with comprehensive validation
    WHEN LENGTH(REGEXP_REPLACE(enddate, '-|/', '')) = 8 THEN 
        CASE
            -- Check for empty substrings before casting
            WHEN SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 3, 2) = '' THEN NULL
            WHEN SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 1, 2) = '' THEN NULL
            WHEN SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 5, 4) = '' THEN NULL
            -- Basic range checks
            WHEN SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 3, 2)::INT NOT BETWEEN 1 AND 12 THEN NULL
            WHEN SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 1, 2)::INT NOT BETWEEN 1 AND 31 THEN NULL
            -- Day 31 in months with only 30 days (Feb, Apr, Jun, Sep, Nov)
            WHEN SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 1, 2)::INT = 31 
                AND SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 3, 2)::INT IN (2, 4, 6, 9, 11) THEN NULL
            -- Day 30 or 31 in February
            WHEN SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 1, 2)::INT >= 30 
                AND SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 3, 2)::INT = 2 THEN NULL
            -- Feb 29 validation - exclude if not a leap year
            WHEN SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 1, 2)::INT = 29 
                AND SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 3, 2)::INT = 2 THEN
                CASE
                    -- Check if leap year: divisible by 4, but century years must be divisible by 400
                    WHEN SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 5, 4)::INT % 4 != 0 THEN NULL  -- Not divisible by 4
                    WHEN SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 5, 4)::INT % 100 = 0 
                        AND SUBSTRING(REGEXP_REPLACE(enddate, '-|/', ''), 5, 4)::INT % 400 != 0 THEN NULL  -- Century year not divisible by 400
                    ELSE TO_DATE(REGEXP_REPLACE(enddate, '-|/', ''), 'DDMMYYYY')
                END
            ELSE TO_DATE(REGEXP_REPLACE(enddate, '-|/', ''), 'DDMMYYYY')
        END
    
    -- Length 10 (Unix epoch)
    WHEN LENGTH(REGEXP_REPLACE(enddate, '-|/', '')) = 10 THEN 
        TO_TIMESTAMP(REGEXP_REPLACE(enddate, '-|/', '')::BIGINT + 19800)::DATE
    
    ELSE NULL
END AS end_date,
                micr AS nach_micr,
                ifsc_code AS nach_ifsccode,
                umrn AS nach_umrn,
                TO_TIMESTAMP(created + 19800)::DATE AS nach_created,
                approved_date,
                CASE
                    WHEN mandate_status = 'Final Approved' THEN ''
                    WHEN mandate_status IN ('Cancelled', 'Deactivated')
                        AND LOWER(rejected_reason) LIKE '%invalid umrn or inactive mandate%' THEN 'Invalid Umrn Or Inactive Mandate'
                    WHEN mandate_status IN ('Cancelled', 'Deactivated')
                        AND LOWER(rejected_reason) LIKE '%activat%' THEN 'Deactivation Request'
                    ELSE ICU_NORMALIZE(TRIM(IFNULL(rejected_reason, '')), 'Any-Title')
                END AS nach_reason,
                CASE
                    WHEN mandate_status IN ('Cancelled', 'Deactivated') THEN 'Deactivated'
                    ELSE ICU_NORMALIZE(TRIM(IFNULL(mandate_status, '')), 'Any-Title')
                END AS nach_status,
                CASE
                    WHEN mandate_status IN ('Cancelled', 'Deactivated') THEN 7
                    WHEN mandate_status = 'Final Approved' THEN 6
                    WHEN mandate_status = 'Final Rejected' THEN 5
                    WHEN mandate_status = 'NPCI Initial Rejected' THEN 4
                    WHEN mandate_status = 'TPSL Rejected' THEN 3
                    WHEN mandate_status = 'WIP' THEN 2
                    WHEN mandate_status = 'Pending' THEN 1
                END AS nach_priority
            FROM fair.public.cent_nach_mandate_register cnmr
            WHERE deleted = 'N'
        ) base
    ) ranked
    WHERE nach_loanentry = 1
),
tb_cheque as (
select
	distinct loan_id as loanid_cheque,
	count(cheque_no) as cheque_total,
	sum(case when cheque_status = 'Pending' then 1 else 0 end) as cheque_available,
	sum(case when cheque_status = 'Cancel' then 1 else 0 end) as cheque_cancel,
	sum(case when cheque_status in ('Paid', 'Bounce') then 1 else 0 end) as cheque_used,
	sum(case when cheque_status = 'Paid' then 1 else 0 end) as cheque_paid,
	sum(case when cheque_status = 'Bounce' then 1 else 0 end) as cheque_bounce
from
	fair.public.cent_borrower_cheque_list cbcl
where
	deleted = 'N'
group by
	loan_id),
tb_icici_emandate as (
select
	distinct loan_id as icici_emandate_loan_id,
	state as icici_mandate_state,
	mandate_id as icici_mandate_id,
	status as icici_mandate_status,
	reason as icici_mandate_reason,
	--to_date(timestamp 'epoch' + (created)* interval '1 second', 'yyyy-mm-dd') as icici_mandate_created
  	TO_TIMESTAMP(created)::DATE AS icici_mandate_created
from
	fair.public.cent_icici_mandate_register cimr
where
	deleted = 'N'
),
tb_icici_emandate_calc as (
select 
	icici_emandate_loan_id,
	icici_mandate_id,
	count(*) as icici_mandate_link_created,
	case 
		when sum(case when icici_mandate_state='Y' and icici_mandate_status='0300' then 1 else 0 end)>0 then 1 
	else 0
	end as icici_mandate_active,
	min(icici_mandate_created) as icici_mandate_first_created_date,
	case 
		when sum(case when icici_mandate_state='Y' and icici_mandate_status='0300' then 1 else 0 end) >0 then max(case when icici_mandate_state='Y' and icici_mandate_status='0300' then icici_mandate_created end) 
	end as icici_mandate_active_date
from 
	tb_icici_emandate
group by
	icici_emandate_loan_id,
	icici_mandate_id
),
tb_emandate as (
select
	distinct unique_reference_no,
	loan_id,
	cf_message,
	cf_status,
	bank_holder_name,
	account_no,
	--to_date(timestamp 'epoch' + (created)* interval '1 second', 'yyyy-mm-dd') as link_created_date
  	TO_TIMESTAMP(created)::DATE AS link_created_date
from
	fair.public.cent_cashfree_subscription
where
	deleted = 'N'
	and status = 'OK'
),
tb_emandate_cal as (
select
	unique_reference_no,
	loan_id,
	count(*) as link_created,
	case
		when sum(case when upper(cf_status) in ('ACTIVE', 'BANK_APPROVAL_PENDING', 'ERROR')
		and (lower(cf_message) in ('subscription activated successfully', 'subscription authorised successfully') or upper(cf_message) like '%SUBSCRIPTION_ALREADY_EXIST%')
		then 1 else 0 end)>= 1 then 1
		else 0
	end as active_subscription,
	min(link_created_date) as first_link_created_date,
	max(case when upper(cf_status) in ('ACTIVE', 'BANK_APPROVAL_PENDING', 'ERROR')
		and (lower(cf_message) in ('subscription activated successfully', 'subscription authorised successfully') or upper(cf_message) like '%SUBSCRIPTION_ALREADY_EXIST%')
		then link_created_date end) as active_subscription_date
from 
	tb_emandate
group by
	unique_reference_no,
	loan_id
),
tb_nach_emandate as (
select
	nach_ref as mandate_id,
	loanid_nach as mandate_loan_id,
	nach_bankholder,
	nach_emailid,
	nach_mobile,
	nach_bank,
	nach_branch,
	nach_accountno,
	nach_account_type,
	nach_ifsccode,
	nach_umrn,
	nach_created,
	start_date as nach_start_date,
	end_date as nach_end_date,
	approved_date,
	nach_reason,
	nach_status,
	case when (link_created is not null and link_created >0) or (icici_mandate_link_created is not null and icici_mandate_link_created >0) then 'E-Mandate' else 'Nach'
	end as nach_source,
	link_created as mandate_link_created,
	first_link_created_date,
	active_subscription_date,
	active_subscription as mandate_active,
	icici_mandate_link_created,
	icici_mandate_active,
	icici_mandate_first_created_date,
	icici_mandate_active_date,
	case when nach_status = 'Final Approved' then
		case when (active_subscription = 1 or icici_mandate_active=1) then 'E-Mandate Activated'
		else 'Nach Activated'
		end
	else 
		case when (active_subscription = 1 or icici_mandate_active=1) then 'E-Mandate Authorised'
		else 'No E-Mandate & Nach'
		end
	end as mandate_status
from
	tb_nach
left join tb_emandate_cal on 
	tb_emandate_cal.loan_id = tb_nach.loanid_nach
	and tb_nach.nach_ref = tb_emandate_cal.unique_reference_no
left join tb_icici_emandate_calc on
	tb_icici_emandate_calc.icici_emandate_loan_id=tb_nach.loanid_nach
	and tb_nach.nach_ref=tb_icici_emandate_calc.icici_mandate_id
),
tb_account as (
select
	*
from
	(
	select
		distinct uid as account_uid,
		account_name,
		account_number,
		upper(account_ifsc_number) as account_ifsc,
		case
			when account_type in ('10', 'Fixed', 'Salaried', 'Saving', 'saving') then 'Saving'
			when account_type in ('11', '13', 'Current', 'current', 'Over_draft') then 'Current'
			else ''
		end as account_type,
		branch_add,
		bank_name,
		micr_number,
		--to_timestamp (timestamp 'epoch' + (created)* interval '1 second',
		--'yyyy-mm-dd hh24:mi:ss') as create1,
		--to_timestamp (timestamp 'epoch' + (udated)* interval '1 second',
		--'yyyy-mm-dd hh24:mi:ss') as udate,
  		TO_TIMESTAMP(created) AS create1,
		TO_TIMESTAMP(udated) AS udate,
		row_number () over (partition by uid
	order by
		created desc) as account_row
	from
		fair.public.cent_bank_details cbd
	where
		deleted = 'N'
		and uid in (
		select
			distinct uid
		from
			fair.public.cent_loan
		where
			deleted = 'N'))
where
	account_row = 1
),
tb_deviation as MATERIALIZED(
select
	distinct loan_id as dev_loan_id,
	first_value (dev_created_by) over (partition by loan_id order by dev_created desc rows between unbounded preceding and unbounded following) as dev_created_by,
	case when count(*) over (partition by loan_id)>0 then 'Y' else 'N' end as dev_exists,
	count(*) over (partition by loan_id) as total_dev,
	sum (case when Short_description='AGE' then 1 else 0 end) over (partition by loan_id) as age_dev,
	sum (case when Short_description='AQB' then 1 else 0 end) over (partition by loan_id) as aqb_dev,
	sum (case when Short_description='Bureau Score' then 1 else 0 end) over (partition by loan_id) as bureau_score_dev,
	sum (case when Short_description='Bureau Vintage' then 1 else 0 end) over (partition by loan_id) as bureau_vintage_dev,
	sum (case when Short_description='Caution Profile' then 1 else 0 end) over (partition by loan_id) as caution_profile_dev,
	sum (case when Short_description='Docs - Income' then 1 else 0 end) over (partition by loan_id) as docs_income_dev,
	sum (case when Short_description='Enquiry' then 1 else 0 end) over (partition by loan_id) as enquiry_dev,
	sum (case when Short_description='FOIR' then 1 else 0 end) over (partition by loan_id) as foir_dev,
	sum (case when Short_description='Higher LA' then 1 else 0 end) over (partition by loan_id) as higher_la_dev,
	sum (case when Short_description='Income Mode' then 1 else 0 end) over (partition by loan_id) as income_mode_dev,
	sum (case when Short_description='Profile' then 1 else 0 end) over (partition by loan_id) as profile_dev,
	sum (case when Short_description='RCO' then 1 else 0 end) over (partition by loan_id) as rco_dev,
	sum (case when Short_description='ROI Waiver' then 1 else 0 end) over (partition by loan_id) as roi_waiver_dev,
	sum (case when Short_description='Stability' then 1 else 0 end) over (partition by loan_id) as statbility_dev,
	sum (case when Short_description='Stressed Sector' then 1 else 0 end) over (partition by loan_id) as stressed_sector_dev
from
	(
		select
			distinct loan_id,
			value1 as deviation_id,
			case 
				when value1 in ('90135088', '90157880') then 'AGE'
				when value1 in ('90135094', '90157877') then 'AQB'
				when value1 in ('90135098', '90157878') then 'Bureau Score'
				when value1 in ('90157879') then 'Bureau Vintage'
				when value1 in ('90157883') then 'Caution Profile'
				when value1 in ('90135090', '90157881') then 'Docs - Income'
				when value1 in ('90135300') then 'Enquiry'
				when value1 in ('90135089', '90135091', '90135093', '90157873', '90157874', '90157875') then 'FOIR'
				when value1 in ('90157876') then 'Higher LA'
				when value1 in ('90135096') then 'Income Mode'
				when value1 in ('90135095', '90157882') then 'Profile'
				when value1 in ('90135092', '90135097') then 'RCO'
				when value1 in ('90157885') then 'ROI Waiver'
				when value1 in ('90135099') then 'Stability'
				when value1 in ('90157884') then 'Stressed Sector'
			end as Short_description,
			--to_date(timestamp 'epoch' + (created)* interval '1 second', 'yyyy-mm-dd') as dev_created,
  			TO_TIMESTAMP(created)::DATE AS dev_created,
			created_by,
			namec_user as dev_created_by
		from
			fair.public.cent_user_additional_info cuai
		left join user_details on uid_user::text =created_by::text
		where
			deleted = 'N'
			and action_type in ('borrower_deviation', 'borrower_deviation_new'))
),
loan_finalresult as (
select
	distinct *
from
	loan_main
inner join user_main on
	loan_main.uid_loan = user_main.uid
left join 
	(
	select
		user_main.uid as uid_reference,
		--initcap(trim(concat(IFNULL(fname, ''), concat(' ', IFNULL(lname, ''))))) as name_reference,
		--initcap(trim(IFNULL(fname, ''))) as first_name_reference,
  		ICU_NORMALIZE(
  		TRIM(CONCAT(IFNULL(fname, ''), CONCAT(' ', IFNULL(lname, '')))),
  		'Any-Title'
		) AS name_reference,
		ICU_NORMALIZE(TRIM(IFNULL(fname, '')), 'Any-Title') AS first_name_reference,
		mobile as mobile_reference,
		landline as landline_reference,
		email_user as email_reference
	from
		user_main
	) reference_user on
	reference_user.uid_reference = loan_main.referenceid_loan
left join (
	select
		ICU_NORMALIZE(channel_source, 'Any-Title') AS channel_source,
		channel,
		sub_category,
		category,
		ROW_NUMBER() OVER (
			PARTITION BY LOWER(ICU_NORMALIZE(COALESCE(channel_source, ''), 'Any-Title'))
			ORDER BY 
				CASE WHEN channel IS NOT NULL AND channel != '' THEN 0 ELSE 1 END,
				CASE WHEN sub_category IS NOT NULL AND sub_category != '' THEN 0 ELSE 1 END,
				CASE WHEN category IS NOT NULL AND category != '' THEN 0 ELSE 1 END,
				channel,
				sub_category,
				category
		) as rn
	from
		fair.public.dm_channel_master) dcm on
	LOWER(dcm.channel_source) = LOWER(COALESCE(NULLIF(TRIM(user_main.source), ''), ''))
	AND dcm.rn = 1
	AND COALESCE(NULLIF(TRIM(user_main.source), ''), '') != ''
left join fair.public.dm_pincode_master dpm on
	dpm.pincode = user_main.pin
left join 
	(
	select
		tenure_code,
		tenure_months,
		tenure as tenure_monthsno
	from
		fair.public.dm_tenure_master) dtm on
	dtm.tenure_code = loan_main.tenure
left join loan_details on
	loan_details.loan_detailsid = loan_main.id
left join loan_employment on
	loan_employment.uid_employmentid = user_main.uid
left join tb_adhaar on
	tb_adhaar.adhaar_loanid = loan_main.id
left join tb_disburse on
	tb_disburse.disburse_loanid = loan_main.id
left join loan_monthlybalance on
	loan_main.id = balance_loan_id
left join tb_nach_emandate on 
	loan_main.id=mandate_loan_id
left join tb_cheque on 
	loan_main.id=loanid_cheque
left join tb_account on 
	loan_main.uid_loan=tb_account.account_uid
left join tb_deviation on 
	loan_main.id=dev_loan_id
left join tb_uw_data on
	uwm_loan_id = loan_main.id
left join tb_proposal on 
	tb_proposal.proposal_loanid= loan_main.id
),
tb_lead as MATERIALIZED (
select
	distinct id as loan_id,
	--initcap(IFNULL(loan_type, 'Others')) as loan_type,
	--initcap(IFNULL(loan_subtype, 'Others')) as loan_subtype,
	--initcap(IFNULL(other_loan_type, 'Others')) as loan_othertype,
	Upper(IFNULL(product_type, 'Others')) as product_type,
  	ICU_NORMALIZE(IFNULL(loan_type, 'Others'), 'Any-Title')       AS loan_type,
	ICU_NORMALIZE(IFNULL(loan_subtype, 'Others'), 'Any-Title')    AS loan_subtype,
	ICU_NORMALIZE(IFNULL(other_loan_type, 'Others'), 'Any-Title') AS loan_othertype,
	case
		when loan_state != 10 then loan_currentstate
	else 'Cancelled'
	end as loan_currentstate,
	loan_sub_staus,
	case 
		when portfolio_type is null and (source!='IMB' or source is null) and cl_created>='2020-09-01' then 'FD' 
		when portfolio_type is null and source='IMB' and cl_created>='2020-09-01' then 'INDMoney' 
		when portfolio_type is null and cl_created<'2020-09-01' then 'Non-FD'
	else
		portfolio_type
	end as portfolio_type, 
	loan_desc,
	uid_loan as user_id,
	--case when rule_pass_date>=created_loan and (rule_pass_date<=livec_date or livec_date is null) then 1 end as rule_pass,
	--case when rule_pass_date::text>=created_loan::text and (rule_pass_date<=livec_date or livec_date is null) then rule_pass_date end as rule_pass_date,
	--case when rule_reject_date::text>=created_loan::text and (rule_reject_date<=livec_date or livec_date is null) then 1 end as rule_reject,
	--case when rule_reject_date::text>=created_loan::text and (rule_reject_date<=livec_date or livec_date is null) then rule_reject_date end as rule_reject_date,
  CASE
  WHEN rule_pass_date >= TO_TIMESTAMP(created_loan + 19800)::DATE
   AND (rule_pass_date <= livec_date OR livec_date IS NULL)
  THEN 1
END AS rule_pass,

CASE
  WHEN rule_pass_date >= TO_TIMESTAMP(created_loan + 19800)::DATE
   AND (rule_pass_date <= livec_date OR livec_date IS NULL)
  THEN rule_pass_date
END AS rule_pass_date,

CASE
  WHEN rule_reject_date >= TO_TIMESTAMP(created_loan + 19800)::DATE
   AND (rule_reject_date <= livec_date OR livec_date IS NULL)
  THEN 1
END AS rule_reject,

CASE
  WHEN rule_reject_date >= TO_TIMESTAMP(created_loan + 19800)::DATE
   AND (rule_reject_date <= livec_date OR livec_date IS NULL)
  THEN rule_reject_date
END AS rule_reject_date,
	cl_livedate as live_date,
	allocated_date,
	accepted_date,
	livec_date as actual_live_date,
	rfd_date,
	first_proposal_date,
	--to_date(first_proposal_date,'yyyy-mm') as first_proposal_month,
  	DATE_TRUNC('month', first_proposal_date)::DATE AS first_proposal_month,
	last_proposal_date,
	--to_date(last_proposal_date,'yyyy-mm') as last_proposal_month,
  	DATE_TRUNC('month', last_proposal_date)::DATE AS last_proposal_month,
	proposal_amount,
	IFNULL(first_disburse_date,first_proposal_date) as first_disburse_date,
	IFNULL(DATE_TRUNC('month', first_disburse_date)::DATE, DATE_TRUNC('month', first_proposal_date)::DATE) AS first_disburse_month,
    IFNULL(last_disburse_date, last_proposal_date) AS last_disburse_date,
  	IFNULL(DATE_TRUNC('month', last_disburse_date)::DATE, DATE_TRUNC('month', last_proposal_date)::DATE) AS last_disburse_month,
	--IFNULL(disburse_amount::numeric(38,0),proposal_amount::numeric(38,0)) as disburse_amount,
  	COALESCE(
  CAST(disburse_amount AS NUMERIC(38, 2)),
  CAST(proposal_amount AS NUMERIC(38, 2))
) AS disburse_amount,
	tenure_months,
	tenure_monthsno,
	loan_amount_expected,
	rate_expected,
	rate_min_approved,
	rate_max_approved,
	max_amount_approved,
	risk_bucket,
	cl_created as loan_registered_date,
	cl_updated as loan_updated_date,
	namec_lupdatedby as loan_updatedby_name,
	mobile_lupdatedby as loan_updatedby_mobile,
	landline_lupdatedby as loan_updatedby_landline,
	email_lupdatedby as loan_updatedby_email,
	uw_assigned_name as uw_name,
	uw_assigned_date,
	uw_cancelled,
	uw_cancelled_date,
	uw_returned,
	uw_returned_date,
	uw_forward_to_cs,
	uw_forward_to_cs_date,
	uw_live,
	uw_live_date,
	ops_remark,
	ops_reason,
	case
		when loan_city = '90047195' then 'Bangalore'
	else
		--initcap(trim(IFNULL(loan_city, '')))
  		ICU_NORMALIZE(TRIM(IFNULL(loan_city, '')), 'Any-Title')
	end as loan_city,
	verified_personal,
	verified_professional,
	percent_fund,
	editor_comment,
	document_pending,
	underwriting_pending,
	doc_verify,
	delisting,
	risk,
	original_loan_amt as loan_original_amount,
	processing_fee,
	remark,
	total_settlement_amount,
	settlement_term,
	product_tag,
	--initcap(trim(concat(IFNULL(fname, ''), concat(' ', IFNULL(lname, ''))))) as user_name,
  	ICU_NORMALIZE(
  TRIM(CONCAT(IFNULL(fname, ''), CONCAT(' ', IFNULL(lname, '')))),
  'Any-Title'
) AS user_name,
	fname as user_fname,
	lname as user_lname,
	pan,
	--regexp_substr(UPPER(TRIM(right(trim(regexp_replace(lower(pan), '-individdual|-individual|fc_invalid_fc_invalid_|fc_reapply_fc_reapply_|fc_invalid_fc_inalid_|fc_invalid_invalid _|fc_invalid_invalid_|lockdown_invalid_|invalid_faircent_|fc_invalid_valid_|fc_closedtopup_|fc_invalid_1_|fc_invalid__|closed_loan_|_ invalid no|fc_closed_1_|fc_closed_5_|fc_invalid1_|fc_invalid_|fc_invalid1|fc_closed1_|_invalid no|_individual|fc_closed2_|fc_reapply_|in_valid _|fc_closed_|_duplicate|fc_invaid_|in_valid_|_invallid|fc_close_|invalid_|_invalid|top_up_|invali_|closed_|inalid_|topup_|_inv|top_|fc_|invalid|invild|-invld|invailid|_test|test', '')), 10))),
	--'[A-Z][A-Z][A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9][A-Z]') as user_pan,
  	REGEXP_EXTRACT(
  UPPER(
    TRIM(
      SUBSTRING(
        TRIM(REGEXP_REPLACE(
          LOWER(pan),
          '-individdual|-individual|fc_invalid_fc_invalid_|fc_reapply_fc_reapply_|fc_invalid_fc_inalid_|fc_invalid_invalid _|fc_invalid_invalid_|lockdown_invalid_|invalid_faircent_|fc_invalid_valid_|fc_closedtopup_|fc_invalid_1_|fc_invalid__|closed_loan_|_ invalid no|fc_closed_1_|fc_closed_5_|fc_invalid1_|fc_invalid_|fc_invalid1|fc_closed1_|_invalid no|_individual|fc_closed2_|fc_reapply_|in_valid _|fc_closed_|_duplicate|fc_invaid_|in_valid_|_invallid|fc_close_|invalid_|_invalid|top_up_|invali_|closed_|inalid_|topup_|_inv|top_|fc_|invalid|invild|-invld|invailid|_test|test',
          ''
        )),
        LENGTH(TRIM(REGEXP_REPLACE(
          LOWER(pan),
          '-individdual|-individual|fc_invalid_fc_invalid_|fc_reapply_fc_reapply_|fc_invalid_fc_inalid_|fc_invalid_invalid _|fc_invalid_invalid_|lockdown_invalid_|invalid_faircent_|fc_invalid_valid_|fc_closedtopup_|fc_invalid_1_|fc_invalid__|closed_loan_|_ invalid no|fc_closed_1_|fc_closed_5_|fc_invalid1_|fc_invalid_|fc_invalid1|fc_closed1_|_invalid no|_individual|fc_closed2_|fc_reapply_|in_valid _|fc_closed_|_duplicate|fc_invaid_|in_valid_|_invallid|fc_close_|invalid_|_invalid|top_up_|invali_|closed_|inalid_|topup_|_inv|top_|fc_|invalid|invild|-invld|invailid|_test|test',
          ''
        ))) - 10 + 1,
        10
      )
    )
  ),
  '[A-Z]{5}[0-9]{4}[A-Z]'
) AS user_pan,
	cibil_score,
	crif_score,
	crif_score_date,
	case when bureau_code not in ('null','','NaN') and bureau_code is not null then cast(bureau_code as INT) end as bureau_score,
	cu_dob as user_dob,
	round(DATE_DIFF('day', cu_dob, cl_created)/ 365, 1) as age_loanregister,
	round(DATE_DIFF('day', cu_dob, current_date)/ 365, 1) as age_current,
	case
		when round(DATE_DIFF('day', cu_dob, cl_created)/ 365, 1) <= 22 then 'Generation Z( upto 22)'
		when round(DATE_DIFF('day', cu_dob, cl_created)/ 365, 1)>22
			and round(DATE_DIFF('day', cu_dob, cl_created)/ 365, 1) <= 38 then 'Millennials(22-38)'
		when round(DATE_DIFF('day', cu_dob, cl_created)/ 365, 1)>38
			and round(DATE_DIFF('day', cu_dob, cl_created)/ 365, 1) <= 54 then 'Generation X(39-54)'
		when round(DATE_DIFF('day', cu_dob, cl_created)/ 365, 1)>54
			and round(DATE_DIFF('day', cu_dob, cl_created)/ 365, 1) <= 73 then 'Boomers(55-73)'
		when round(DATE_DIFF('day', cu_dob, cl_created)/ 365, 1) is null then 'Unknown'
	end as generations,
	--regexp_replace(regexp_replace(initcap(trim(IFNULL(address, '')|| ' ' || IFNULL(add, '')|| ' ' || IFNULL(add2, '')|| ' ' || IFNULL(landmark, ''))), '[\n|\r]+', ','),'  +',' ') as user_fulladdress,
  	REGEXP_REPLACE(
  REGEXP_REPLACE(
    ICU_NORMALIZE(
      TRIM(
        IFNULL(address, '') || ' ' ||
        IFNULL(add, '') || ' ' ||
        IFNULL(add2, '') || ' ' ||
        IFNULL(landmark, '')
      ),
      'Any-Title'
    ),
    '[\n|\r]+',
    ','
  ),
  '  +',
  ' '
) AS user_fulladdress,
	address as user_address,
	add as user_address1,
	add2 as user_address2,
	landmark as user_landmark,
	pin as user_pin,
	IFNULL(city_pin,
	'') as user_city,
	IFNULL(district,
	'') as user_district,
	IFNULL(IFNULL(state,
	origion_state),'') as user_state,
	IFNULL(zone,
	'') as user_zone,
	city as user_input_city,
	case
		when city_pin is null then city
		else city_pin
	end as user_final_city,
	input_state as user_input_state,
	case
		when state is null then IFNULL(input_state,origion_state)
	else state
	end as user_final_state,
	origion_state as user_origion_state,
	mobile as user_mobile,
	CASE
  WHEN LENGTH(
         SUBSTRING(
           REGEXP_REPLACE(
             REGEXP_REPLACE(
               REGEXP_REPLACE(mobile, '\\+91', ''),
               '[^0-9]',
               ''
             ),
             '^0+',
             ''
           ),
           LENGTH(
             REGEXP_REPLACE(
               REGEXP_REPLACE(
                 REGEXP_REPLACE(mobile, '\\+91', ''),
                 '[^0-9]',
                 ''
               ),
               '^0+',
               ''
             )
           ) - 10 + 1,
           10
         )
       ) = 10
   AND SUBSTRING(
         SUBSTRING(
           REGEXP_REPLACE(
             REGEXP_REPLACE(
               REGEXP_REPLACE(mobile, '\\+91', ''),
               '[^0-9]',
               ''
             ),
             '^0+',
             ''
           ),
           LENGTH(
             REGEXP_REPLACE(
               REGEXP_REPLACE(
                 REGEXP_REPLACE(mobile, '\\+91', ''),
                 '[^0-9]',
                 ''
               ),
               '^0+',
               ''
             )
           ) - 10 + 1,
           10
         ),
         1,
         1
       ) IN ('6','7','8','9')
  THEN SUBSTRING(
         REGEXP_REPLACE(
           REGEXP_REPLACE(
             REGEXP_REPLACE(mobile, '\\+91', ''),
             '[^0-9]',
             ''
           ),
           '^0+',
           ''
         ),
         LENGTH(
           REGEXP_REPLACE(
             REGEXP_REPLACE(
               REGEXP_REPLACE(mobile, '\\+91', ''),
               '[^0-9]',
               ''
             ),
             '^0+',
             ''
           )
         ) - 10 + 1,
         10
       )
END AS bor_mobile,

landline AS user_landline,

CASE
  WHEN LENGTH(
         SUBSTRING(
           REGEXP_REPLACE(
             REGEXP_REPLACE(
               REGEXP_REPLACE(landline, '\\+91', ''),
               '[^0-9]',
               ''
             ),
             '^0+',
             ''
           ),
           LENGTH(
             REGEXP_REPLACE(
               REGEXP_REPLACE(
                 REGEXP_REPLACE(landline, '\\+91', ''),
                 '[^0-9]',
                 ''
               ),
               '^0+',
               ''
             )
           ) - 10 + 1,
           10
         )
       ) = 10
   AND SUBSTRING(
         SUBSTRING(
           REGEXP_REPLACE(
             REGEXP_REPLACE(
               REGEXP_REPLACE(landline, '\\+91', ''),
               '[^0-9]',
               ''
             ),
             '^0+',
             ''
           ),
           LENGTH(
             REGEXP_REPLACE(
               REGEXP_REPLACE(
                 REGEXP_REPLACE(landline, '\\+91', ''),
                 '[^0-9]',
                 ''
               ),
               '^0+',
               ''
             )
           ) - 10 + 1,
           10
         ),
         1,
         1
       ) IN ('6','7','8','9')
  THEN SUBSTRING(
         REGEXP_REPLACE(
           REGEXP_REPLACE(
             REGEXP_REPLACE(landline, '\\+91', ''),
             '[^0-9]',
             ''
           ),
           '^0+',
           ''
         ),
         LENGTH(
           REGEXP_REPLACE(
             REGEXP_REPLACE(
               REGEXP_REPLACE(landline, '\\+91', ''),
               '[^0-9]',
               ''
             ),
             '^0+',
             ''
           )
         ) - 10 + 1,
         10
       )
END AS bor_landline,
	email_user as user_email,
	email_org_user,
	cu_created as user_created_date,
	cu_updated as user_updated_date,
	namec_jobassigned as user_jobassigned_name,
	mobile_jobassigned as user_jobassigned_mobile,
	landline_jobassigned as user_jobassigned_landline,
	email_jobassigned as user_jobassigned_email,
	agent_uid,
	case 
		when sub_category ='Fintranxect' then 'Fcpartner_Fintranxect_Digital'
		when sub_category ='FinnovIndia' then 'Fcpartner_Finnov_India_Development_Center_Pvt_Ltd'
		when sub_category ='Smrll Finance' then 'Fcpartner_Smrll'
		when sub_category ='Freemi' then 'Fcpartner_Ntactus_Financial'
		when sub_category ='Atlanta Group' then 'Fcpartner_Atlanta_Group'
		when sub_category ='Abhay' then 'Fcpartner_Abhayinfosys'
		when sub_category ='Bikash' then 'Fcpartner_Bikash_Pal'
		when sub_category ='Amit_Hooda' then 'Fcpartner_Rokdaa'
		when sub_category ='Destinyservices' then 'Fcpartner_Destinyentiresolution'
		when sub_category ='Easyloan' then 'Fcpartner_Easyloancentre In'
		when sub_category ='Financialpatron' then 'Fcpartner_Patron_Financial'
		when sub_category ='Kkgroup' then 'Fcpartner_Kk_Group_India'
		when sub_category ='Kps Ghiri' then 'Fcpartner_Kpsghiri'
		when sub_category ='Moneycircle' then 'Fcpartner_Moneycircle'
		when sub_category ='Gurufinserv' then 'Fcpartner_Guru Finance Nanded'
		when sub_category =	'Rajput Finance' then 'Fcpartner_Rajput_Finance'
		when sub_category ='Singhfinserv' then 'Fcpartner_Singh Finance85'
		when sub_category ='Gopalk' then 'Fcpartner_Gopalkadam007'
		when sub_category ='Viinome Advisors Pvt. Ltd.' then 'Fcpartner_Viinome'
		when sub_category ='Finlead Financial Advisory' then 'Fcpartner_Finlead_Financial_Advisory'
		when sub_category ='Creditenable' then 'Fcpartner_Oktober6'
		when sub_category ='Kriti Financial Services' then 'Fcpartner_Kriti'
		when sub_category in ('Afinoz','Paisagrowth','Moneyloji','Onecode','Lending Adda','Way2Online','Ls','Navigant','Bookmyfinance') then sub_category
		else namec_agent
	end as user_agent_name,
	namec_org_agent as user_org_agents_name,
	name_agent as user_agents_name,
	mobile_agent as user_agent_mobile,
	landline_agent as user_agent_landline,
	email_agent as user_agent_email,
	email_org_agent as user_org_agent_email,
	sub_agent_uid,
	namec_sub_agent as user_sub_agent_name,
	namec_org_sub_agent as user_org_sub_agent_name,
	name_sub_agent as user_sub_agents_name,
	mobile_sub_agent as user_sub_agent_mobile,
	landline_sub_agent as user_sub_agent_landline,
	email_sub_agent as user_sub_agent_email,	
	email_org_sub_agent as user_org_sub_agent_email,	
	portfolio_manager,
	namec_portfolio_manager as user_portfoliomanager_name,
	mobile_portfolio_manager as user_portfoliomanager_mobile,
	landline_portfolio_manager as user_portfoliomanager_landline,
	email_portfolio_manager as user_portfoliomanager_email,
	religion as user_religion,
	caste as user_caste,
	mothertounge as user_mothertounge,
	spouse,
	father,
	mother,
	aadhaar_number,
	marital_status,
	IFNULL(gender,'Unknown') as gender,
	reg_step,
	source,
	/*case 
		when user_agent_name='Fcpartner_Openbank' then 'Aggregator'
		when user_agent_name='Fcpartner_Earnwealth_Solutions' then 'Aggregator'
		when user_agent_name='Fcpartner_Dtpl' then 'Aggregator'
		when user_agent_name='Fcpartner_Parvatiraut' then 'DSA Partner'
		when user_agent_name is not null and (channel in ('Marketing') or channel is null) then 'DSA Partner'
		when product_type='IMB' then 'Aggregator'
		else channel
	end as channel,
	case 
		when user_agent_name='Fcpartner_Openbank' then 'Fcpartner_Openbank'
		when user_agent_name='Fcpartner_Earnwealth_Solutions' then 'Earnwealth'
		when user_agent_name='Fcpartner_Dtpl' then 'Droom'
		when user_agent_name='Fcpartner_Parvatiraut' then 'Fcpartner_Parvatiraut'
		when user_agent_name is not null and (channel in ('DSA Partner', 'Marketing') or channel is null) then user_agent_name
		when product_type='IMB' then 'IndMoney'
		else sub_category
	end as sub_category,
	case 
		when user_agent_name='Fcpartner_Openbank' then 'Aggregator'
		when user_agent_name='Fcpartner_Earnwealth_Solutions' then 'Aggregator'
		when user_agent_name='Fcpartner_Dtpl' then 'Aggregator-Two Wheeler'
		when user_agent_name='Fcpartner_Parvatiraut' then 'DSA'
		when user_agent_name is not null and (channel in ('Marketing') or channel is null) then 'DSA'
		when product_type='IMB' then 'Aggregator'
		else category
	end as category,*/
	old_source,
	mobile_verify,
	case
		when highest_education in ('0') then 'No graduation'
		when highest_education in ('320', '321', '991') then 'Undergraduation'
		when highest_education in ('', '1', '322', '992', 'others') then 'Graduation'
		when highest_education in ('323', '990', '993') then 'Post graduation'
		when highest_education in ('994') then 'Professional'
	else 'Unknown'
	end as highest_education,
	registration_type,
	source_type,
	(
	select
		IFNULL(value2,value1)
	from
		fair.public.cent_user_additional_info
	where
		lower(action_type) like '%loan_eligibility_fixed_obligation%'
		and loan_id = loan_finalresult.id
		and deleted = 'N'
	order by
		id desc
		limit 1) as total_obligation,
	(
	select
		IFNULL(value1,value2)
	from
		fair.public.cent_user_additional_info
	where
		lower(action_type) like '%loan_eligibility_fixed_obligation%'
		and loan_id = loan_finalresult.id
		and deleted = 'N'
	order by
		id desc
		limit 1) as obligation,
	(
	select
		value3
	from
		fair.public.cent_user_additional_info
	where
		lower(action_type) like '%loan_eligibility_fixed_obligation%'
		and loan_id = loan_finalresult.id
		and deleted = 'N'
	order by
		id desc
		limit 1) as savings,
	(
	select
		IFNULL(value3,
		value1)
	from
		fair.public.cent_user_additional_info
	where
		lower(action_type) like '%loan_eligibility_monthly_income%'
		and loan_id = loan_finalresult.id
		and deleted = 'N'
	order by
		id desc
		limit 1) as Income,
	(
	select
		value1
	from
		fair.public.cent_user_additional_info
	where
		lower(action_type) like '%loan_eligibility_debt_service_ratio%'
		and loan_id = loan_finalresult.id
		and deleted = 'N'
	order by
		id desc
		limit 1) as foir_before,
	(
	select
		value2
	from
		fair.public.cent_user_additional_info
	where
		lower(action_type) like '%loan_eligibility_debt_service_ratio%'
		and loan_id = loan_finalresult.id
		and deleted = 'N'
	order by
		id desc
		limit 1) as foir_after,
	monthly_average_balance,
	gst,
	relation_type,
	campaign_id,
	is_pan_verified,
	child,
	dependent,
	total_exp,
	sibling,
	cibil,
	equinox,
	gross_salary_m,
	take_home_salary_m,
	deductions_m,
	IFNULL(residence_type,
	'Unknown') as residence_type,
	field1 as user_devicename,
	dept,
	business_reg_proof,
	business_owner_proof,
	itr_gstin_proof,
	itr_ack_num,
	itr_valid_message,
	itr_valid_status,
	refer_product,
	is_nbfc_pass,
	IFNULL(employement_type,'Unknown') as employement_type,
	is_current as is_current_employement,
	comp_designation,
	comp_name,
	comp_address,
	comp_city,
	comp_state,
	comp_tenure,
	comp_pin,
	comp_phone,
	comp_email,
	comp_startdate,
	com_enddate,
	comp_businesstype,
	comp_industry,
	comp_profit,
	comp_depreciation,
	comp_salarypaid,
	comp_officeownership,
	is_verified,
	designation_level,
	comp_profession,
	other_salariespaid,
	work_exp,
	previous_takehomesalary,
	business_type,
	comp_officetype,
	comp_noofemployee,
	comp_turnover,
	comp_registrationno,
	comp_contactno2,
	comp_natureofbusiness,
	comp_industrytype,
	comp_subindustrytype,
	caution_profile,
	comp_cashcomponent,
	adhaar_adhaar,
	adhaar_name,
	adhaar_dob,
	adhaar_gender,
	adhaar_fulladdress,
	adhaar_house_number,
	adhaar_street,
	adhaar_locality,
	adhaar_vtc,
	adhaar_sub_district,
	adhaar_district,
	adhaar_state,
	referenceid_loan,
	name_reference as loan_main_borrower_name,
	first_name_reference as loan_main_borrower_first_name,
	mobile_reference as loan_main_borrower_mobile,
	landline_reference as loan_main_borrower_landline,
	email_reference as loan_main_borrower_email,
	loan_img1,
	loan_img2,
	pd_value,
	pd_status,
	permannent_address,
	permannent_contact,
	permannent_contact1,
	permannent_address_1,
	permannent_contact_1,
	permannent_contact1_1,
	ref_name,
	ref_contact,
	ref_name_1,
	ref_contact_1,
	ref_name_2,
	ref_contact_2,
	ref_name_3,
	ref_contact_3,
	ref_name_4,
	ref_contact_4,
	ref_name_5,
	ref_contact_5,
	ref_name_6,
	ref_contact_6,
	ref_name_7,
	ref_contact_7,
	IFNULL(plus,0) as plus,
	"minus" as call_stat,
	case
		when fraud_id is not null then 1
	else 0
	end as fraud_flag,
	case
		when ignore_id is not null then 1
	else 0
	end as ignore_flag,
	case
		when npa_id is not null then 1
		else 0
	end as ex360_npa,
	case
		when loan_currentstate = 'Foreclosure Initiated' then foreclosure_created
	end as foreclosure_initiated_date,
	cancelled_created as cancelled_date,
	case when cancelled_created>=livec_date and (loan_currentstate ='Cancelled' or loan_state=10) then cancelled_created end as delist_date,
	mandate_id,
	nach_bankholder,
	nach_emailid,
	nach_mobile,
	nach_bank,
	nach_branch,
	nach_accountno,
	nach_account_type,
	nach_ifsccode,
	nach_umrn,
	nach_created,
	nach_start_date,
	nach_end_date,
	approved_date as enach_approved_date,
	nach_reason,
	nach_status,
	nach_source,
	mandate_link_created,
	first_link_created_date as enach_first_link_created_date,
	active_subscription_date as enach_active_subscription_date,
	mandate_active,
	icici_mandate_link_created,
	icici_mandate_active,
	icici_mandate_first_created_date,
	icici_mandate_active_date,
	mandate_status,
	CASE
  WHEN nach_status = 'Final Approved'
   AND (
     (nach_end_date IS NOT NULL
      AND nach_end_date >= DATE_ADD(
            'day',
            -1,
            DATE_ADD('month', 1, DATE_TRUNC('month', CURRENT_DATE))
          )
     )
     OR nach_end_date IS NULL
   )
  THEN mandate_status

  WHEN cheque_available > 1 THEN 'PDC'
  ELSE 'Online'
END AS payment_registration_method,
	cheque_total,
	cheque_available,
	cheque_cancel,
	cheque_used,
	cheque_paid,
	cheque_bounce,
	dev_created_by,
	dev_exists,
	total_dev,
	age_dev,
	aqb_dev,
	bureau_score_dev,
	bureau_vintage_dev,
	caution_profile_dev,
	docs_income_dev,
	enquiry_dev,
	foir_dev,
	higher_la_dev,
	income_mode_dev,
	profile_dev,
	rco_dev,
	roi_waiver_dev,
	statbility_dev,
	stressed_sector_dev,
	account_name,
	account_number,
	account_ifsc,
	account_type,
	branch_add,
	bank_name,
	micr_number,
	parent_uid
from
	loan_finalresult
left join 
	(
	select
		distinct loan_id as fraud_id
	from
		fair.public.cent_ignore_bureau_reporting cibr
	where
		deleted = 'N' and remark is null) fraud on
	fraud_id = id
left join 
	(
	select
		distinct loan_id as ignore_id
	from
		fair.public.cent_ignore_bureau_reporting cibr
	where
		deleted = 'N' ) ignor on
	ignore_id = id
left join 
	(
	select
		distinct id as npa_id
	from
		fair.public.cent_loan cnl
	where
		deleted = 'N' and settlement_term=1) npa on
	npa_id = id
left join 
	(
	select
		logged_entity_id as foreclosure_id,
		max(created_datetime) as foreclosure_created
	from
		tb_state_log
	where
		new_state = 7500
		and	logged_entity_type='cent_loan'
	group by
		logged_entity_id
	) foreclosure on 
	foreclosure_id=id
left join 
	(
	select
		logged_entity_id as cancelled_id,
		max(created_datetime) as cancelled_created
	from
		tb_state_log
	where
		new_state in (-1000,10)
		and	logged_entity_type='cent_loan'
	group by
		logged_entity_id
	) cancelled on 
	cancelled_id=id
left join tb_rule_pass on loan_id_pass=id
left join tb_rule_reject on loan_id_reject=id
),
tb_lead_case AS (
  SELECT
    tl.*,

    CASE
      WHEN user_agent_name='Fcpartner_Openbank' THEN 'Aggregator'
      WHEN user_agent_name='Fcpartner_Earnwealth_Solutions' THEN 'Aggregator'
      WHEN user_agent_name='Fcpartner_Dtpl' THEN 'Aggregator'
      WHEN user_agent_name='Fcpartner_Parvatiraut' THEN 'DSA Partner'
      WHEN user_agent_name IS NOT NULL AND (dcm.channel IN ('Marketing') OR dcm.channel IS NULL) THEN 'DSA Partner'
      WHEN product_type='IMB' THEN 'Aggregator'
      ELSE dcm.channel
    END AS channel,

    CASE
      WHEN user_agent_name='Fcpartner_Openbank' THEN 'Fcpartner_Openbank'
      WHEN user_agent_name='Fcpartner_Earnwealth_Solutions' THEN 'Earnwealth'
      WHEN user_agent_name='Fcpartner_Dtpl' THEN 'Droom'
      WHEN user_agent_name='Fcpartner_Parvatiraut' THEN 'Fcpartner_Parvatiraut'
      WHEN user_agent_name IS NOT NULL AND (dcm.channel IN ('DSA Partner','Marketing') OR dcm.channel IS NULL) THEN user_agent_name
      WHEN product_type='IMB' THEN 'IndMoney'
      ELSE dcm.sub_category
    END AS sub_category,

    CASE
      WHEN user_agent_name='Fcpartner_Openbank' THEN 'Aggregator'
      WHEN user_agent_name='Fcpartner_Earnwealth_Solutions' THEN 'Aggregator'
      WHEN user_agent_name='Fcpartner_Dtpl' THEN 'Aggregator-Two Wheeler'
      WHEN user_agent_name='Fcpartner_Parvatiraut' THEN 'DSA'
      WHEN user_agent_name IS NOT NULL AND (dcm.channel IN ('Marketing') OR dcm.channel IS NULL) THEN 'DSA'
      WHEN product_type='IMB' THEN 'Aggregator'
      ELSE dcm.category
    END AS category,

    CASE
      WHEN icici_mandate_active=1 AND payment_registration_method NOT IN ('PDC','Online') THEN 'E-MANDATE thru ICICI'
      WHEN mandate_active=1 AND payment_registration_method NOT IN ('PDC','Online') THEN 'E-MANDATE thru CASHFREE'
      WHEN payment_registration_method NOT IN ('PDC','Online') THEN 'MANDATE - Manual'
      WHEN payment_registration_method='PDC' THEN 'Reliable Data Services'
      ELSE 'Not Known'
    END AS payment_registration_vendor

  FROM tb_lead tl
  LEFT JOIN (
    SELECT 
      ICU_NORMALIZE(channel_source, 'Any-Title') AS channel_source,
      channel,
      sub_category,
      category,
      ROW_NUMBER() OVER (
        PARTITION BY LOWER(ICU_NORMALIZE(COALESCE(channel_source, ''), 'Any-Title'))
        ORDER BY 
          CASE WHEN channel IS NOT NULL AND channel != '' THEN 0 ELSE 1 END,
          CASE WHEN sub_category IS NOT NULL AND sub_category != '' THEN 0 ELSE 1 END,
          CASE WHEN category IS NOT NULL AND category != '' THEN 0 ELSE 1 END,
          channel,
          sub_category,
          category
      ) as rn
    FROM fair.public.dm_channel_master
  ) dcm ON LOWER(dcm.channel_source) = LOWER(COALESCE(NULLIF(TRIM(tl.source), ''), ''))
    AND dcm.rn = 1
    AND COALESCE(NULLIF(TRIM(tl.source), ''), '') != ''
),
tb_lead_result as MATERIALIZED(
select tb_lead_case.*,
	case 
		when disp.cnd_name is null and plus in (0,30) then ''
		when plus =301 then 'Not Contactable/Not Reachable'
		when plus =302 then 'Not Contactable/Not Picking'
		when plus =303 then 'Not Contactable/Switched off'
		when plus =424 then 'Not_Reachable'
		when plus =425 then 'Ringing'
		when plus =75 then 'Lender made live'
		when plus =210 then 'Paid/Follow-up for docs'
		when plus =211 then 'Unpaid/Follow-up for docs'
		when plus =311 then 'Agreement Initiated -Lender'
		when plus =312 then 'Agreement Completed -Lender'
		else disp.cnd_name
	end as call_disposition,
	case
		when call_stat = 0 then 'Open'
		when call_stat = 1 then 'Valid'
		when call_stat = 2 then 'Invalid'
	end as call_status,
	ct.registration_fees,
	ct.processing_fees,
	case when product_type in ('NST','POCKETLY','POCKET_LOAN','IMB') then 'UW Not Assigned' else
		case when uw_cancelled_date is null and uw_returned_date is null and uw_forward_to_cs_date is null and uw_live_date is null then 
			case when uw_assigned_date is null then 'UW Not Assigned' else uw_name end
		else
		case 
			when uw_returned_date is not null and uw_returned_date>= uw_assigned_date 
				and (uw_forward_to_cs_date is null or uw_forward_to_cs_date<uw_returned_date)
				and (uw_cancelled_date is null or uw_cancelled_date<uw_returned_date)
				and (uw_live_date is null or uw_live_date<uw_returned_date)
				and (rfd_date is null or rfd_date<uw_returned_date)	then uw_returned
			when uw_forward_to_cs_date is not null and uw_forward_to_cs_date>= uw_assigned_date 
				and (uw_returned_date is null or uw_returned_date<uw_forward_to_cs_date)
				and (uw_cancelled_date is null or uw_cancelled_date<uw_forward_to_cs_date)
				and (uw_live_date is null or uw_live_date<uw_forward_to_cs_date)
				and (rfd_date is null or rfd_date<uw_forward_to_cs_date) then uw_forward_to_cs
			when uw_cancelled_date is not null and uw_cancelled_date>= uw_assigned_date 
				and (uw_returned_date is null or uw_returned_date<uw_cancelled_date)
				and (uw_forward_to_cs_date is null or uw_forward_to_cs_date<uw_cancelled_date)
				and (uw_live_date is null or uw_live_date<uw_cancelled_date)
				and (rfd_date is null or rfd_date<uw_cancelled_date) then uw_cancelled
			when uw_live_date is not null and uw_live_date>= uw_assigned_date 
				and (uw_returned_date is null or uw_returned_date<uw_live_date)
				and (uw_cancelled_date is null or uw_cancelled_date<uw_live_date)
				and (uw_forward_to_cs_date is null or uw_forward_to_cs_date<uw_live_date) then uw_live
		else
			case when uw_assigned_date is null then 'UW Not Assigned' else uw_name end
		end 
	end end as uw_name_final,
	case when product_type in ('NST','POCKETLY','POCKET_LOAN','IMB') then 'UW Not Assigned' else
		case when uw_cancelled_date is null and uw_returned_date is null and uw_forward_to_cs_date is null and uw_live_date is null then 
			case when uw_assigned_date is null then 'UW Not Assigned' else 'Assigned' end
		else
		case 
			when uw_returned_date is not null and uw_returned_date>= uw_assigned_date 
				and (uw_forward_to_cs_date is null or uw_forward_to_cs_date<uw_returned_date)
				and (uw_cancelled_date is null or uw_cancelled_date<uw_returned_date)
				and (uw_live_date is null or uw_live_date<uw_returned_date)
				and (rfd_date is null or rfd_date<uw_returned_date)	then 'Returned'
			when uw_forward_to_cs_date is not null and uw_forward_to_cs_date>= uw_assigned_date 
				and (uw_returned_date is null or uw_returned_date<uw_forward_to_cs_date)
				and (uw_cancelled_date is null or uw_cancelled_date<uw_forward_to_cs_date)
				and (uw_live_date is null or uw_live_date<uw_forward_to_cs_date)
				and (rfd_date is null or rfd_date<uw_forward_to_cs_date) then 'Forward to CS'
			when uw_cancelled_date is not null and uw_cancelled_date>= uw_assigned_date 
				and (uw_returned_date is null or uw_returned_date<uw_cancelled_date)
				and (uw_forward_to_cs_date is null or uw_forward_to_cs_date<uw_cancelled_date)
				and (uw_live_date is null or uw_live_date<uw_cancelled_date)
				and (rfd_date is null or rfd_date<uw_cancelled_date) then 'Cancelled'
			when (uw_live_date is not null and uw_live_date>= uw_assigned_date 
				and (uw_returned_date is null or uw_returned_date<uw_live_date)
				and (uw_cancelled_date is null or uw_cancelled_date<uw_live_date)
				and (uw_forward_to_cs_date is null or uw_forward_to_cs_date<uw_live_date)) or (rfd_date is not null and (rfd_date>=uw_assigned_date or first_disburse_date is not null)) then 'Live'
			when delist_date is not null and delist_date >=uw_assigned_date and loan_currentstate='Cancelled' and uw_live_date is not null then 'Cancelled'
		else
			case when uw_assigned_date is null then 'UW Not Assigned' else 'Assigned' end
		end 
	end end as uw_status_final,
	case when product_type not in ('NST','POCKETLY','POCKET_LOAN','IMB') then 
		case when uw_cancelled_date is null and uw_returned_date is null and uw_forward_to_cs_date is null and uw_live_date is null then 
			case when uw_assigned_date is not null then uw_assigned_date end
		else
		case 
			when uw_returned_date is not null and uw_returned_date>= uw_assigned_date 
				and (uw_forward_to_cs_date is null or uw_forward_to_cs_date<uw_returned_date)
				and (uw_cancelled_date is null or uw_cancelled_date<uw_returned_date)
				and (uw_live_date is null or uw_live_date<uw_returned_date)
				and (rfd_date is null or rfd_date<uw_returned_date)	then uw_returned_date
			when uw_forward_to_cs_date is not null and uw_forward_to_cs_date>= uw_assigned_date 
				and (uw_returned_date is null or uw_returned_date<uw_forward_to_cs_date)
				and (uw_cancelled_date is null or uw_cancelled_date<uw_forward_to_cs_date)
				and (uw_live_date is null or uw_live_date<uw_forward_to_cs_date)
				and (rfd_date is null or rfd_date<uw_forward_to_cs_date) then uw_forward_to_cs_date
			when uw_cancelled_date is not null and uw_cancelled_date>= uw_assigned_date 
				and (uw_returned_date is null or uw_returned_date<uw_cancelled_date)
				and (uw_forward_to_cs_date is null or uw_forward_to_cs_date<uw_cancelled_date)
				and (uw_live_date is null or uw_live_date<uw_cancelled_date)
				and (rfd_date is null or rfd_date<uw_cancelled_date) then uw_cancelled_date
			when (uw_live_date is not null and uw_live_date>= uw_assigned_date 
				and (uw_returned_date is null or uw_returned_date<uw_live_date)
				and (uw_cancelled_date is null or uw_cancelled_date<uw_live_date)
				and (uw_forward_to_cs_date is null or uw_forward_to_cs_date<uw_live_date)) then uw_live_date
			when rfd_date is not null and (rfd_date>=uw_assigned_date or first_disburse_date is not null) then rfd_date
			when delist_date is not null and delist_date >=uw_assigned_date and loan_currentstate='Cancelled' and uw_live_date is not null then delist_date
		else
			case when uw_assigned_date is not null then uw_assigned_date end
		end 
	end end as uw_updated_date,
	case 
		when first_disburse_date is not null then 1
		when loan_img1 in ('REFER','APPROVED','APPROVED_STPQ') then 1
		when rule_pass_date is not null then 1
		when rule_reject_date is not null then 1
	else 0
	end as rule_executed_flag,
	case 
		when first_disburse_date is not null then 1
		when loan_img1 in ('REFER','APPROVED','APPROVED_STPQ') then 1
		when rule_pass_date is not null then
			case when rule_reject_date is null then 1
			else
				case when rule_reject_date <=rule_pass_date then 1 else 0 end
			end
		when rule_reject_date is not null then 0
	end as pq_approved_flag,
	case
		when first_disburse_date is not null and greatest(rule_pass_date,rule_reject_date) is null then first_disburse_date
		when first_disburse_date is not null and first_disburse_date<=greatest(rule_pass_date,rule_reject_date) then first_disburse_date
		when first_disburse_date is not null and first_disburse_date>greatest(rule_pass_date,rule_reject_date) then greatest(rule_pass_date,rule_reject_date)
		when loan_img1 in ('REFER','APPROVED','APPROVED_STPQ') then greatest(rule_pass_date,rule_reject_date)
		when rule_pass_date is not null then
			case when rule_reject_date is null then rule_pass_date
			else
				case when rule_reject_date <=rule_pass_date then rule_pass_date end
			end
	end as pq_approved_date,	
	--case when uw_name_final!='UW Not Assigned' then 1 else 0 end as uw_allocated_flag,
	--case when uw_status_final='Cancelled' then 1 else 0 end as uw_cancelled_flag,
	--case when uw_status_final='Returned' then 1 else 0 end as uw_returned_flag,
	--case when uw_status_final='Forward to CS' then 1 else 0 end as uw_forward_to_cs_flag,
	--case when uw_status_final='Live' then 1 else 0 end as uw_live_flag,
	case when first_disburse_date is not null then 1 else 0 end as disbursed_flag,
	--case when delist_date is not null and delist_date>uw_updated_date and loan_currentstate='Cancelled' and uw_status_final='Live' then 1 else 0 end as delist_flag,
	case 
		when rm_sub.partner_uid is not null then rm_sub.rm_dsa
		when rm_main.partner_uid is not null then rm_main.rm_dsa
		when rm_others is not null then rm_others
		when tb_lead_case.sub_category in ('Fcpartner_Oktober6','Rupeezo') then 'Komal Soni'
		when tb_lead_case.sub_category in ('Fcpartner_Finnov_India_Development_Center_Pvt_Ltd') then 'Hemant Likhitkar'
		when tb_lead_case.sub_category in ('One Infinity Arc') then 'Nazir Ansari'
		when tb_lead_case.channel in ('Marketing') then 'Call_Center'
		when tb_lead_case.channel in ('Topup-Xsell') then 'Sahila Kathuria'
	end as rm,
	ba_uid,
	ba_name,
	ba_mobile,
	ba_login_name,
	ba_login_name_clean,
	ba_email_original,
	ba_name_from_email,
	preuw_uid,
	preuw_name,
	preuw_mobile,
	preuw_login_name,
	preuw_login_name_clean,
	preuw_email_original,
	preuw_name_from_email	
from tb_lead_case
left join tb_dsa_rm rm_sub on rm_sub.partner_uid=tb_lead_case.sub_agent_uid
left join tb_dsa_rm rm_main on rm_main.partner_uid=tb_lead_case.agent_uid
left join tb_other_rm on lower(tb_lead_case."source")=lower(tb_other_rm.sources_name)
left join cnd disp on disp.cnd_code::text=tb_lead_case.plus::text and disp.cnd_group='DISPOSITION'
left join tb_borrower_agent on tb_borrower_agent.ba_loan_id=tb_lead_case.loan_id
left join tb_preuw_agent on tb_preuw_agent.preuw_loan_id=tb_lead_case.loan_id
left join (
	select
		loan_id,
		round(sum(case when txn_type = 1358 then txn_amount else 0 end), 0) as registration_fees,
		round(sum(case when txn_type = 90133286 then txn_amount else 0 end), 0) as processing_fees
	from
		fair.public.cent_transaction 
	where deleted = 'N' 
	  and txn_state = 200
	group by loan_id) ct on
	ct.loan_id = tb_lead_case.loan_id
),
  tb_lead_result_case as (select *, case when uw_name_final!='UW Not Assigned' then 1 else 0 end as uw_allocated_flag,
	case when uw_status_final='Cancelled' then 1 else 0 end as uw_cancelled_flag,
	case when uw_status_final='Returned' then 1 else 0 end as uw_returned_flag,
	case when uw_status_final='Forward to CS' then 1 else 0 end as uw_forward_to_cs_flag,
	case when uw_status_final='Live' then 1 else 0 end as uw_live_flag,
  case when delist_date is not null and delist_date>uw_updated_date and loan_currentstate='Cancelled' and uw_status_final='Live' then 1 else 0 end as delist_flag from tb_lead_result)
select tb_report.*,tb_lead_result_case.* from tb_lead_result_case cross join tb_report
);

drop table if exists fair.public.dm_lead_channel_details;

create table fair.public.dm_lead_channel_details as (
select
	loan_id as lead_id,
	source,
	channel,
	sub_category,
	category
from
	fair.public.dm_lead_details
);

drop table if exists fair.public.dm_lead_borrower_basic_details;

create table fair.public.dm_lead_borrower_basic_details as (
select
	loan_id as loan_id,
	loan_registered_date as lead_registered_date,
	loan_type,
	loan_subtype,
	loan_othertype,
	product_type,
	loan_currentstate as lead_current_status,
	loan_desc as lead_description,
	user_id as borrower_id,
	risk_bucket,
	user_name as borrower_name,
	user_pan as borrower_pan,
	cibil_score,
	crif_score,
	user_dob as borrower_dob,
	age_loanregister as borrower_age_loandate,
	generations as borrower_generation,
	user_fulladdress as borrower_address,
	user_pin as borrower_pin,
	user_city as borrower_city,
	user_district as borrower_district,
	user_state as borrower_state,
	user_zone as borrower_zone,
	user_final_city as borrower_final_city,
	user_final_state as borrower_final_state,
	user_origion_state as borrower_origion_state,
	user_mobile as borrower_mobile,
	user_landline as borrower_landline,
	user_email as borrower_email,
	user_created_date as borrower_created_date,
	user_jobassigned_name as borrower_jobassigned_name,
	user_jobassigned_mobile as borrower_jobassigned_mobile,
	user_jobassigned_email as borrower_jobassigned_email,
	user_agent_name as borrower_agent_name,
	user_agent_mobile as borrower_agent_mobile,
	user_agent_email as borrower_agent_email,
	gender as borrower_gender,
	aadhaar_number as borrower_adhaarnumber,
	spouse as borrower_spouse_name,
	father as borrower_father_name,
	mother as borrower_mother_name,
	loan_main_borrower_first_name,
	loan_main_borrower_name,
	loan_main_borrower_mobile,
	loan_main_borrower_landline,
	loan_main_borrower_email
from
	fair.public.dm_lead_details
);

GRANT ALL ON TABLE dm_lead_details IN SCHEMA public to account_admin;
GRANT SELECT ON TABLE dm_lead_details in schema public to analytics_admin; 

GRANT ALL ON TABLE dm_channel_master IN SCHEMA public to account_admin;
GRANT SELECT ON TABLE dm_channel_master in schema public to analytics_admin; 

GRANT ALL ON TABLE dm_tenure_master IN SCHEMA public to account_admin;
GRANT SELECT ON TABLE dm_tenure_master in schema public to analytics_admin; 

GRANT ALL ON TABLE dm_pincode_master IN SCHEMA public to account_admin;
GRANT SELECT ON TABLE dm_pincode_master in schema public to analytics_admin; 

GRANT ALL ON TABLE dm_lead_channel_details IN SCHEMA public to account_admin;
GRANT SELECT ON TABLE dm_lead_channel_details in schema public to analytics_admin; 

GRANT ALL ON TABLE dm_lead_borrower_basic_details IN SCHEMA public to account_admin;
GRANT SELECT ON TABLE dm_lead_borrower_basic_details in schema public to analytics_admin; 

