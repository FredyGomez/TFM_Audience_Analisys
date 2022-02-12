-- denormalization

--STEP1
####YOUGOV_STATIC_MICS_MODEL####
## RESPONSE_LEVEL ##
create or replace table yougov_mics.response_level
PARTITION BY
   RANGE_BUCKET(country_code, GENERATE_ARRAY(0, 18, 1)) 
CLUSTER BY  variable_id as
select
user.date as week_id,
user.country_id as country_code,
user.yougov_id as respondent_id,
user.alias as variable_id,
user.response as response_id,
cast(weight.response as float64) as weight
from (select * from yougov.user_response where alias !='weight_natrep') user
join (select * from yougov.weight_all where date=(select max(date) from yougov.user_response)) weight
on user.country_id=weight.country_id and user.yougov_id=weight.yougov_id;

## REFERENCE_DATA ##
create or replace table yougov_mics.reference_data
PARTITION BY RANGE_BUCKET(country_code, GENERATE_ARRAY(0, 18, 1)) as
select
def_all.country_id as country_code,
0 as language_code,
case when split(def_all.category, ' | ')[safe_offset(0)] is not null then split(def_all.category, ' | ')[safe_offset(0)] else split(parent.category, ' | ')[safe_offset(0)] end as category_1,
case when split(def_all.category, ' | ')[safe_offset(1)] is not null then split(def_all.category, ' | ')[safe_offset(1)] else split(parent.category, ' | ')[safe_offset(1)] end as category_2,
case when split(def_all.category, ' | ')[safe_offset(2)] is not null then split(def_all.category, ' | ')[safe_offset(2)] else split(parent.category, ' | ')[safe_offset(2)] end as category_3,
case when split(def_all.category, ' | ')[safe_offset(3)] is not null then split(def_all.category, ' | ')[safe_offset(3)] else split(parent.category, ' | ')[safe_offset(3)] end as category_4,
case when split(def_all.category, ' | ')[safe_offset(1)] like '52 %' then null 
     when split(def_all.category, ' | ')[safe_offset(4)] is null and split(parent.category, ' | ')[safe_offset(1)] like '52 %' then null 
     when split(def_all.category, ' | ')[safe_offset(4)] is null and split(parent.category, ' | ')[safe_offset(1)] not like '52 %' then split(parent.category, ' | ')[safe_offset(4)]
     else split(def_all.category, ' | ')[safe_offset(4)] end as category_5,
case when split(def_all.category, ' | ')[safe_offset(5)] is not null then split(def_all.category, ' | ')[safe_offset(5)] else split(parent.category, ' | ')[safe_offset(5)] end as category_6,
case when split(def_all.category, ' | ')[safe_offset(6)] is not null then split(def_all.category, ' | ')[safe_offset(6)] else split(parent.category, ' | ')[safe_offset(6)] end as category_7,
case when gv.is_global_variable = 1 then 1 else 0 end as isglobal,
case when alias in (select distinct definition_alias from yougov.subvariables) then 1 else 0 end as isparent,
alias as variable_id,
codes.value as response_id,
parent_id,
country_iso as locale,
description as variable_text,
case when def_all.entity_name is not null then def_all.entity_name else def_all.name end as variable_name,
parent_name,
parent_text,
codes.name as response_text
from
(select alias, name, description, entity_name, category, country_id from yougov.definitions) def_all
left join
(select distinct subvariable_alias, definition_alias as parent_alias, country_id from yougov.subvariables) sub_all
on def_all.alias=sub_all.subvariable_alias and def_all.country_id=sub_all.country_id
left join
(select distinct name as parent_name, description as parent_text, alias as parent_id, category, country_id from yougov.definitions) parent
on sub_all.parent_alias=parent.parent_id and sub_all.country_id=parent.country_id
join yougov.country_dim coun
on def_all.country_id=coun.country_id
left join (select * from yougov.codes) codes
on def_all.alias=codes.definition_alias and def_all.country_id=codes.country_id
left join (select distinct variable,	is_global_variable,	country_id from yougov.global_variables) gv
on (replace(def_all.alias, 'pdl_','')=gv.variable or replace(def_all.alias, 'pdlc_','')=gv.variable or replace(def_all.alias, 'bix_','')=gv.variable or
replace(replace(def_all.alias, 'issues_scale_',''), '_','-')=gv.variable or replace(replace(def_all.alias, 'issues_importance_',''), '_','-')=gv.variable or 
replace(replace(def_all.alias, 'personality_single_',''), '_','-')=gv.variable or replace(replace(def_all.alias, 'attitudes_agree_',''), '_','-')=gv.variable or 
replace(replace(def_all.alias, 'attitudes_recoded_',''), '_','-')=gv.variable) 
and def_all.country_id=gv.country_id
union all 
select
def_all.country_id as country_code,
1 as language_code,
case when split(def_all.category, ' | ')[safe_offset(0)] is not null then split(def_all.category, ' | ')[safe_offset(0)] else split(parent.category, ' | ')[safe_offset(0)] end as category_1,
case when split(def_all.category, ' | ')[safe_offset(1)] is not null then split(def_all.category, ' | ')[safe_offset(1)] else split(parent.category, ' | ')[safe_offset(1)] end as category_2,
case when split(def_all.category, ' | ')[safe_offset(2)] is not null then split(def_all.category, ' | ')[safe_offset(2)] else split(parent.category, ' | ')[safe_offset(2)] end as category_3,
case when split(def_all.category, ' | ')[safe_offset(3)] is not null then split(def_all.category, ' | ')[safe_offset(3)] else split(parent.category, ' | ')[safe_offset(3)] end as category_4,
case when split(def_all.category, ' | ')[safe_offset(1)] like '52 %' then null 
     when split(def_all.category, ' | ')[safe_offset(4)] is null and split(parent.category, ' | ')[safe_offset(1)] like '52 %' then null 
     when split(def_all.category, ' | ')[safe_offset(4)] is null and split(parent.category, ' | ')[safe_offset(1)] not like '52 %' then split(parent.category, ' | ')[safe_offset(4)]
     else split(def_all.category, ' | ')[safe_offset(4)] end as category_5,
case when split(def_all.category, ' | ')[safe_offset(5)] is not null then split(def_all.category, ' | ')[safe_offset(5)] else split(parent.category, ' | ')[safe_offset(5)] end as category_6,
case when split(def_all.category, ' | ')[safe_offset(6)] is not null then split(def_all.category, ' | ')[safe_offset(6)] else split(parent.category, ' | ')[safe_offset(6)] end as category_7,
case when gv.is_global_variable = 1 then 1 else 0 end as isglobal,
case when alias in (select distinct definition_alias from yougov.subvariables) then 1 else 0 end as isparent,
alias as variable_id,
codes.value as response_id,
parent_id,
country_iso as locale,
description as variable_text,
case when def_all.entity_name is not null then def_all.entity_name else def_all.name end as variable_name,
parent_name,
parent_text,
codes.name as response_text
from
(select alias, name, description, entity_name, category, country_id from yougov.definitions_lan) def_all
left join
(select distinct subvariable_alias, definition_alias as parent_alias, country_id from yougov.subvariables_lan) sub_all
on def_all.alias=sub_all.subvariable_alias and def_all.country_id=sub_all.country_id
left join
(select distinct name as parent_name, description as parent_text, alias as parent_id, category, country_id from yougov.definitions_lan) parent
on sub_all.parent_alias=parent.parent_id and sub_all.country_id=parent.country_id
join yougov.country_dim coun
on def_all.country_id=coun.country_id
left join (select * from yougov.codes_lan) codes
on def_all.alias=codes.definition_alias and def_all.country_id=codes.country_id
left join (select distinct variable,	is_global_variable,	country_id from yougov.global_variables) gv
on (replace(def_all.alias, 'pdl_','')=gv.variable or replace(def_all.alias, 'pdlc_','')=gv.variable or replace(def_all.alias, 'bix_','')=gv.variable or
replace(replace(def_all.alias, 'issues_scale_',''), '_','-')=gv.variable or replace(replace(def_all.alias, 'issues_importance_',''), '_','-')=gv.variable or 
replace(replace(def_all.alias, 'personality_single_',''), '_','-')=gv.variable or replace(replace(def_all.alias, 'attitudes_agree_',''), '_','-')=gv.variable or 
replace(replace(def_all.alias, 'attitudes_recoded_',''), '_','-')=gv.variable) 
and def_all.country_id=gv.country_id;


-- STEP 2





--FINAL DENORMALIZED INTERACTIVE MODEL

--(BEFORE THIS WE NEED TO INTEGRATE MICS QUERIES)






create or replace table yougov_viz.responses_definitions as
select a.category_1,a.category_2, a.category_3, a.category_4, a.category_5, a.category_6, a.category_7, a.variable_text,a.response_text, b.* 
from yougov_mics.reference_data a join yougov_mics.response_level b on (a.variable_id=b.variable_id) and (a.country_code=b.country_code) and (a.response_id=b.response_id)
where language_code = 0 
and b.week_id='2022-01-23'
and a.country_code=7
and b.country_code=7




-- Extracting the respondent and weights in case is needed


create or replace table yougov_viz.respondent_weight as select country_code, respondent_id, max(weight) as weight from yougov_viz.responses_definitions
group by country_code, respondent_id