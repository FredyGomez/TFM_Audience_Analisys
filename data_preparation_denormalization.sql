-- denormalization
--STEP1
--first we get some staging table for responses and after that we create the denormalized final table, this is to seaprate scanning from parsing and other compute operations but could be done in a single step
-- this consumes more storage resources but takes less tiem in total and is safer for bigquery optimizer

## RESPONSE_LEVEL ##

create or replace table yougov_viz.stg_weight as
select yougov_id, alias, response, country_id, date from yougov_viz.user_response where alias = 'weight_natrep';


-- now we generate the final response level table
create or replace table yougov_viz.stg_response_level as
select
user.date as week_id,
user.country_id as country_code,
user.yougov_id as respondent_id,
user.alias as variable_id,
user.response as response_id,
cast(weight.response as float64) as weight
from (select * from yougov_viz.user_response where alias !='weight_natrep') user
join (select * from yougov_viz.stg_weight) weight
on user.country_id=weight.country_id and user.yougov_id=weight.yougov_id;




## REFERENCE_DATA ##
create or replace table yougov_viz.stg_reference_data as
select
def.country_id as country_code,
0 as language_code,
case when split(def.category, ' | ')[safe_offset(0)] is not null then split(def.category, ' | ')[safe_offset(0)] else split(parent.category, ' | ')[safe_offset(0)] end as category_1,
case when split(def.category, ' | ')[safe_offset(1)] is not null then split(def.category, ' | ')[safe_offset(1)] else split(parent.category, ' | ')[safe_offset(1)] end as category_2,
case when split(def.category, ' | ')[safe_offset(2)] is not null then split(def.category, ' | ')[safe_offset(2)] else split(parent.category, ' | ')[safe_offset(2)] end as category_3,
case when split(def.category, ' | ')[safe_offset(3)] is not null then split(def.category, ' | ')[safe_offset(3)] else split(parent.category, ' | ')[safe_offset(3)] end as category_4,
case when split(def.category, ' | ')[safe_offset(1)] like '52 %' then null 
     when split(def.category, ' | ')[safe_offset(4)] is null and split(parent.category, ' | ')[safe_offset(1)] like '52 %' then null 
     when split(def.category, ' | ')[safe_offset(4)] is null and split(parent.category, ' | ')[safe_offset(1)] not like '52 %' then split(parent.category, ' | ')[safe_offset(4)]
     else split(def.category, ' | ')[safe_offset(4)] end as category_5,
case when split(def.category, ' | ')[safe_offset(5)] is not null then split(def.category, ' | ')[safe_offset(5)] else split(parent.category, ' | ')[safe_offset(5)] end as category_6,
case when split(def.category, ' | ')[safe_offset(6)] is not null then split(def.category, ' | ')[safe_offset(6)] else split(parent.category, ' | ')[safe_offset(6)] end as category_7,
0 as isglobal,
case when alias in (select distinct definition_alias from yougov.subvariables) then 1 else 0 end as isparent,
alias as variable_id,
codes.value as response_id,
parent_id,
country_iso as locale,
description as variable_text,
case when def.entity_name is not null then def.entity_name else def.name end as variable_name,
parent_name,
parent_text,
codes.name as response_text
from
(select alias, name, description, entity_name, category, country_id from yougov_viz.definitions) def
left join
(select distinct subvariable_alias, definition_alias as parent_alias, country_id from yougov_viz.subvariables) sub
on def.alias=sub.subvariable_alias and def.country_id=sub.country_id
left join
(select distinct name as parent_name, description as parent_text, alias as parent_id, category, country_id from yougov_viz.definitions) parent
on sub.parent_alias=parent.parent_id and sub.country_id=parent.country_id
join yougov_viz.country_dim coun
on def.country_id=coun.country_id
left join (select * from yougov_viz.codes) codes
on def.alias=codes.definition_alias and def.country_id=codes.country_id;



-- STEP 2
-- final denormalization
-- big table with everything for a given country
create or replace table yougov_viz.responses_definitions as
select a.category_1,a.category_2, a.category_3, a.category_4, a.category_5, a.category_6, a.category_7, a.variable_text,a.response_text, b.* 
from yougov_viz.reference_data a join yougov_mics.response_level b on (a.variable_id=b.variable_id) and (a.country_code=b.country_code) and (a.response_id=b.response_id)
where language_code = 0 
and a.country_code=7
and b.country_code=7;


-- respondents in case is needed separated
create or replace table yougov_viz.respondent_weight as select country_code, respondent_id, max(weight) as weight from yougov_viz.responses_definitions
group by country_code, respondent_id;