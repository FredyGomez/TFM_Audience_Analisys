
--SPECIFIC VIZ MODEL

-- entity_dim

create or replace table yougov_viz.entity_dim as 
(select cat1, cat2, cat3, cat4, cat5, cat6, cat7, name, description,
definition_alias, subvariable_alias, entity_name, id, country_id
from yougov.entity_dim where country_id = 7)


-- demographic_fact

create or replace table  yougov_viz.demographic_fact as 
(select a.*, cast(b.weight as float64) as weight 
from yougov_viz.demographic_fact a join (select yougov_id, max(response) as weight 
	from yougov.user_response_all where alias = 'weight_natrep' 
	and country_id=7 group by yougov_id) b on (a.yougov_id=b.yougov_id))


-- yougov_fact

create or replace table yougov_viz.yougov_fact as 
(select cat1, cat2, cat3, cat4, cat5, cat6, cat7, name, description,
definition_alias, subvariable_alias, entity_name, selected, response, 
yougov_id, id, country_id
from yougov.entity_dim where country_id = 7)

