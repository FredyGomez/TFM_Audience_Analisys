--QUERIES TFM
DECLARE country ARRAY<STRING>;
DECLARE country_id INT64 DEFAULT 0;
DECLARE c INT64 DEFAULT 0;
DECLARE date_insert date;
DECLARE table_codes_suf string;
DECLARE TableCodesExists INT64 DEFAULT 0;
DECLARE table_def_suf STRING;
DECLARE TableDefinitionsExists INT64 DEFAULT 0;
DECLARE table_sub_suf STRING;
DECLARE TableSubvariablesExists INT64 DEFAULT 0;
DECLARE table_codes_lan_suf string;
DECLARE TableCodesLanExists INT64 DEFAULT 0;
DECLARE table_def_lan_suf STRING;
DECLARE TableDefinitionsLanExists INT64 DEFAULT 0;
DECLARE table_sub_lan_suf STRING;
DECLARE TableSubvariablesLanExists INT64 DEFAULT 0;
DECLARE table_resp_suf STRING;
DECLARE TableResponsesExists INT64 DEFAULT 0;
DECLARE interval_days INT64;
SET interval_days=4;

create or replace table yougov.codes(definition_alias	STRING, value	STRING, name STRING, selected int64,country_id int64, date DATE) PARTITION BY RANGE_BUCKET(country_id, GENERATE_ARRAY(1, 18, 1));
create or replace table yougov.codes_lan(definition_alias	STRING, value	STRING, name STRING, selected int64,country_id int64, date DATE) PARTITION BY RANGE_BUCKET(country_id, GENERATE_ARRAY(1, 18, 1));
create or replace table yougov.subvariables(definition_alias STRING, subvariable_alias STRING, country_id int64, date DATE) PARTITION BY RANGE_BUCKET(country_id, GENERATE_ARRAY(1, 18, 1));
create or replace table yougov.subvariables_lan(definition_alias STRING, subvariable_alias STRING, country_id int64, date DATE) PARTITION BY RANGE_BUCKET(country_id, GENERATE_ARRAY(1, 18, 1));
create or replace table yougov.definitions(alias STRING, type STRING, level STRING, name STRING, description STRING, entity_name STRING, category STRING, uniform_basis int64, min	STRING, max	STRING, roles	STRING, origin_source STRING, origin_metric STRING, origin_entity STRING, time_limit int64, country_id int64, date	DATE) PARTITION BY RANGE_BUCKET(country_id, GENERATE_ARRAY(1, 18, 1));
create or replace table yougov.definitions_lan(alias STRING, type STRING, level STRING, name STRING, description STRING, entity_name STRING, category STRING, uniform_basis int64, min	STRING, max	STRING, roles	STRING, origin_source STRING, origin_metric STRING, origin_entity STRING, time_limit int64, country_id int64, date	DATE) PARTITION BY RANGE_BUCKET(country_id, GENERATE_ARRAY(1, 18, 1));
create or replace table yougov.user_response(yougov_id STRING, alias	STRING, response	STRING, country_id int64, date DATE) PARTITION BY RANGE_BUCKET(country_id, GENERATE_ARRAY(1, 18, 1));
create or replace table yougov_integration.profiles_not_exist_tables(not_exist_table STRING);

#Iteramos para todos los países
SET country = ['au', 'be', 'ca', 'cn', 'de', 'es', 'fr', 'gb', 'hk', 'ie', 'inurb', 'it', 'nl', 'ph', 'pl', 'pt', 'us'];

WHILE c < ARRAY_LENGTH(country) DO
  SET c = c + 1;
  SET table_codes_suf =  (SELECT CONCAT("codes_", country[ORDINAL(c)], "_", FORMAT_DATE('%Y%m%d', DATE_SUB( CURRENT_DATE(), INTERVAL interval_days DAY))));
  SET table_sub_suf =  (SELECT CONCAT("subvariables_", country[ORDINAL(c)], "_", FORMAT_DATE('%Y%m%d', DATE_SUB( CURRENT_DATE(), INTERVAL interval_days DAY))));
  SET table_def_suf =  (SELECT CONCAT("definitions_", country[ORDINAL(c)], "_", FORMAT_DATE('%Y%m%d', DATE_SUB( CURRENT_DATE(), INTERVAL interval_days DAY))));
  SET table_codes_lan_suf =  (SELECT CONCAT("codes_", country[ORDINAL(c)], "_lan_", FORMAT_DATE('%Y%m%d', DATE_SUB( CURRENT_DATE(), INTERVAL interval_days DAY))));
  SET table_sub_lan_suf =  (SELECT CONCAT("subvariables_", country[ORDINAL(c)], "_lan_", FORMAT_DATE('%Y%m%d', DATE_SUB( CURRENT_DATE(), INTERVAL interval_days DAY))));
  SET table_def_lan_suf =  (SELECT CONCAT("definitions_", country[ORDINAL(c)], "_lan_", FORMAT_DATE('%Y%m%d', DATE_SUB( CURRENT_DATE(), INTERVAL interval_days DAY))));
  SET table_resp_suf =  (SELECT CONCAT("long_", country[ORDINAL(c)], "_", FORMAT_DATE('%Y%m%d', DATE_SUB( CURRENT_DATE(), INTERVAL interval_days DAY))));

  IF country[ORDINAL(c)]='inurb' THEN 
    SET country_id = 15;
  ELSE
    SET country_id = (SELECT country_id from yougov.country_dim where country_iso=country[ORDINAL(c)]);
  END IF ;

  ####CODES####
  #chequeo si existe la tabla de codes
  SET TableCodesExists = (SELECT COUNT(1) AS cnt
  FROM `converged-havas-de.yougov_integration.__TABLES_SUMMARY__`
  WHERE table_id = table_codes_suf);

  IF TableCodesExists>0 THEN

   EXECUTE IMMEDIATE
	CONCAT('insert yougov.codes select *, ', country_id, ' as country_id, DATE_SUB( CURRENT_DATE(), INTERVAL ', interval_days, ' DAY) as date from converged-havas-de.yougov_integration.',table_codes_suf);
  
  ELSE 
   INSERT yougov_integration.profiles_not_exist_tables select table_codes_suf as not_exist_table;

  END IF ;

  ####SUBVARIABLES####
  #chequeo si existe la tabla de subvariables
  SET TableSubvariablesExists = (SELECT COUNT(1) AS cnt
  FROM `converged-havas-de.yougov_integration.__TABLES_SUMMARY__`
  WHERE table_id = table_sub_suf);

  IF TableSubvariablesExists>0 THEN

   EXECUTE IMMEDIATE
	CONCAT('insert yougov.subvariables select *, ', country_id, ' as country_id, DATE_SUB( CURRENT_DATE(), INTERVAL ', interval_days, ' DAY) as date from converged-havas-de.yougov_integration.',table_sub_suf);

  ELSE 
   INSERT yougov_integration.profiles_not_exist_tables select table_sub_suf as not_exist_table;

  END IF ;

  ####DEFINITIONS####
  #chequeo si existe la tabla de definitions
  SET TableDefinitionsExists = (SELECT COUNT(1) AS cnt
  FROM `converged-havas-de.yougov_integration.__TABLES_SUMMARY__`
  WHERE table_id = table_def_suf);

  IF TableDefinitionsExists>0 THEN

   EXECUTE IMMEDIATE
	CONCAT('insert yougov.definitions select alias,type,level,name,description,entity_name,category,uniform_basis	,cast(min as string) as min,cast(max as string) as max,roles,origin_source,origin_metric, cast(origin_entity as string) as origin_entity, time_limit, ', country_id, ' as country_id, DATE_SUB( CURRENT_DATE(), INTERVAL  ', interval_days, ' DAY) as date from converged-havas-de.yougov_integration.',table_def_suf);

  ELSE 
   INSERT yougov_integration.profiles_not_exist_tables select table_def_suf as not_exist_table;

  END IF ;

####CODES LAN####
  #chequeo si existe la tabla de codes_lan
  IF country[ORDINAL(c)] IN ('it','es', 'fr', 'de', 'pt') THEN
    SET TableCodesLanExists = (SELECT COUNT(1) AS cnt
    FROM `converged-havas-de.yougov_integration.__TABLES_SUMMARY__`
    WHERE table_id = table_codes_lan_suf);

    IF TableCodesLanExists>0 THEN

      EXECUTE IMMEDIATE
	    CONCAT('insert yougov.codes_lan select *, ', country_id, ' as country_id, DATE_SUB( CURRENT_DATE(), INTERVAL ', interval_days, ' DAY) as date from converged-havas-de.yougov_integration.',table_codes_lan_suf);
  
    ELSE 
      INSERT yougov_integration.profiles_not_exist_tables select table_codes_lan_suf as not_exist_table;
    END IF;
  END IF ;

  ####SUBVARIABLES LAN####
  #chequeo si existe la tabla de subvariables_lan
  IF country[ORDINAL(c)] IN ('it','es', 'fr', 'de', 'pt') THEN
    SET TableSubvariablesLanExists = (SELECT COUNT(1) AS cnt
    FROM `converged-havas-de.yougov_integration.__TABLES_SUMMARY__`
    WHERE table_id = table_sub_lan_suf);

    IF TableSubvariablesLanExists>0 THEN

      EXECUTE IMMEDIATE
      CONCAT('insert yougov.subvariables_lan select *, ', country_id, ' as country_id, DATE_SUB( CURRENT_DATE(), INTERVAL ', interval_days, ' DAY) as date from converged-havas-de.yougov_integration.',table_sub_lan_suf);

    ELSE 
      INSERT yougov_integration.profiles_not_exist_tables select table_sub_lan_suf as not_exist_table;

    END IF ;
  END IF ;

  ####DEFINITIONS LAN####
  #chequeo si existe la tabla de definitions_lan
  IF country[ORDINAL(c)] IN ('it','es', 'fr', 'de', 'pt') THEN
    SET TableDefinitionsLanExists = (SELECT COUNT(1) AS cnt
    FROM `converged-havas-de.yougov_integration.__TABLES_SUMMARY__`
    WHERE table_id = table_def_lan_suf);

    IF TableDefinitionsLanExists>0 THEN

      EXECUTE IMMEDIATE
      CONCAT('insert yougov.definitions_lan select alias,type,level,name,description,entity_name,category,uniform_basis	,cast(min as string) as min,cast(max as string) as max,roles,origin_source,origin_metric, cast(origin_entity as string) as origin_entity, time_limit, ', country_id, ' as country_id, DATE_SUB( CURRENT_DATE(), INTERVAL  ', interval_days, ' DAY) as date from converged-havas-de.yougov_integration.',table_def_lan_suf);

    ELSE 
      INSERT yougov_integration.profiles_not_exist_tables select table_def_lan_suf as not_exist_table;
  
    END IF ;
  END IF ;

  ####USER_RESPONSE####
  #chequeo si existe la tabla de user_response
  SET TableResponsesExists = (SELECT COUNT(1) AS cnt
  FROM `converged-havas-de.yougov_integration.__TABLES_SUMMARY__`
  WHERE table_id = table_resp_suf);

  IF TableResponsesExists>0 THEN

   EXECUTE IMMEDIATE
	CONCAT('insert yougov.user_response select *, ', country_id, ' as country_id, DATE_SUB( CURRENT_DATE(), INTERVAL ', interval_days, ' DAY) as date from converged-havas-de.yougov_integration.',table_resp_suf);
  
  ELSE 
   INSERT yougov_integration.profiles_not_exist_tables select table_resp_suf as not_exist_table;

  END IF ;

END WHILE;


####CODES_INSERT####
INSERT yougov.codes_all(definition_alias, value, name, selected, country_id, date)
select * from `converged-havas-de.yougov.codes`;

####CODES_DATE_INSERT####
INSERT converged_user.codes_all_date(definition_alias, value, name, selected, country_id, date)
select * from `converged-havas-de.yougov.codes`;

####SUBVARIABLES_INSERT####
INSERT yougov.subvariables_all(definition_alias, subvariable_alias, country_id, date)
select * from `converged-havas-de.yougov.subvariables`;

####SUBVARIABLES_DATE_INSERT####
INSERT converged_user.subvariables_all_date(definition_alias, subvariable_alias, country_id, date)
select * from `converged-havas-de.yougov.subvariables`;

####DEFINITIONS_INSERT####
INSERT yougov.definitions_all(alias, type, level, name, description, entity_name, category, uniform_basis, min, max,
roles, origin_source, origin_metric, origin_entity, time_limit, country_id, date)
select * from `converged-havas-de.yougov.definitions`;

####DEFINITIONS_DATE_INSERT####
INSERT converged_user.definitions_all_date(alias, type, level, name, description, entity_name, category, uniform_basis, min, max,
roles, origin_source, origin_metric, origin_entity, time_limit, country_id, date)
select * from `converged-havas-de.yougov.definitions`;

####WEIGHT_ALL_DATE_INSERT####
INSERT yougov.weight_all
select * from `converged-havas-de.yougov.user_response` where alias='weight_natrep';

####USER_RESPONSE_EVOLUTION####
set date_insert =(select distinct date from yougov.user_response);

create or replace table yougov.user_response_pivot as
select hist.yougov_id, hist.alias, hist.response, hist.country_id, first_time_seen,
case when new_del.last_time_seen is not null then new_del.last_time_seen else hist.last_time_seen end as last_time_seen
from (select * from yougov.user_response_evolution where last_time_seen= (select max(last_time_seen) from yougov.user_response_evolution)) hist
left join
(select yougov_id, alias, response, country_id, date as last_time_seen
from yougov.user_response where alias !='weight_natrep') new_del
on hist.yougov_id=new_del.yougov_id and hist.alias=new_del.alias and hist.response=new_del.response and hist.country_id=new_del.country_id
union all
select yougov_id, alias, response, country_id, first_time_seen, last_time_seen
from yougov.user_response_evolution where last_time_seen<(select max(last_time_seen) from yougov.user_response_evolution)
union all
select yougov_id, alias, response, country_id, date_insert as first_time_seen, date_insert as last_time_seen
from
(select yougov_id, alias, response, country_id from yougov.user_response where alias !='weight_natrep'
except distinct select yougov_id, alias, response, country_id from yougov.user_response_evolution
where last_time_seen= (select max(last_time_seen) from yougov.user_response_evolution));

create or replace table yougov.user_response_evolution
PARTITION BY
   RANGE_BUCKET(country_id, GENERATE_ARRAY(0, 18, 1)) as
select * from yougov.user_response_pivot;
