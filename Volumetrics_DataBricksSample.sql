-- Databricks notebook source

%scala
dbutils.widgets.dropdown("targetMarket", "NY", Seq("OH", "NY", "IN","OR","CT","WA", "SC", "GA","WI", "VA", "TN"), "Target Market");

var targetMarket = dbutils.widgets.get("targetMarket")

dbutils.widgets.dropdown("targetDomain", "medclaim_header_hist", Seq("medclaim_header_hist", "lab_result_hist", "pharmacy_hist", "eligibility_hist", "patient_hist", "medclaim_details_hist", "provider_hist"), "Target Domain");

var targetDomain = dbutils.widgets.get("targetDomain")

dbutils.widgets.dropdown("File_export", "CDOS", Seq("CDOS", "CDOS_ARCADIA", "CDOS_WELLMED", "cdos_dry_run"), "File_export");

var File_export = dbutils.widgets.get("File_export")

-- COMMAND ----------

%scala

var df = spark.sql("""select distinct loadtime from """+File_export+"""_history_validation."""+targetMarket+"""_"""+targetDomain+""" WHERE loadtime >= date_sub(current_date(), 120) ORDER BY loadtime desc""")
val loadtime_list = (df.select("loadtime").collect().map(_.getString(0)).mkString("','")).mkString
println(loadtime_list)

var query = spark.sql("""

SELECT *FROM (
SELECT loadtime, source_system_sk, count(source_system_sk) as count_source_system_sk

FROM """+File_export+"""_history_validation."""+targetMarket+"""_"""+targetDomain+"""
WHERE loadtime >= date_sub(current_date(), 120)
--and (GMPI is null or GMPI ='')
and (record_start_dt < current_date 
and nvl(record_end_dt,'9999-12-31')>current_date)
 GROUP BY loadtime, source_system_sk
)

PIVOT ( SUM(count_source_system_sk)
FOR loadtime IN ('"""+loadtime_list+"""')
)

""")
display(query)
-- COMMAND ----------
%scala

var df = spark.sql("""select distinct loadtime from """+File_export+"""_history_validation."""+targetMarket+"""_"""+targetDomain+""" WHERE loadtime >= date_sub(current_date(), 70) ORDER BY loadtime desc""")
val loadtime_list = (df.select("loadtime").collect().map(_.getString(0)).mkString("','")).mkString
println(loadtime_list)

var query = spark.sql("""

SELECT *FROM (
SELECT loadtime, source_system_sk, count(distinct entity_member_id) as count_entity_member_ID

FROM """+File_export+"""_history_validation."""+targetMarket+"""_"""+targetDomain+"""
WHERE loadtime >= date_sub(current_date(), 70)
--and record_start_dt < current_date and nvl(record_end_dt,'9999-12-31')>current_date
 GROUP BY loadtime, source_system_sk
)

PIVOT ( SUM(count_entity_member_ID)
FOR loadtime IN ('"""+loadtime_list+"""')
)

""")
display(query)

-- COMMAND ----------

%scala

var df = spark.sql("""select distinct loadtime from """+File_export+"""_history_validation."""+targetMarket+"""_"""+targetDomain+""" WHERE loadtime >= date_sub(current_date(), 80) ORDER BY loadtime desc""")
val loadtime_list = (df.select("loadtime").collect().map(_.getString(0)).mkString("','")).mkString
//println(loadtime_list) 

var query = spark.sql("""

SELECT *FROM (
SELECT loadtime, health_plan, count(health_plan) as count_health_plan
FROM """+File_export+"""_history_validation."""+targetMarket+"""_"""+targetDomain+"""
WHERE loadtime >= date_sub(current_date(), 80) GROUP BY loadtime, health_plan
)

PIVOT ( SUM(count_health_plan)
FOR loadtime IN ('"""+loadtime_list+"""')
)

""")
display(query)


-- COMMAND ----------

%scala

var df =  spark.sql (""" 
select distinct Filename from """+File_export+"""_history_validation."""+targetMarket+"""_provider_hist WHERE LoadTime in (select MAX(LoadTime) AS LoadTime from """+File_export+"""_history_validation."""+targetMarket+"""_provider_hist)
union
select distinct Filename from """+File_export+"""_history_validation."""+targetMarket+"""_eligibility_hist WHERE LoadTime in (select MAX(LoadTime) AS LoadTime from """+File_export+"""_history_validation."""+targetMarket+"""_eligibility_hist)
union
select distinct Filename from """+File_export+"""_history_validation."""+targetMarket+"""_empi_hist WHERE LoadTime in (select MAX(LoadTime) AS LoadTime from """+File_export+"""_history_validation."""+targetMarket+"""_empi_hist)
union
select distinct Filename from """+File_export+"""_history_validation."""+targetMarket+"""_incr_mbrhicnhistory_hist WHERE LoadTime in (select MAX(LoadTime) AS LoadTime from """+File_export+"""_history_validation."""+targetMarket+"""_incr_mbrhicnhistory_hist)
union
select distinct Filename from """+File_export+"""_history_validation."""+targetMarket+"""_inscope_patient_hist WHERE LoadTime in (select MAX(LoadTime) AS LoadTime from """+File_export+"""_history_validation."""+targetMarket+"""_inscope_patient_hist)
union
select distinct Filename from """+File_export+"""_history_validation."""+targetMarket+"""_lab_result_hist WHERE LoadTime in (select MAX(LoadTime) AS LoadTime from """+File_export+"""_history_validation."""+targetMarket+"""_lab_result_hist)
union
select distinct Filename from """+File_export+"""_history_validation."""+targetMarket+"""_location_hist WHERE LoadTime in (select MAX(LoadTime) AS LoadTime from """+File_export+"""_history_validation."""+targetMarket+"""_location_hist)
union
select distinct Filename from """+File_export+"""_history_validation."""+targetMarket+"""_medclaim_details_hist WHERE LoadTime in (select MAX(LoadTime) AS LoadTime from """+File_export+"""_history_validation."""+targetMarket+"""_medclaim_details_hist)
union
select distinct Filename from """+File_export+"""_history_validation."""+targetMarket+"""_medclaim_header_hist WHERE LoadTime in (select MAX(LoadTime) AS LoadTime from """+File_export+"""_history_validation."""+targetMarket+"""_medclaim_header_hist)
union
select distinct Filename from """+File_export+"""_history_validation."""+targetMarket+"""_patient_hist WHERE LoadTime in (select MAX(LoadTime) AS LoadTime from """+File_export+"""_history_validation."""+targetMarket+"""_patient_hist)
union
select distinct Filename from """+File_export+"""_history_validation."""+targetMarket+"""_pharmacy_hist WHERE LoadTime in (select MAX(LoadTime) AS LoadTime from """+File_export+"""_history_validation."""+targetMarket+"""_pharmacy_hist)
union
select distinct Filename from """+File_export+"""_history_validation."""+targetMarket+"""_pod_hist WHERE LoadTime in (select MAX(LoadTime) AS LoadTime from """+File_export+"""_history_validation."""+targetMarket+"""_pod_hist)
union
select distinct Filename from """+File_export+"""_history_validation."""+targetMarket+"""_provider_group_hist WHERE LoadTime in (select MAX(LoadTime) AS LoadTime from """+File_export+"""_history_validation."""+targetMarket+"""_provider_group_hist)

""")
display(df)
