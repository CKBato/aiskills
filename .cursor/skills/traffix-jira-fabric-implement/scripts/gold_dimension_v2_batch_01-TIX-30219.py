# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Gold Dimension Tables
# - Supersedes old dimensions.
# - Star schema which will follow Power BI best practice guidance: https://learn.microsoft.com/en-us/power-bi/guidance/star-schema
# - Initial dimensions will be SCD 1
# - Initial load pattern will be full loads
#  

# MARKDOWN ********************

# ## Spark Configuration

# CELL ********************

%run spark_configuration

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

history_start_date='2023-01-01'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run silver_configuration

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold Configuration

# CELL ********************

# table path

path_source_tgtms_hub="/Tables/tgtms_hub/"
path_source_netsuite="/Tables/netsuite/"
path_source_office365="/Tables/office365/"
path_source_dimensions="/Tables/dimensions/"
path_source_truckmate = "/Tables/truckmate/"
path_source_lookup = "/Tables/lookup/"
path_source_hubspot = "/Tables/hubspot/"
path_source_hr = "/Tables/hr/"

path_target="/Tables/dimension/"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run gold_configuration

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold dim_load_action Table

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_load_action"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="primary_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Query_

# CELL ********************

# declare full load query. add primary_key, calculated __sys_synced and __sys_deleted columns

query_full="""
WITH action_types AS 
(
    SELECT 'EDI Accept' AS action_type, 'Text' AS action_value_type
    UNION 
    SELECT 'Commodity' AS action_type, 'Text' AS action_value_type
    UNION 
    SELECT 'Handling Unit Count' AS action_type, 'Numeric' AS action_value_type 
    UNION 
    SELECT 'Customs Broker' AS action_type, 'Text' AS action_value_type          
    UNION 
    SELECT 'Service Option' AS action_type, 'Text' AS action_value_type        
    UNION 
    SELECT 'Equipment' AS action_type, 'Text' AS action_value_type         
    UNION 
    SELECT 'Customer Charge' AS action_type, 'Numeric' AS action_value_type         
    UNION 
    SELECT 'Weight' AS action_type, 'Numeric' AS action_value_type         
    UNION 
    SELECT 'Shipment Value' AS action_type, 'Numeric' AS action_value_type         
    UNION 
    SELECT 'Appointment Numbers' AS action_type, 'Text' AS action_value_type         
    UNION 
    SELECT 'Appointment' AS action_type, 'Date' AS action_value_type         
    UNION 
    SELECT 'Early Window' AS action_type, 'Date' AS action_value_type         
    UNION 
    SELECT 'Comments' AS action_type, 'Text' AS action_value_type         
    UNION 
    SELECT 'Reference Numbers' AS action_type, 'Text' action_value_type         
    UNION 
    SELECT 'Appointment Required Flag' AS action_type, 'Text' AS action_value_type         
    UNION 
    SELECT 'Appointment Requested Flag' AS action_type, 'Text' AS action_value_type         
    UNION 
    SELECT 'Appointment Made Flag' AS action_type, 'Text' AS action_value_type         
)
, action_subtypes AS 
(
    SELECT
        qualifier_name AS action_subtype,
        CASE qualifier_type
            WHEN 'Comment' THEN 'Comments'
            WHEN 'RefNum' THEN 'Reference Numbers'
        END AS action_type
    FROM {qualifier}
)
, action_values AS
(
    SELECT 'EDI Accept' AS action_type, 'AcceptTender' AS action_value_text
    UNION
    SELECT 'Service Option' AS action_type, service_option_name AS action_value_text
    FROM {service_option}
    UNION
    SELECT 'Equipment' AS action_type, equipment_name AS action_value_text
    FROM {available_equipment}
    UNION
    SELECT 'Appointment Required Flag' AS action_type, 'Yes' AS action_value_text
    UNION 
    SELECT 'Appointment Required Flag' AS action_type, 'No' AS action_value_text
    UNION
    SELECT 'Appointment Requested Flag' AS action_type, 'Yes' AS action_value_text
    UNION 
    SELECT 'Appointment Requested Flag' AS action_type, 'No' AS action_value_text
    UNION
    SELECT 'Appointment Made Flag' AS action_type, 'Yes' AS action_value_text
    UNION 
    SELECT 'Appointment Made Flag' AS action_type, 'No' AS action_value_text
)
, action_relative_to_release_types AS
(
    SELECT 'Before' AS action_relative_to_release
    UNION 
    SELECT 'During' AS action_relative_to_release
    UNION
    SELECT 'After' AS action_relative_to_release
)
, revision_types AS
(
    SELECT 'Creation' AS revision_type
    UNION 
    SELECT 'Modification' AS revision_type
    UNION
    SELECT 'Deletion' AS revision_type
)
SELECT 
    -1 AS load_action_key, 
    'Undefined' AS primary_key,
    'Undefined' AS action_type,
    'Undefined' AS action_value_type,
    'Undefined' AS action_subtype,
    'Undefined' AS action_value_text,
    'Undefined' AS action_relative_to_release,
    'Undefined' AS revision_type,
    TO_TIMESTAMP(current_timestamp(), 'M/d/y h:m:ss a') AS __sys_synced,
    CAST(0 AS BOOLEAN) AS __sys_deleted
UNION
SELECT
    ROW_NUMBER() OVER(ORDER BY t1.action_type) AS load_action_key, 
    CONCAT_WS('|', t1.action_type, action_subtype, action_value_text, action_relative_to_release, revision_type) AS primary_key,
    t1.action_type, 
    t1.action_value_type, 
    COALESCE(t2.action_subtype, '') AS action_subtype, 
    COALESCE(t3.action_value_text, '') AS action_value_text, 
    t4.action_relative_to_release, 
    t5.revision_type,
    TO_TIMESTAMP(current_timestamp(), 'M/d/y h:m:ss a') AS __sys_synced,
    CAST(0 AS BOOLEAN) AS __sys_deleted
FROM 
    action_types AS t1
    LEFT JOIN action_subtypes AS t2
        ON t1.action_type = t2.action_type
    LEFT JOIN action_values AS t3
        ON t1.action_type = t3.action_type
    CROSS JOIN  action_relative_to_release_types AS t4
    CROSS JOIN revision_types AS t5
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

qualifier=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'qualifier')
service_option=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'service_option')
available_equipment=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'available_equipment')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Write_

# CELL ********************

print(f"{table_target} table full load started...")

df_source = spark.sql(
    query_full,
    qualifier=qualifier,
    service_option=service_option,
    available_equipment=available_equipment
)

write_gold_table(
    df=df_source,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,   # ✅ no forced partition
    zorder_cols=None,      # ✅ optional, often unnecessary here
    mode="overwrite",
    run_optimize=True
)

print(f"{table_target} table full load completed")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{table_target} table full load started...")

# create target table dataframe. make sure to modify parameters per target table
df_source=spark.sql(query_full, qualifier=qualifier, service_option=service_option, available_equipment=available_equipment)

# convert dataframe to temporary view first in order to save it later as delta managed table
df_source.createOrReplaceTempView(f"{table_source}")
df_source=spark.sql(f"select * from {table_source}")
spark.catalog.dropTempView(f"{table_source}")

# save target dataframe
df_source.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{current_lakehouse_gold_abfss+path_target+table_target}")
print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ## Gold dim_user Table

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_user"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="primary_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Query_

# CELL ********************

# declare full load query. add primary_key, calculated __sys_synced and __sys_deleted columns

query_full="""

WITH 
user_cte AS
(
	SELECT 
		--ROW_NUMBER() OVER(ORDER BY u1.upn, u1.created_date) AS user_key, 
		lower(u1.upn) as primary_key,
		lower(u1.upn) as upn,
		u1.first_name,
		u1.last_name,
		u1.full_name,
		lower(u1.email) as email,
		u1.phone_number,
		u1.job_title,
		u1.country,
		u1.company,
		u1.department,
		u1.functional_group,
		u1.subgroup,
		u1.manager,
		lower(u1.manager_email) as manager_email,
		u1.created_date,
		u1.disabled_date,
		CASE u1.status WHEN 1 THEN 'Active' WHEN 0 THEN 'Inactive' END AS status,
		COALESCE(u2.primary_division_code, 'undefined') AS primary_division_code,
		COALESCE(t.division_name, 'undefined') AS tms_3g_division_name,
		COALESCE(b.branch_name, 'undefined') AS netsuite_branch_name,
		COALESCE(b.subsidiary_name, 'undefined') AS subsidiary_name,
		COALESCE(b.home_currency, 'undefined') AS home_currency,
		COALESCE(u2.status, 'undefined') AS tms_3g_status,
		u2.user_3g_id AS tms_3g_userid,
		COALESCE(e.status, 'undefined') AS netsuite_status,
		COALESCE(e.adp_position_id, 'undefined') AS position_id,
		COALESCE(e1.adp_position_id, 'undefined') AS manager_position_id
		,COALESCE(
			tu.employee_type,
			CASE
				WHEN LOWER(bul.employee_id) = 'emp' THEN 'Employee'
				WHEN LOWER(bul.employee_id) = 'con' THEN 'Contractor'
			END,
			'undefined'
		) AS employee_type
		,COALESCE(tu.office_name, 'undefined') AS office_name
		,tu.disabled_date AS adp_disabled_date
		,COALESCE(tu.netsuite_job_title, 'undefined') AS netsuite_job_title
		,COALESCE(tu.netsuite_manager, 'undefined') AS netsuite_manager

		,COALESCE(tu.adp_payroll_company_code, 'undefined') AS adp_payroll_company_code
		,COALESCE(tu.adp_file_number, 'undefined') AS adp_file_number
		,COALESCE(tu.adp_position_id, 'undefined') AS adp_position_id
		,COALESCE(tu.adp_first_name, 'undefined') AS adp_first_name
		,COALESCE(tu.adp_last_name, 'undefined') AS adp_last_name
		,COALESCE(tu.adp_middle_initial, 'undefined') AS adp_middle_initial
		,COALESCE(tu.adp_job_title, 'undefined') AS adp_job_title
		,COALESCE(tu.adp_home_department_code, 'undefined') AS adp_home_department_code
		,COALESCE(tu.adp_department_description, 'undefined') AS adp_department_description
		,COALESCE(tu.adp_position_status, 'undefined') AS adp_position_status
		,COALESCE(tu.adp_is_enabled, 0) AS adp_is_enabled
		,tu.adp_termination_date
		,COALESCE(tu.adp_is_loa, 0) AS adp_is_loa
		,tu.adp_loa_start_date
		,tu.adp_hire_date
		,DATEDIFF(CURRENT_DATE(),tu.adp_hire_date) AS days_of_service
		,COALESCE(tu.adp_email, 'undefined') AS adp_email
		,COALESCE(tu.adp_user_fullname, 'undefined') AS adp_user_fullname
		,COALESCE(tu.adp_manager, 'undefined') AS adp_manager
		,COALESCE(tu.adp_job_title_code, 'undefined') AS adp_job_title_code

		,COALESCE(tu.flag_missing_country, 0) AS flag_missing_country
		,COALESCE(tu.flag_missing_department, 0) AS flag_missing_department
		,COALESCE(tu.flag_missing_office_name, 0) AS flag_missing_office_name
		,COALESCE(tu.flag_missing_manager, 0) AS flag_missing_manager
		,COALESCE(
			tu.flag_invalid_employee_type,
			CASE
				WHEN LOWER(bul.employee_id) IN ('emp', 'con') THEN 0
				ELSE 1
			END,
			0
		) AS flag_invalid_employee_type
		,COALESCE(tu.flag_missing_job_title, 0) AS flag_missing_job_title
		,COALESCE(tu.flag_mismatch_manager_adp_to_azure, 0) AS flag_mismatch_manager_adp_to_azure
		,COALESCE(tu.flag_mismatch_job_title_adp_to_azure, 0) AS flag_mismatch_job_title_adp_to_azure
		,COALESCE(tu.flag_mismatch_manager_netsuite_to_azure, 0) AS flag_mismatch_manager_netsuite_to_azure
		,COALESCE(tu.flag_mismatch_job_title_netsuite_to_azure, 0) AS flag_mismatch_job_title_netsuite_to_azure
		,ROW_NUMBER() OVER(PARTITION BY u1.upn ORDER BY e.created_date DESC) AS RN 
	FROM 
		{user_list} AS u1
		LEFT JOIN {user_3g} AS u2
			ON LOWER(u1.upn) = LOWER(u2.user_name)
		LEFT JOIN {trading_partner_division} AS t
			ON u2.primary_division_code = t.division_number
		LEFT JOIN {employee} AS e
			ON LOWER(u1.upn) = LOWER(e.email)
		LEFT JOIN {employee} AS e1
			ON LOWER(u1.manager_email) = LOWER(e1.email)
		LEFT JOIN {branch} AS b
			ON u2.primary_division_code = b.branch_code
		LEFT JOIN {talent_users} tu
    		ON LOWER(u1.email) = LOWER(tu.email)
		LEFT JOIN {bronze_user_list} bul
			ON LOWER(u1.email) = LOWER(bul.email)
	UNION ALL

	select 
		concat(h.dos_email,'|',h.house_account_email) as primary_key
		,h.dos_email as upn
		,u1.first_name
		,u1.last_name
		,u1.full_name
		,lower(h.dos_email) as email
		,'undefined' as phone_number
		,'undefined' as job_title
		,u2.country
		,u2.company
		,u2.department
		,u2.functional_group
		,u2.subgroup
		,'undefined' as manager
		,'undefined' as manager_email
		,'undefined' as created_date
		,'undefined' as disabled_date
		,CASE u1.status WHEN 1 THEN 'Active' WHEN 0 THEN 'Inactive' END AS status
		,COALESCE(u1.primary_division_code, 'undefined') AS primary_division_code
		,COALESCE(t.division_name, 'undefined') AS tms_3g_division_name
		,COALESCE(b.branch_name, 'undefined') AS netsuite_branch_name
		,COALESCE(b.subsidiary_name, 'undefined') AS subsidiary_name
		,'undefined' AS home_currency
		,COALESCE(u1.status, 'undefined') AS tms_3g_status
		,u1.user_3g_id AS tms_3g_userid
		,'undefined' AS netsuite_status
		,'undefined' AS position_id
		,'undefined' AS manager_position_id
		,'undefined' AS employee_type
		,'undefined' AS office_name
		,'1900-01-01' AS adp_disabled_date
		,'undefined' AS netsuite_job_title
		,'undefined' AS netsuite_manager

		,'undefined' AS adp_payroll_company_code
		,'undefined' AS adp_file_number
		,'undefined' AS adp_position_id
		,'undefined' AS adp_first_name
		,'undefined' AS adp_last_name
		,'undefined' AS adp_middle_initial
		,'undefined' AS adp_job_title
		,'undefined' AS adp_home_department_code
		,'undefined' AS adp_department_description
		,'undefined' AS adp_position_status
		,0 AS adp_is_enabled
		,'1900-01-01' AS adp_termination_date
		,0 AS adp_is_loa
		,'1900-01-01' AS adp_loa_start_date
		,'1900-01-01' AS adp_hire_date
		,0 as days_of_service
		,'undefined' AS adp_email
		,'undefined' AS adp_user_fullname
		,'undefined' AS adp_manager
		,'undefined' AS adp_job_title_code

		,0 AS flag_missing_country
		,0 AS flag_missing_department
		,0 AS flag_missing_office_name
		,0 AS flag_missing_manager
		,0 AS flag_invalid_employee_type
		,0 AS flag_missing_job_title
		,0 AS flag_mismatch_manager_adp_to_azure
		,0 AS flag_mismatch_job_title_adp_to_azure
		,0 AS flag_mismatch_manager_netsuite_to_azure
		,0 AS flag_mismatch_job_title_netsuite_to_azure
		,1 AS RN

	from {house_accounts} h
	left join {user_3g} u1
		on LOWER(u1.user_name) = h.dos_email 
	left join {user_list} u2
		on LOWER(u2.upn) = h.house_account_email
	left join {trading_partner_division} AS t
		on u1.primary_division_code = t.division_number
	left join {branch} AS b
		on u1.primary_division_code = b.branch_code
	
)
SELECT
	-1 AS user_key,
	'undefined' AS primary_key,
	'undefined' AS upn,
	'undefined' AS first_name,
	'undefined' AS last_name,
	'undefined' AS full_name,
	'undefined' AS email,
	'undefined' AS phone_number,
	'undefined' AS job_title,
	'undefined' AS country,
	'undefined' AS company,
	'undefined' AS department,
	'undefined' AS functional_group,
	'undefined' AS subgroup,
	'undefined' AS manager, 
	'undefined' AS manager_email,
	'1900-01-01' AS created_date,
	'1900-01-01' AS disabled_date,
	'undefined' AS status,
	'undefined' AS primary_division_code,
	'undefined' AS tms_3g_division_name,
	'undefined' AS netsuite_branch_name,
	'undefined' AS subsidiary_name,
	'undefined' AS home_currency,
	'undefined' AS tms_3g_status,
	'undefined' AS tms_3g_userid,
	'undefined' AS netsuite_status,
	'undefined' AS position_id,
	'undefined' AS manager_position_id,
	'undefined' AS employee_type,
	'undefined' AS office_name,
	'1900-01-01' AS adp_disabled_date,
	'undefined' AS netsuite_job_title,
	'undefined' AS netsuite_manager,
	'undefined' AS adp_payroll_company_code,
	'undefined' AS adp_file_number,
	'undefined' AS adp_position_id,
	'undefined' AS adp_first_name,
	'undefined' AS adp_last_name,
	'undefined' AS adp_middle_initial,
	'undefined' AS adp_job_title,
	'undefined' AS adp_home_department_code,
	'undefined' AS adp_department_description,
	'undefined' AS adp_position_status,
	0 AS adp_is_enabled,
	'1900-01-01' AS adp_termination_date,
	0 AS adp_is_loa,
	'1900-01-01' AS adp_loa_start_date,
	'1900-01-01' AS adp_hire_date,
	0 AS days_of_service,
	'undefined' AS adp_email,
	'undefined' AS adp_user_fullname,
	'undefined' AS adp_manager,
	'undefined' AS adp_job_title_code,
	0 AS flag_missing_country,
	0 AS flag_missing_department,
	0 AS flag_missing_office_name,
	0 AS flag_missing_manager,
	0 AS flag_invalid_employee_type,
	0 AS flag_missing_job_title,
	0 AS flag_mismatch_manager_adp_to_azure,
	0 AS flag_mismatch_job_title_adp_to_azure,
	0 AS flag_mismatch_manager_netsuite_to_azure,
	0 AS flag_mismatch_job_title_netsuite_to_azure,
	TO_TIMESTAMP(current_timestamp(), 'M/d/y h:m:ss a') AS __sys_synced,
    CAST(0 AS BOOLEAN) AS __sys_deleted
UNION
SELECT
	ROW_NUMBER() OVER(ORDER BY upn, created_date) AS user_key,
	primary_key,
	upn,
	first_name,
	last_name,
	full_name,
	email,
	phone_number,
	job_title,
	country,
	company,
	department,
	functional_group,
	subgroup,
	manager, 
	manager_email,
	created_date,
	disabled_date,
	status,
	primary_division_code,
	tms_3g_division_name,
	netsuite_branch_name,
	subsidiary_name,
	home_currency,
	tms_3g_status,
	tms_3g_userid,
	netsuite_status
	,position_id
	,manager_position_id,
	employee_type,
	office_name,
	adp_disabled_date,
	netsuite_job_title,
	netsuite_manager,
	adp_payroll_company_code,
	adp_file_number,
	adp_position_id,
	adp_first_name,
	adp_last_name,
	adp_middle_initial,
	adp_job_title,
	adp_home_department_code,
	adp_department_description,
	adp_position_status,
	adp_is_enabled,
	adp_termination_date,
	adp_is_loa,
	adp_loa_start_date,
	adp_hire_date,
	days_of_service,
	adp_email,
	adp_user_fullname,
	adp_manager,
	adp_job_title_code,
	flag_missing_country,
	flag_missing_department,
	flag_missing_office_name,
	flag_missing_manager,
	flag_invalid_employee_type,
	flag_missing_job_title,
	flag_mismatch_manager_adp_to_azure,
	flag_mismatch_job_title_adp_to_azure,
	flag_mismatch_manager_netsuite_to_azure,
	flag_mismatch_job_title_netsuite_to_azure,
	TO_TIMESTAMP(current_timestamp(), 'M/d/y h:m:ss a') AS __sys_synced,

    CAST(0 AS BOOLEAN) AS __sys_deleted
FROM user_cte
WHERE RN = 1
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

user_list=spark.read.load(production_lakehouse_silver_abfss+path_source_office365+'user_list')
user_3g=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'user_3g')
trading_partner_division=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'trading_partner_division')
employee=spark.read.load(production_lakehouse_silver_abfss+path_source_netsuite+'employee')
branch=spark.read.load(production_lakehouse_silver_abfss+path_source_netsuite+'branch')
house_accounts=spark.read.load(production_lakehouse_silver_abfss+path_source_lookup+'house_accounts_email_dos_mapping')
talent_users=spark.read.load(production_lakehouse_gold_abfss+path_source_hr+'talent_users')
bronze_user_list=spark.read.load(production_lakehouse_bronze_abfss+path_source_office365+'user_list')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Write_

# CELL ********************

print(f"{table_target} table full load started...")

# --------------------------------------------------
# ✅ Build target dataframe from SQL
# --------------------------------------------------
df_source = spark.sql(
    query_full,
    user_list=user_list,
    user_3g=user_3g,
    trading_partner_division=trading_partner_division,
    employee=employee,
    branch=branch,
    house_accounts=house_accounts,
    talent_users=talent_users,
    bronze_user_list=bronze_user_list
)

# --------------------------------------------------
# ✅ (Optional) Preserve SQL projection behavior
#     – safe to remove later if unnecessary
# --------------------------------------------------
df_source.createOrReplaceTempView(table_source)
df_source = spark.sql(f"SELECT * FROM {table_source}")
spark.catalog.dropTempView(table_source)

# --------------------------------------------------
# ✅ Write Gold table (Fabric-optimized, no partition)
# --------------------------------------------------
write_gold_table(
    df=df_source,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,        # ✅ DO NOT FORCE PARTITIONING
    zorder_cols=None,           # ✅ Optional; dimension table
    mode="overwrite",
    run_optimize=True
)

print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{table_target} table full load started...")

# create target table dataframe. make sure to modify parameters per target table
df_source=spark.sql(query_full
                    , user_list=user_list
                    , user_3g=user_3g
                    , trading_partner_division=trading_partner_division
                    , employee=employee
                    , branch=branch
                    , house_accounts=house_accounts
                    )

# convert dataframe to temporary view first in order to save it later as delta managed table
df_source.createOrReplaceTempView(f"{table_source}")
df_source=spark.sql(f"select * from {table_source}")
spark.catalog.dropTempView(f"{table_source}")

# save target dataframe
df_source.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{current_lakehouse_gold_abfss+path_target+table_target}")
print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ## Gold dim_load Table

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_load"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="primary_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

load=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'load')
shipment_load=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'shipment_load')
shipment_order_leg=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'shipment_order_leg')
order_leg=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'order_leg')
order_line=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'order_line')
organization=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'organization')
load_reference_number=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'load_reference_number')

# table sources added for service_lane_changed enhancement
mrt_load=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'mrt_load')
available_equipment=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'available_equipment')

order_header=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'order_header')
qualifier=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'qualifier')
load_status_activity=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'load_status_activity')
load_comment=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'load_comment')
load_tender=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'load_tender')
location=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'location')


load_status_activity=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'load_status_activity')
order_header=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'order_header')
load_tender=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'load_tender')
employee=spark.read.load(production_lakehouse_silver_abfss+path_source_netsuite+'employee')
stop=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'stop')



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## _Build base load dataframe_
# Capture fields needed from silver load table

# CELL ********************

query="""
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY load_number
            ORDER BY last_modified_date DESC
        ) AS rn
    FROM {load}
    WHERE __sys_deleted = 0
)
SELECT
    load_id AS primary_key,
    load_id,
    load_number,
    load_status,
    trading_partner_carrier_number AS carrier_number,
    trading_partner_carrier_name AS carrier_name,
    driver_number,
    driver_name,
    driver_phone,
    origin_address,
    origin_name, 
    origin_city,
    origin_state,
    origin_country,
    origin_postal_code as origin_zip,
    destination_address,
    destination_name,
    destination_city,
    destination_state,
    destination_country,
    destination_postal_code as destination_zip,
    origin_id,
    destination_id,
    pickup_date AS load_pickup_date,
    delivery_date_type AS load_delivery_date_type,
    delivery_date AS load_delivery_date,
    tender_date AS tender_date_temp,
    tender_method,
    organization_id,
    organization_name,
    service_option_name,
    contract_name,
    route_number,
    bol_number,
    pro_number,
    equipment_id,
    equipment_name,
    mode_name as mode,
    distance AS distance_temp,
    distance_unit AS distance_unit_temp,
    weight_net,
    weight_unit,
    volume_net,
    volume_unit,
    piece_count,
    handling_unit_count,
    primary_division_code AS primary_division_code_temp,
    primary_division_name AS primary_division_name_temp,
    alternative_division_code AS alternative_division_code_temp,
    alternative_division_name AS alternative_division_name_temp,
    booked_by_fullname,
    booked_by_username,
    created_by_fullname,
    created_by_username,
    cast(created_date as date) as load_created_date,
    equipment_number,
    concat(origin_city, ', ', origin_state, ' -> ', destination_city, ', ', destination_state) as lane_city_state,
    code_name AS rate_qualifier
FROM deduped
WHERE rn = 1
"""

load_df = spark.sql(query,load=load)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Enhance with following columns_
# 1. origin_city_state_country
# 2. destination_city_state_country
# 3. load_original_total_cost
# 4. load_currency
# 5. load_days_after_delivery
# 6. lane_state
# 7. lane_city
# 8. load_accepted_date


# CELL ********************

query = """
WITH deduped_load AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY load_number
            ORDER BY last_modified_date DESC
        ) AS rn
    FROM {load}
    WHERE __sys_deleted = 0
)
SELECT
    l.*,
    trim(concat_ws(',', trim(s.origin_city), trim(s.origin_state), trim(s.origin_country))) AS origin_city_state_country,
    trim(concat_ws(',', trim(s.destination_city), trim(s.destination_state), trim(s.destination_country))) AS destination_city_state_country,
    s.currency_net_cost AS load_original_total_cost,
    s.currency_code AS load_currency,
    CASE
        WHEN s.delivery_date IS NOT NULL THEN datediff(current_date(), CAST(s.delivery_date AS DATE))
        ELSE NULL
    END AS load_days_after_delivery,
    NULLIF(trim(concat_ws(' to ', s.origin_state, s.destination_state)), '') AS lane_state,
    NULLIF(trim(concat_ws(' to ', s.origin_city, s.destination_city)), '') AS lane_city,
    CASE
        WHEN s.tender_date IS NOT NULL THEN date_format(CAST(s.tender_date AS DATE), 'M/d/yyyy')
        ELSE NULL
    END AS load_accepted_date
FROM {load_df} AS l
INNER JOIN deduped_load AS s
    ON l.load_id = s.load_id
    AND s.rn = 1
"""

load_df = spark.sql(query, load=load, load_df=load_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Enhance with fields coming from  load_reference_number, load_comment, location table_

# CELL ********************

query = """
WITH deduped_load AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY load_number
            ORDER BY last_modified_date DESC
        ) AS rn
    FROM {load}
    WHERE __sys_deleted = 0
),
lrn_quote AS (
    SELECT
        lr.load_id,
        concat_ws(', ', sort_array(collect_set(lr.reference_number_value))) AS ref_vals
    FROM {load_reference_number} AS lr
    INNER JOIN {qualifier} AS q
        ON lr.qualifier_id = q.qualifier_id
        AND q.__sys_deleted = 0
    WHERE lr.__sys_deleted = 0
    GROUP BY lr.load_id
),
lrn_purchase AS (
    SELECT
        lr.load_id,
        concat_ws(', ', sort_array(collect_set(lr.reference_number_value))) AS ref_vals
    FROM {load_reference_number} AS lr
    INNER JOIN {qualifier} AS q
        ON lr.qualifier_id = q.qualifier_id
        AND q.__sys_deleted = 0
    WHERE lr.__sys_deleted = 0
    GROUP BY lr.load_id
),
lrn_department AS (
    SELECT
        lr.load_id,
        concat_ws(', ', sort_array(collect_set(lr.reference_number_value))) AS ref_vals
    FROM {load_reference_number} AS lr
    INNER JOIN {qualifier} AS q
        ON lr.qualifier_id = q.qualifier_id
        AND q.__sys_deleted = 0
    WHERE lr.__sys_deleted = 0
    GROUP BY lr.load_id
),
lrn_direction AS (
    SELECT
        lr.load_id,
        concat_ws(', ', sort_array(collect_set(lr.reference_number_value))) AS ref_vals
    FROM {load_reference_number} AS lr
    INNER JOIN {qualifier} AS q
        ON lr.qualifier_id = q.qualifier_id
        AND q.__sys_deleted = 0
    WHERE lr.__sys_deleted = 0
    GROUP BY lr.load_id
),
lrn_traffix_ltl AS (
    SELECT
        lr.load_id,
        concat_ws(', ', sort_array(collect_set(lr.reference_number_value))) AS ref_vals
    FROM {load_reference_number} AS lr
    WHERE lr.__sys_deleted = 0
        AND lr.qualifier_id = 112
    GROUP BY lr.load_id
),
billing_comment AS (
    SELECT
        lc.load_id,
        concat_ws(', ', sort_array(collect_set(lc.comment_value))) AS load_billing_comment
    FROM {load_comment} AS lc
    INNER JOIN {qualifier} AS q
        ON lc.qualifier_id = q.qualifier_id
        AND q.__sys_deleted = 0
    WHERE lc.__sys_deleted = 0
    GROUP BY lc.load_id
),
loc_nums AS (
    SELECT
        l.load_id,
        lo.location_number AS origin_loc_number,
        ld.location_number AS destination_loc_number
    FROM deduped_load AS l
    LEFT JOIN {location} AS lo
        ON l.origin_id = lo.location_id
        AND lo.__sys_deleted = 0
    LEFT JOIN {location} AS ld
        ON l.destination_id = ld.location_id
        AND ld.__sys_deleted = 0
    WHERE l.rn = 1
)
SELECT
    l.*,
    COALESCE(NULLIF(TRIM(lrq.ref_vals), ''), 'no quote ID') AS quote_id,
    bc.load_billing_comment,
    lrp.ref_vals AS load_purchase_number,
    ln.origin_loc_number,
    ln.destination_loc_number,
    lrd.ref_vals AS department_number,
    lrdi.ref_vals AS direction,
    ltl.ref_vals AS Traffix_LTL_load_number
FROM {load_df} AS l
LEFT JOIN lrn_quote AS lrq ON l.load_id = lrq.load_id
LEFT JOIN billing_comment AS bc ON l.load_id = bc.load_id
LEFT JOIN lrn_purchase AS lrp ON l.load_id = lrp.load_id
LEFT JOIN lrn_department AS lrd ON l.load_id = lrd.load_id
LEFT JOIN lrn_direction AS lrdi ON l.load_id = lrdi.load_id
LEFT JOIN lrn_traffix_ltl AS ltl ON l.load_id = ltl.load_id
LEFT JOIN loc_nums AS ln ON l.load_id = ln.load_id
"""

load_df = spark.sql(
    query,
    load=load,
    load_df=load_df,
    load_reference_number=load_reference_number,
    qualifier=qualifier,
    load_comment=load_comment,
    location=location,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Enhance with fields coming from order_header, load_tender, load_status_activity and shipment_load

# CELL ********************

query = """
WITH latest_status AS (
    SELECT
        load_id,
        TRIM(
            CONCAT_WS(
                ' ',
                initcap(TRIM(status_city)),
                initcap(TRIM(status_location))
            )
        ) AS load_last_location
    FROM (
        SELECT
            load_id,
            status_city,
            status_location,
            ROW_NUMBER() OVER (
                PARTITION BY load_id
                ORDER BY status_event_date DESC NULLS LAST
            ) AS rn
        FROM {load_status_activity}
        WHERE __sys_deleted = 0
    ) AS s
    WHERE rn = 1
),
early_dates AS (
    SELECT
        s.load_id,
        MIN(oh.early_pickup_date) AS load_early_pickup,
        MIN(oh.early_delivery_date) AS load_early_delivery
    FROM {shipment_load} AS s
    INNER JOIN {shipment_order_leg} AS sol
        ON s.shipment_id = sol.shipment_id
        AND s.__sys_deleted = 0
        AND sol.__sys_deleted = 0
    INNER JOIN {order_leg} AS ol
        ON sol.order_leg_id = ol.order_leg_id
        AND ol.__sys_deleted = 0
    INNER JOIN {order_header} AS oh
        ON ol.order_header_id = oh.order_header_id
        AND oh.__sys_deleted = 0
    GROUP BY s.load_id
),
latest_tender AS (
    SELECT
        load_id,
        rate_source
    FROM (
        SELECT
            load_id,
            rate_source,
            ROW_NUMBER() OVER (
                PARTITION BY load_id
                ORDER BY COALESCE(response_date, offer_date, created_date) DESC NULLS LAST
            ) AS rn
        FROM {load_tender}
        WHERE __sys_deleted = 0
    ) AS t
    WHERE rn = 1
),
ship_num AS (
    SELECT
        load_id,
        concat_ws(', ', sort_array(collect_set(CAST(shipment_number AS STRING)))) AS shipment_number
    FROM {shipment_load}
    WHERE __sys_deleted = 0
    GROUP BY load_id
)
SELECT
    l.*,
    ls.load_last_location,
    oe.load_early_pickup,
    oe.load_early_delivery,
    CASE
        WHEN oe.load_early_pickup IS NOT NULL OR oe.load_early_delivery IS NOT NULL THEN
            CONCAT_WS(
                ' - ',
                date_format(CAST(oe.load_early_pickup AS DATE), 'M/d/yyyy'),
                date_format(CAST(oe.load_early_delivery AS DATE), 'M/d/yyyy')
            )
        ELSE NULL
    END AS load_early_pickup_delivery,
    lt.rate_source,
    sn.shipment_number
FROM {load_df} AS l
LEFT JOIN latest_status AS ls ON l.load_id = ls.load_id
LEFT JOIN early_dates AS oe ON l.load_id = oe.load_id
LEFT JOIN latest_tender AS lt ON l.load_id = lt.load_id
LEFT JOIN ship_num AS sn ON l.load_id = sn.load_id
"""

load_df = spark.sql(
    query,
    load_df=load_df,
    load_status_activity=load_status_activity,
    shipment_load=shipment_load,
    shipment_order_leg=shipment_order_leg,
    order_leg=order_leg,
    order_header=order_header,
    load_tender=load_tender,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Enrich with handling unit total length_

# CELL ********************

query = """
SELECT
    l1.*,
    SUM(CEIL(COALESCE(o2.handling_unit_length_ft, 0), 3)) OVER(PARTITION BY s1.load_id) AS total_length,
    MAX(o1.trading_partner_client_name) OVER (PARTITION BY s1.load_id) AS customer_name, -- should be no duplicates
    MAX(o1.trading_partner_client_number) OVER (PARTITION BY s1.load_id) AS customer_number -- used for USPS NASS filter
FROM
    {shipment_load} AS s1
    INNER JOIN {shipment_order_leg} AS s2
        ON s1.shipment_id = s2.shipment_id
        AND s1.__sys_deleted = 0
        AND s2.__sys_deleted = 0
    INNER JOIN {order_leg} AS o1
        ON s2.order_leg_id = o1.order_leg_id
        AND o1.__sys_deleted = 0     
    INNER JOIN {order_line} AS o2
        ON o1.order_header_id = o2.order_header_id
        AND o2.__sys_deleted = 0  
    INNER JOIN {organization} AS o3
        ON s1.organization_id = o3.organization_id
        AND o3.organization_parent_id = 4
    RIGHT JOIN {load_df} AS l1
        ON s1.load_id = l1.load_id
"""

load_df = spark.sql(query, shipment_load=shipment_load, shipment_order_leg=shipment_order_leg, order_leg=order_leg, order_line=order_line, organization=organization, load_df=load_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Enrich with commodity, freight class, freight nmfc_

# CELL ********************

query = """
WITH raw AS (
    SELECT DISTINCT
        s1.load_id,
        o2.commodity_description,
        o2.freight_class,
        o2.nmfc_code                                                                                                    AS freight_nmfc_val,
        CASE WHEN o2.uom_temperature_min = 'F' THEN o2.temperature_min
             ELSE (o2.temperature_min * 1.8) + 32 END                                                                  AS temperature_min_far_val,
        CASE WHEN o2.uom_temperature_max = 'F' THEN o2.temperature_max
             ELSE (o2.temperature_max * 1.8) + 32 END                                                                  AS temperature_max_far_val,
        CASE WHEN o2.uom_temperature_min = 'F' THEN (o2.temperature_min - 32) * 5/9
             ELSE o2.temperature_min END                                                                                AS temperature_min_cel_val,
        CASE WHEN o2.uom_temperature_max = 'F' THEN (o2.temperature_max - 32) * 5/9
             ELSE o2.temperature_max END                                                                                AS temperature_max_cel_val
    FROM
        {shipment_load} AS s1
        INNER JOIN {shipment_order_leg} AS s2
            ON  s1.shipment_id  = s2.shipment_id
            AND s1.__sys_deleted = 0
            AND s2.__sys_deleted = 0
        INNER JOIN {order_leg} AS o1
            ON  s2.order_leg_id = o1.order_leg_id
            AND o1.__sys_deleted = 0
        INNER JOIN {order_line} AS o2
            ON  o1.order_header_id = o2.order_header_id
            AND o2.__sys_deleted  = 0
        INNER JOIN {organization} AS o3
            ON  s1.organization_id       = o3.organization_id
            AND o3.organization_parent_id = 4
),

commodity_agg AS (
    SELECT
        load_id,
        concat_ws(', ', collect_list(commodity_description))                         AS commodity_description,
        concat_ws(', ', collect_list(freight_class))                                 AS freight_class,
        concat_ws(', ', collect_list(freight_nmfc_val))                              AS freight_nmfc,
        concat_ws(', ', collect_list(CAST(ROUND(temperature_min_far_val, 2) AS STRING))) AS temperature_min_far,
        concat_ws(', ', collect_list(CAST(ROUND(temperature_max_far_val, 2) AS STRING))) AS temperature_max_far,
        concat_ws(', ', collect_list(CAST(ROUND(temperature_min_cel_val, 2) AS STRING))) AS temperature_min_cel,
        concat_ws(', ', collect_list(CAST(ROUND(temperature_max_cel_val, 2) AS STRING))) AS temperature_max_cel
    FROM raw
    GROUP BY load_id
)

SELECT
    l1.*,
    ca.commodity_description,
    ca.freight_class,
    ca.freight_nmfc,
    ca.temperature_min_far,
    ca.temperature_max_far,
    ca.temperature_min_cel,
    ca.temperature_max_cel
FROM
    {load_df}    AS l1
    LEFT JOIN commodity_agg AS ca
        ON l1.load_id = ca.load_id
"""

load_df = spark.sql(
    query,
    shipment_load=shipment_load,
    shipment_order_leg=shipment_order_leg,
    order_leg=order_leg,
    order_line=order_line,
    organization=organization,
    load_df=load_df
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Enrich with order load list_

# CELL ********************

query = """

with base as (
    select distinct
        s1.load_id,
        o2.order_header_id,
        o2.order_number
    from {shipment_load} s1
    inner join {shipment_order_leg} s2
        on s1.shipment_id = s2.shipment_id
        and s1.__sys_deleted = 0
        and s2.__sys_deleted = 0
    inner join {order_leg} o1
        on s2.order_leg_id = o1.order_leg_id
        and o1.__sys_deleted = 0
    inner join {order_line} o2
        on o1.order_header_id = o2.order_header_id
        and o2.__sys_deleted = 0
),

numbered as (

    select
        load_id,
        order_header_id,
         order_number,
        row_number() over (
            partition by load_id
            order by order_header_id
        ) as rn
    from base
),

final as (

    select
        *,
        concat(
            cast(load_id as string),
            '_',
            cast(order_header_id as string),
            '_',
            cast(rn as string)
        ) as order_string,
        concat(
           cast(order_number as string)
        ) as order_number_string
    from numbered
)

select distinct
    l1.*,
    concat_ws(
        ';',
        collect_list(order_string) over (partition by list.load_id)
    ) as load_order_list,
     concat_ws(
        ';',
        collect_list(order_number_string) over (partition by list.load_id)
    ) as order_number_list
from final list
    RIGHT JOIN {load_df} AS l1
        ON list.load_id = l1.load_id

"""



load_df = spark.sql(
    query, 
    shipment_load=shipment_load, 
    shipment_order_leg=shipment_order_leg, 
    order_leg=order_leg, 
    order_line=order_line,     
    load_df=load_df
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Enrich with intracompany load_

# CELL ********************

query = """
SELECT
    l4.*,
    CAST(CASE WHEN l1.load_number IS NOT NULL THEN 1 ELSE 0 END AS BOOLEAN) AS intra_company_load_flag,
    CASE WHEN l1.load_number IS NOT NULL AND l4.primary_division_name_temp IS NULL AND l4.organization_id = 7 THEN 'LTL' ELSE l4.primary_division_code_temp END AS primary_division_code,
    CASE WHEN l1.load_number IS NOT NULL AND l4.primary_division_name_temp IS NULL AND l4.organization_id = 7 THEN 'Digital LTL' ELSE l4.primary_division_name_temp END AS primary_division_name,
    CASE WHEN l1.load_number IS NOT NULL THEN 'LTL' ELSE l4.alternative_division_code_temp END AS alternative_division_code, 
    CASE WHEN l1.load_number IS NOT NULL THEN 'Digital LTL' ELSE l4.alternative_division_name_temp END AS alternative_division_name, 
    CASE WHEN COALESCE(l4.distance_temp, 0) = 0 THEN COALESCE(l1.distance, 0) ELSE COALESCE(l4.distance_temp, 0) END AS distance,
    CASE WHEN COALESCE(l4.distance_temp, 0) = 0 THEN l1.distance_unit ELSE l4.distance_unit_temp END AS distance_unit,
    CASE WHEN l1.load_number IS NOT NULL THEN l1.tender_date ELSE l4.tender_date_temp END AS tender_date 
FROM 
    {load} AS l1
    INNER JOIN {load_reference_number} AS l2
        ON l1.load_id = l2.load_id
        AND l1.organization_id = 7
        AND TRIM(l1.load_status) NOT IN ('Canceled', 'Canceled with AR Charge')
        AND l1.__sys_deleted = 0
        AND l2.qualifier_id = 110 -- logistics load number
        AND l2.__sys_deleted = 0           
    INNER JOIN  {load_reference_number} AS l3
        ON l1.load_id = l3.load_id
        AND l3.qualifier_id = 112 -- interacompany y
        AND l3.__sys_deleted = 0
    RIGHT JOIN {load_df} AS l4
        ON l4.load_number = l2.reference_number_value    
"""

load_df = spark.sql(query, load=load, load_reference_number=load_reference_number, load_df=load_df)
load_df = load_df.drop('primary_division_code_temp', 'primary_division_name_temp', 'alternative_division_code_temp', 'alternative_division_name_temp', 'distance_temp', 'distance_unit_temp', 'tender_date_temp')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Enrich with Service Line_

# CELL ********************

query="""
WITH equip_cat AS (
    SELECT
        available_equipment_id,
        equipment_name,
        CASE
            WHEN equipment_name LIKE ANY (
                'Dry Van%', 'Van Or Reefer%', 'Partial Dry Van%', 'Team Van%', 'Plated Trailer%'
            ) THEN 'Van'
            WHEN equipment_name LIKE ANY (
                'Heated%', 'Reefer%', 'Partial Reefer%', 'Multi Temp%'
            ) THEN 'Reefer'
            WHEN equipment_name LIKE 'Container%' THEN 'Container'
            WHEN equipment_name LIKE 'Power Only%' THEN 'Power Only'
            ELSE 'Specialized'
        END AS equipment_category
    FROM {available_equipment}
    WHERE __sys_deleted = 0
),

sales_rep_expanded AS (
    SELECT
        mrt.load_id,
        TRIM(email) AS email
    FROM {mrt_load} mrt
    LATERAL VIEW explode(split(mrt.sales_rep_id, ',')) t AS email
    WHERE mrt.sales_rep_id IS NOT NULL
),

sales_rep_joined AS (
    SELECT
        sre.load_id,
        sre.email,
        CONCAT_WS(' ', e.first_name, e.last_name) AS full_name
    FROM sales_rep_expanded sre
    LEFT JOIN {employee} e
        ON UPPER(TRIM(e.email)) = UPPER(TRIM(sre.email))
),

sales_rep_agg AS (
    SELECT
        load_id,
        concat_ws(', ', collect_set(email)) AS sales_rep_email_list,
        concat_ws(', ', collect_set(full_name)) AS sales_rep_list
    FROM sales_rep_joined
    GROUP BY load_id
)

SELECT
    l.*,
    CASE
        WHEN mrt.organization_id IN (6, 7) AND TRIM(COALESCE(mrt.equipment, '')) = '' THEN 'No equipment'
        WHEN mrt.organization_id IN (6, 7) AND de.equipment_category = 'Power Only' THEN 'Power Only'
        WHEN (mrt.total_length >= 24 OR mrt.total_length = 0)
            AND mrt.organization_id = 6
            AND (de.equipment_category IN ('Van', 'Power Only'))
            AND mrt.service_option <> 'Warehouse Freight' THEN 'TL Brokerage'
        WHEN mrt.organization_id = 6 AND de.equipment_category = 'Specialized' THEN 'Open Deck'
        WHEN mrt.organization_id = 6 AND de.equipment_category = 'Reefer' THEN 'Refrigerated'
        WHEN mrt.organization_id = 6
            AND (
                de.equipment_category = 'Container'
                OR LOWER(de.equipment_name) = LOWER('chassis')
                OR mrt.service_option = 'Intermodal'
            ) THEN 'Port Services'
        WHEN mrt.organization_id = 6
            AND (de.equipment_category = 'Van' OR LOWER(mrt.equipment) LIKE LOWER('part%'))
            AND mrt.service_option <> 'Warehouse Freight' THEN 'LTL Brokerage'
        WHEN mrt.organization_id = 6
            AND (
                mrt.alternative_division_code = 'EX'
                OR (
                    LOWER(mrt.equipment) LIKE LOWER('hot%')
                    AND LOWER(mrt.equipment) LIKE LOWER('%straight%')
                    AND LOWER(mrt.equipment) LIKE LOWER('%sprinter%')
                )
            ) THEN 'Expedited'
        WHEN (
                mrt.organization_id = 6
                AND (
                    LOWER(mrt.equipment) LIKE LOWER('intermodal')
                    OR LOWER(mrt.service_option) = LOWER('intermodal')
                )
            )
            OR mrt.carrier_name = 'Traffix Intermodal' THEN 'Intermodal'
        WHEN LOWER(l.customer_name) LIKE LOWER('idexx%') THEN 'Courier'
        WHEN mrt.organization_id = 7
            AND (LOWER(l.created_by_username) NOT LIKE LOWER('%@traffix.com%')) THEN 'Managed LTL'
        WHEN mrt.organization_id = 7
            AND (
                LOWER(l.created_by_username) LIKE LOWER('%@traffix.com%')
                OR LOWER(l.created_by_username) LIKE LOWER('%@rating%')
            ) THEN 'Transactional LTL'
        WHEN mrt.service_option = 'Warehouse Freight' THEN 'Warehousing'
        ELSE 'Definition Error'
    END AS service_line,
    CASE 
        WHEN sr.sales_rep_email_list IS NULL OR TRIM(sr.sales_rep_email_list) = 'undefined' 
        THEN NULL 
        ELSE sr.sales_rep_email_list 
    END AS sales_rep_email_list,

    CASE 
        WHEN sr.sales_rep_email_list IS NULL OR TRIM(sr.sales_rep_email_list) = 'undefined' 
        THEN NULL 
        ELSE sr.sales_rep_list 
    END AS sales_rep_list
FROM {load_df} AS l
LEFT JOIN {mrt_load} AS mrt
    ON mrt.load_id = l.load_id
LEFT JOIN equip_cat AS de
    ON TRY_CAST(TRIM(CAST(COALESCE(mrt.equipment_id, l.equipment_id) AS STRING)) AS INT) = de.available_equipment_id
LEFT JOIN sales_rep_agg sr
    ON l.load_id = sr.load_id
"""

load_df = spark.sql(
    query,
    load_df=load_df,
    mrt_load=mrt_load,
    available_equipment=available_equipment,
    employee=employee
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Enrich with extra fields_

# CELL ********************

from pyspark.sql import functions as F
from decimal import Decimal

query = """
WITH first_stop AS (
    SELECT
        load_id,
        city_name                        AS first_pickup_city,
        state_code                       AS first_pickup_state,
        concat(city_name, ',', state_code) AS first_pickup_city_state,
        appointment_date                 AS first_pickup_appointment_date_time,
        actual_arrival                   AS first_pickup_actual_arrival_date_time,
        late_arrival                     AS first_pickup_late_arrival_date_time,
        actual_departure                 AS first_pickup_actual_departure_date_time
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY load_id ORDER BY stop_number ASC) AS rn
        FROM {stop}
        WHERE __sys_deleted = 0
    ) WHERE rn = 1
),
last_stop AS (
    SELECT
        load_id,
        city_name                          AS last_delivery_city,
        state_code                         AS last_delivery_state,
        concat(city_name, ',', state_code) AS last_delivery_city_state,
        appointment_date                   AS last_delivery_appointment_date_time,
        actual_arrival                     AS last_delivery_actual_arrival_date_time
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY load_id ORDER BY stop_number DESC) AS rn
        FROM {stop}
        WHERE __sys_deleted = 0
    ) WHERE rn = 1
)
SELECT
    l.*,
    -- First Pickup Location
    fs.first_pickup_city,
    fs.first_pickup_state,
    fs.first_pickup_city_state,
    -- Last Delivery Location
    ls.last_delivery_city,
    ls.last_delivery_state,
    ls.last_delivery_city_state,
    -- First Pickup DateTimes
    fs.first_pickup_appointment_date_time,
    fs.first_pickup_actual_arrival_date_time,
    fs.first_pickup_late_arrival_date_time,
    fs.first_pickup_actual_departure_date_time,
    -- Last Delivery DateTimes
    ls.last_delivery_appointment_date_time,
    ls.last_delivery_actual_arrival_date_time,
    -- First Pickup Arrival Status
    CASE
        WHEN fs.first_pickup_actual_arrival_date_time IS NULL THEN 'TBD'
        WHEN fs.first_pickup_actual_arrival_date_time
             <= fs.first_pickup_appointment_date_time + INTERVAL 15 MINUTES THEN 'Ontime'
        ELSE 'Late'
    END AS first_pickup_arrival_status,
    -- First Pickup Departure Status
    CASE
        WHEN fs.first_pickup_actual_departure_date_time IS NULL THEN 'TBD'
        WHEN fs.first_pickup_actual_departure_date_time
             <= fs.first_pickup_late_arrival_date_time + INTERVAL 15 MINUTES THEN 'Ontime'
        ELSE 'Late'
    END AS first_pickup_departure_status,
    -- Last Delivery Arrival Status
    CASE
        WHEN ls.last_delivery_actual_arrival_date_time IS NULL THEN 'TBD'
        WHEN ls.last_delivery_actual_arrival_date_time
             <= ls.last_delivery_appointment_date_time + INTERVAL 15 MINUTES THEN 'Ontime'
        ELSE 'Late'
    END AS last_delivery_arrival_status,
    -- USPS Origin NASS Code
    -- Filter: customer_number IN ('TC-343041', 'NS-396724') — trading_partner_client numbers,
    -- not carrier numbers. origin_loc_number must be non-null and NOT contain 'LOC'
    -- (LOC-prefixed numbers are internal Traffix locations).
    CASE
        WHEN l.customer_number IN ('TC-343041', 'NS-396724')
            AND l.origin_loc_number IS NOT NULL
            AND l.origin_loc_number NOT LIKE '%LOC%'
        THEN
            CASE
                WHEN LOCATE(' ', REVERSE(TRIM(l.origin_name))) = 0
                    THEN TRIM(l.origin_name)
                ELSE SUBSTRING(
                    l.origin_name,
                    LENGTH(TRIM(l.origin_name))
                        - LOCATE(' ', REVERSE(TRIM(l.origin_name))) + 2
                )
            END
        ELSE '-'
    END AS usps_origin_nass_code,
    -- USPS Destination NASS Code
    CASE
        WHEN l.customer_number IN ('TC-343041', 'NS-396724')
            AND l.destination_loc_number IS NOT NULL
            AND l.destination_loc_number NOT LIKE '%LOC%'
        THEN
            CASE
                WHEN LOCATE(' ', REVERSE(TRIM(l.destination_name))) = 0
                    THEN TRIM(l.destination_name)
                ELSE SUBSTRING(
                    l.destination_name,
                    LENGTH(TRIM(l.destination_name))
                        - LOCATE(' ', REVERSE(TRIM(l.destination_name))) + 2
                )
            END
        ELSE '-'
    END AS usps_destination_nass_code
FROM {load_df} AS l
LEFT JOIN first_stop AS fs ON l.load_id = fs.load_id
LEFT JOIN last_stop  AS ls ON l.load_id = ls.load_id
"""

load_df = spark.sql(query, load_df=load_df, stop=stop)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Add metadata and save to lakehouse_


# CELL ********************



# --------------------------------------
# Build dataset
# --------------------------------------
final_query = """
SELECT 
    ROW_NUMBER() OVER (ORDER BY load_id) AS load_key,
    l1.*,
    current_timestamp() AS __sys_synced,
    CAST(0 AS BOOLEAN) AS __sys_deleted
FROM {load_df} AS l1
"""

df_source = spark.sql(final_query, load_df=load_df)

# --------------------------------------------------
# Create Undefined Record using existing schema
# --------------------------------------------------

undefined_row = {}

for field in df_source.schema.fields:

    if isinstance(field.dataType, StringType):
        undefined_row[field.name] = "Undefined"

    elif isinstance(field.dataType, BooleanType):
        undefined_row[field.name] = False

    elif isinstance(field.dataType, DecimalType):
        scale = field.dataType.scale
        undefined_row[field.name] = Decimal("-1").quantize(
            Decimal("1." + ("0" * scale))
        )

    elif isinstance(field.dataType, (IntegerType, LongType, ShortType)):
        undefined_row[field.name] = -1

    elif isinstance(field.dataType, (FloatType, DoubleType)):
        undefined_row[field.name] = -1.0

    elif isinstance(field.dataType, DateType):
        undefined_row[field.name] = datetime(1900, 1, 1).date()

    elif isinstance(field.dataType, TimestampType):
        undefined_row[field.name] = datetime(1900, 1, 1)

    else:
        undefined_row[field.name] = None

# Keep system columns consistent
undefined_row["__sys_deleted"] = False
undefined_row["__sys_synced"] = datetime.now()

# Create row using EXACT schema
undefined_values = [
    undefined_row.get(field.name)
    for field in df_source.schema.fields
]

undefined_df = spark.createDataFrame(
    [tuple(undefined_values)],
    schema=df_source.schema
)

# Union Undefined row
df_source = undefined_df.unionByName(df_source)

# --------------------------------------
# Repartition
# --------------------------------------
df_source = df_source.repartition(20, "load_created_date")

# --------------------------------------
# ✅ CALL NEW FUNCTION (FIXED)
# --------------------------------------
write_gold_table(
    df=df_source,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,
    zorder_cols=["load_key"],
    mode="overwrite",
    run_optimize=True
)

print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold dim_date

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_date"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="date_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Build data frame and save to lakehouse_

# CELL ********************

import pandas as pd
import numpy as np
from pandas.tseries.offsets import DateOffset
from datetime import datetime

# ------------------------------------------------------------------
# Date Range
# ------------------------------------------------------------------
start_date = "2020-01-01"
end_date = "2050-12-31"

df = pd.DataFrame({"date": pd.date_range(start_date, end_date)})
df["date_key"] = df["date"].dt.strftime("%Y%m%d").astype(int)

# ------------------------------------------------------------------
# Current Anchors
# ------------------------------------------------------------------
today = pd.Timestamp.now().normalize()
current_year = today.year
current_month = today.month
current_quarter = today.quarter
current_week = today.isocalendar().week

# Fiscal anchors (July = Fiscal Month 1)
fiscal_shifted = df["date"] + DateOffset(months=6)
current_fiscal = today + DateOffset(months=6)

current_fy = current_fiscal.year
current_fq = current_fiscal.quarter
prev_fq_year = (current_fy - 1) if current_fq == 1 else current_fy
prev_fq = 4 if current_fq == 1 else current_fq - 1

# ------------------------------------------------------------------
# Core Date Attributes
# ------------------------------------------------------------------
df["date_long_format"] = df["date"].dt.strftime("%B %d, %Y")
df["date_iso_format"] = df["date"].dt.strftime("%Y-%m-%d")

df["day_of_week_name"] = df["date"].dt.day_name()
df["day_of_week_name_short"] = df["day_of_week_name"].str[:3]

df["month_name"] = df["date"].dt.month_name()
df["month_name_short"] = df["month_name"].str[:3]

df["year"] = df["date"].dt.year
df["quarter"] = df["date"].dt.quarter
df["month"] = df["date"].dt.month

df["year_quarter_name"] = df["year"].astype(str) + "Q" + df["quarter"].astype(str)

df["day_of_month"] = df["date"].dt.day
df["day_of_year"] = df["date"].dt.dayofyear
df["day_of_quarter"] = (df["date"].dt.dayofyear - 1) % 91 + 1

df["week_of_year"] = df["date"].dt.isocalendar().week.astype("int64")
df["week_of_quarter"] = (df["day_of_quarter"] - 1) // 7 + 1

df["month_of_quarter"] = (df["month"] - 1) % 3 + 1
df["date_year_month"] = df["date"].dt.strftime("%Y-%m")          # 2026-04
df["date_year_month_full"] = df["date"].dt.strftime("%Y-%b")     # 2026-Apr
df["date_year_month_sort"] = df["date"].dt.year * 100 + df["date"].dt.month

# ------------------------------------------------------------------
# Month / Week Boundaries
# ------------------------------------------------------------------
df["days_in_month"] = df["date"].dt.days_in_month
df["days_in_year"] = df["date"].dt.is_leap_year.astype(int) + 365
df["days_in_quarter"] = (df["date"] + pd.offsets.QuarterEnd(0)).dt.day

df["month_start_date"] = df["date"].values.astype("datetime64[M]")
df["month_end_date"] = df["month_start_date"] + pd.offsets.MonthEnd(1)

df["week_start_date"] = df["date"] - pd.to_timedelta(df["date"].dt.weekday, unit="D")
df["week_end_date"] = df["week_start_date"] + pd.Timedelta(days=6)

# ------------------------------------------------------------------
# Flags
# ------------------------------------------------------------------
df["first_day_of_month_flag"] = df["date"].dt.is_month_start.astype(int)
df["last_day_of_month_flag"] = df["date"].dt.is_month_end.astype(int)

df["is_today_flag"] = (df["date"] == today).astype(int)
df["is_current_week_flag"] = (
    (df["week_of_year"] == current_week) & (df["year"] == current_year)
).astype(int)

df["is_current_month_flag"] = (
    (df["month"] == current_month) & (df["year"] == current_year)
).astype(int)

df["is_current_quarter_flag"] = (
    (df["quarter"] == current_quarter) & (df["year"] == current_year)
).astype(int)

df["is_currentYear_flag"] = (df["year"] == current_year).astype(int)

df["is_weekend_flag"] = (df["date"].dt.weekday >= 5).astype(int)

# ------------------------------------------------------------------
# Relative Dates
# ------------------------------------------------------------------
df["previous_day"] = df["date"] - DateOffset(days=1)
df["previous_month_day"] = df["date"] - DateOffset(months=1)
df["previous_year_day"] = df["date"] - DateOffset(years=1)

df["next_day"] = df["date"] + DateOffset(days=1)
df["next_month_day"] = df["date"] + DateOffset(months=1)
df["next_year_day"] = df["date"] + DateOffset(years=1)

# ------------------------------------------------------------------
# Fiscal Attributes
# ------------------------------------------------------------------
df["fiscal_year"] = fiscal_shifted.dt.year
df["fiscal_quarter"] = fiscal_shifted.dt.quarter
df["fiscal_month"] = fiscal_shifted.dt.month

df["fiscal_month_of_quarter"] = (df["fiscal_month"] - 1) % 3 + 1
df["fiscal_day_of_month"] = fiscal_shifted.dt.day
df["fiscal_day_of_year"] = fiscal_shifted.dt.dayofyear
df["fiscal_day_of_quarter"] = (fiscal_shifted.dt.dayofyear - 1) % 91 + 1

df["fiscal_week_of_year"] = fiscal_shifted.dt.isocalendar().week.astype("int64")
df["fiscal_week_of_quarter"] = (df["fiscal_day_of_quarter"] - 1) // 7 + 1

df["fiscal_quarter_name"] = (
    df["fiscal_year"].astype(str) + "Q" + df["fiscal_quarter"].astype(str)
)

df["fiscal_year_month"] = fiscal_shifted.dt.strftime("%Y-%m")

# ------------------------------------------------------------------
# ✅ Default Values (WITH FUTURE BLANKING)
# ------------------------------------------------------------------
df["default_calendar_day"] = np.where(
    df["date"] > today,
    "",
    np.where(df["date"] == today, "Today", df["date"].dt.strftime("%Y-%m-%d"))
)

df["default_calendar_month"] = np.where(
    df["date"] > today.replace(day=1) + pd.offsets.MonthEnd(0),
    "",
    np.where(
        (df["year"] == current_year) & (df["month"] == current_month),
        "Current Month",
        df["date"].dt.strftime("%Y-%m"),
    )
)

df["default_fiscal_period"] = np.where(
    fiscal_shifted > current_fiscal.replace(day=1) + pd.offsets.MonthEnd(0),
    "",
    np.where(
        (fiscal_shifted.dt.year == current_fy) &
        (fiscal_shifted.dt.month == current_fiscal.month),
        "Current Month",
        fiscal_shifted.dt.strftime("%Y-%m"),
    )
)

# ------------------------------------------------------------------
# ✅ NEW — Default Fiscal Quarter
# ------------------------------------------------------------------
df["default_fiscal_quarter"] = np.where(
    (fiscal_shifted.dt.year == current_fy) &
    (fiscal_shifted.dt.quarter == current_fq),
    "Current",
    np.where(
        (fiscal_shifted.dt.year == prev_fq_year) &
        (fiscal_shifted.dt.quarter == prev_fq),
        "Previous",
        np.where(
            fiscal_shifted >
            (current_fiscal - DateOffset(months=((current_fq - 1) * 3))),
            "",
            df["fiscal_quarter_name"]
        )
    )
)

# ------------------------------------------------------------------
# System Columns
# ------------------------------------------------------------------
df["__sys_synced"] = pd.Timestamp.now()
df["__sys_deleted"] = 0

# ------------------------------------------------------------------
# Create Spark DataFrame
# ------------------------------------------------------------------
df_source = spark.createDataFrame(df)

# ------------------------------------------------------------------
# Undefined Record (TIX-30219: default_* use computed values for seed date)
# ------------------------------------------------------------------
seed_date = pd.Timestamp("1900-01-01")
seed_fiscal = seed_date + DateOffset(months=6)

if seed_date > today:
    seed_default_calendar_day = ""
elif seed_date == today:
    seed_default_calendar_day = "Today"
else:
    seed_default_calendar_day = seed_date.strftime("%Y-%m-%d")

if seed_date > today.replace(day=1) + pd.offsets.MonthEnd(0):
    seed_default_calendar_month = ""
elif (seed_date.year == current_year) and (seed_date.month == current_month):
    seed_default_calendar_month = "Current Month"
else:
    seed_default_calendar_month = seed_date.strftime("%Y-%m")

if seed_fiscal > current_fiscal.replace(day=1) + pd.offsets.MonthEnd(0):
    seed_default_fiscal_period = ""
elif (seed_fiscal.year == current_fy) and (seed_fiscal.month == current_fiscal.month):
    seed_default_fiscal_period = "Current Month"
else:
    seed_default_fiscal_period = seed_fiscal.strftime("%Y-%m")

seed_fiscal_quarter_name = f"{seed_fiscal.year}Q{seed_fiscal.quarter}"
if (seed_fiscal.year == current_fy) and (seed_fiscal.quarter == current_fq):
    seed_default_fiscal_quarter = "Current"
elif (seed_fiscal.year == prev_fq_year) and (seed_fiscal.quarter == prev_fq):
    seed_default_fiscal_quarter = "Previous"
elif seed_fiscal > (current_fiscal - DateOffset(months=((current_fq - 1) * 3))):
    seed_default_fiscal_quarter = ""
else:
    seed_default_fiscal_quarter = seed_fiscal_quarter_name

undefined_row = {
    "date": datetime(1900, 1, 1),
    "date_key": -1,

    "date_long_format": "Undefined",
    "date_iso_format": "Undefined",

    "day_of_week_name": "Undefined",
    "day_of_week_name_short": "Undefined",

    "month_name": "Undefined",
    "month_name_short": "Undefined",

    "year": -1,
    "quarter": -1,
    "month": -1,

    "year_quarter_name": "Undefined",

    "day_of_month": -1,
    "day_of_year": -1,
    "day_of_quarter": -1,

    "week_of_year": -1,
    "week_of_quarter": -1,

    "month_of_quarter": -1,

    "date_year_month": "Undefined",
    "date_year_month_full": "Undefined",
    "date_year_month_sort": -1,

    "days_in_month": -1,
    "days_in_year": -1,
    "days_in_quarter": -1,

    "month_start_date": datetime(1900, 1, 1),
    "month_end_date": datetime(1900, 1, 1),

    "week_start_date": datetime(1900, 1, 1),
    "week_end_date": datetime(1900, 1, 1),

    "first_day_of_month_flag": -1,
    "last_day_of_month_flag": -1,

    "is_today_flag": -1,
    "is_current_week_flag": -1,
    "is_current_month_flag": -1,
    "is_current_quarter_flag": -1,
    "is_currentYear_flag": -1,

    "is_weekend_flag": -1,

    "previous_day": datetime(1900, 1, 1),
    "previous_month_day": datetime(1900, 1, 1),
    "previous_year_day": datetime(1900, 1, 1),

    "next_day": datetime(1900, 1, 1),
    "next_month_day": datetime(1900, 1, 1),
    "next_year_day": datetime(1900, 1, 1),

    "fiscal_year": -1,
    "fiscal_quarter": -1,
    "fiscal_month": -1,

    "fiscal_month_of_quarter": -1,
    "fiscal_day_of_month": -1,
    "fiscal_day_of_year": -1,
    "fiscal_day_of_quarter": -1,

    "fiscal_week_of_year": -1,
    "fiscal_week_of_quarter": -1,

    "fiscal_quarter_name": "Undefined",
    "fiscal_year_month": "Undefined",

    "default_calendar_day": seed_default_calendar_day,
    "default_calendar_month": seed_default_calendar_month,
    "default_fiscal_period": seed_default_fiscal_period,
    "default_fiscal_quarter": seed_default_fiscal_quarter,

    "__sys_synced": datetime.now(),
    "__sys_deleted": 0
}

# ------------------------------------------------------------------
# Build Undefined row using EXACT schema from df_source
# ------------------------------------------------------------------
undefined_values = []

for field in df_source.schema.fields:
    undefined_values.append(undefined_row.get(field.name))

undefined_spark_df = spark.createDataFrame(
    [tuple(undefined_values)],
    schema=df_source.schema
)

# ------------------------------------------------------------------
# Union Undefined Row
# ------------------------------------------------------------------
df_source = undefined_spark_df.unionByName(df_source)

# ------------------------------------------------------------------
# Write Delta Table
# ------------------------------------------------------------------
df_source.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{current_lakehouse_gold_abfss}{path_target}{table_target}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold dim_presentation_date

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_presentation_date"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="date_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Build data frame and save to lakehouse_

# CELL ********************

import pandas as pd
import numpy as np
from pandas.tseries.offsets import DateOffset, BMonthEnd
from datetime import datetime
from pyspark.sql import functions as F

start_date = '2020-01-01'
end_date = '2050-12-31'

df = pd.DataFrame({"date": pd.date_range(start_date, end_date)})
df["date_key"] = df.date.dt.strftime('%Y%m%d').astype(int)

today = pd.Timestamp.now().normalize()
current_week = today.week
current_month = today.month
current_quarter = today.quarter
current_year = today.year

df["date_long_format"] = df.date.dt.strftime('%B %-d, %Y') 
df["date_iso_format"] = df.date.dt.strftime('%Y-%m-%d')
df["day_of_week_name"] = df.date.dt.day_name()
df["day_of_week_name_short"] = df.date.dt.day_name().str[:3]
df["month_name"] = df.date.dt.month_name()
df["month_name_short"] = df.date.dt.month_name().str[:3]
df["year_quarter_name"] = df.date.dt.year.astype(str) + 'Q' + df.date.dt.quarter.astype(str)
df["year"] = df.date.dt.year
df["day_of_month"] = df.date.dt.day
df["day_of_quarter"] = (df.date.dt.dayofyear - 1) % 91 + 1
df["day_of_year"] = df.date.dt.dayofyear
df["week_of_quarter"] = (df.date.dt.day - 1) // 7 + 1
df["week_of_year"] = df.date.dt.isocalendar().week.astype('int64')
df["month"] = df.date.dt.month
df["month_of_quarter"] = (df.date.dt.month - 1) % 3 + 1
df["quarter"] = df.date.dt.quarter
df["days_in_month"] = df.date.dt.days_in_month
df["days_in_quarter"] = (df.date + pd.offsets.QuarterEnd(1)).dt.day
df['days_in_year'] = df['date'].dt.is_leap_year+365
df['first_day_of_month_flag'] = (df['date'].dt.is_month_start).astype(int)
df['last_day_of_month_flag'] = (df['date'].dt.is_month_end).astype(int)
df['is_today_flag'] = np.where(df['date'].dt.date == today, 1, 0)
df['is_current_week_flag'] = np.where(df['date'].dt.isocalendar().week == current_week, 1, 0)
df['is_current_month_flag'] = np.where(df['date'].dt.month == current_month, 1, 0)
df['is_currentYear_flag'] = np.where(df['date'].dt.year == current_year, 1, 0)
df['is_current_quarter_flag'] = np.where(df['date'].dt.quarter == current_quarter, 1, 0)
df['previous_day'] = df['date'] - DateOffset(days=1)
df['previous_year_day'] = df['date'] - DateOffset(years=1)
df['previous_month_day'] = df['date'] - DateOffset(months=1)
df['next_day'] = df['date'] + DateOffset(days=1)
df['next_year_day'] = df['date'] + DateOffset(years=1)
df['next_month_day'] = df['date'] + DateOffset(months=1)
df["fiscal_year"] = (df.date+DateOffset(months=6)).dt.year
df["fiscal_day_of_month"] = (df.date+DateOffset(months=6)).dt.day
df["fiscal_day_of_quarter"] = ((df.date+DateOffset(months=6)).dt.dayofyear - 1) % 91 + 1
df["fiscal_day_of_year"] = (df.date+DateOffset(months=6)).dt.dayofyear
df["fiscal_week_of_quarter"] = ((df.date+DateOffset(months=6)).dt.day - 1) // 7 + 1
df["fiscal_week_of_year"] = (df.date+DateOffset(months=6)).dt.isocalendar().week.astype('int64')
df["fiscal_month"] = (df.date+DateOffset(months=6)).dt.month
df["fiscal_month_of_quarter"] = ((df.date+DateOffset(months=6)).dt.month - 1) % 3 + 1
df["fiscal_quarter"] = (df.date+DateOffset(months=6)).dt.quarter
df["date_year_month"] = df.date.dt.strftime('%Y-%m')              # 2026-01
df["date_year_month_full"] = df.date.dt.strftime('%Y-%b')         # 2026-Jan
df["date_year_month_sort"] = df.date.dt.year * 100 + df.date.dt.month  # 202601
df["month_start_date"] = df.date.values.astype('datetime64[M]')
df["month_end_date"] = df["month_start_date"] + pd.offsets.MonthEnd(1)
df["week_start_date"] = df.date - pd.to_timedelta(df.date.dt.weekday, unit='D')
df["week_end_date"] = df["week_start_date"] + pd.Timedelta(days=6)

# --- Fiscal Year-Month (using +6 month shift logic) ---
fiscal_shifted = df.date + DateOffset(months=6)
df["fiscal_year_month"] = fiscal_shifted.dt.strftime('%Y-%m')

# --- Default Calendar Month ---
df["default_calendar_month"] = np.where(
    (df.date.dt.year == current_year) & (df.date.dt.month == current_month),
    "Current Month",
    df.date.dt.strftime('%Y-%m')
)

# --- Default Fiscal Period ---
current_fiscal = today + DateOffset(months=6)

df["default_fiscal_period"] = np.where(
    (fiscal_shifted.dt.year == current_fiscal.year) &
    (fiscal_shifted.dt.month == current_fiscal.month),
    "Current Month",
    fiscal_shifted.dt.strftime('%Y-%m')
)

# --- Default Calendar Day ---
df["default_calendar_day"] = np.where(
    df["date"].dt.date == today.date(),
    "Today",
    df["date"].dt.strftime('%Y-%m-%d')
)

# --- Fiscal Quarter Name ---
df["fiscal_quarter_name"] = (
    fiscal_shifted.dt.year.astype(str) +
    "Q" +
    fiscal_shifted.dt.quarter.astype(str)
)

df["fiscal_quarter_name"] = df["fiscal_year"].astype(str) + 'Q' + df["fiscal_quarter"].astype(str)

df["__sys_synced"] = pd.Timestamp("now")
df["__sys_deleted"] = 0

# ------------------------------------------------------------------
# Create Spark DataFrame First
# ------------------------------------------------------------------
column_order = list(df.columns)

df_source = spark.createDataFrame(df).select(*column_order)

# ------------------------------------------------------------------
# Add Undefined Record Using Spark Schema
# ------------------------------------------------------------------

undefined_row = {
    "date": datetime(1900, 1, 1),
    "date_key": -1,

    "date_long_format": "Undefined",
    "date_iso_format": "Undefined",

    "day_of_week_name": "Undefined",
    "day_of_week_name_short": "Undefined",

    "month_name": "Undefined",
    "month_name_short": "Undefined",

    "year_quarter_name": "Undefined",

    "year": -1,
    "day_of_month": -1,
    "day_of_quarter": -1,
    "day_of_year": -1,
    "week_of_quarter": -1,
    "week_of_year": -1,
    "month": -1,
    "month_of_quarter": -1,
    "quarter": -1,

    "days_in_month": -1,
    "days_in_quarter": -1,
    "days_in_year": -1,

    "first_day_of_month_flag": -1,
    "last_day_of_month_flag": -1,

    "is_today_flag": -1,
    "is_current_week_flag": -1,
    "is_current_month_flag": -1,
    "is_currentYear_flag": -1,
    "is_current_quarter_flag": -1,

    "previous_day": datetime(1900, 1, 1),
    "previous_year_day": datetime(1900, 1, 1),
    "previous_month_day": datetime(1900, 1, 1),

    "next_day": datetime(1900, 1, 1),
    "next_year_day": datetime(1900, 1, 1),
    "next_month_day": datetime(1900, 1, 1),

    "fiscal_year": -1,
    "fiscal_day_of_month": -1,
    "fiscal_day_of_quarter": -1,
    "fiscal_day_of_year": -1,
    "fiscal_week_of_quarter": -1,
    "fiscal_week_of_year": -1,
    "fiscal_month": -1,
    "fiscal_month_of_quarter": -1,
    "fiscal_quarter": -1,

    "date_year_month": "Undefined",
    "date_year_month_full": "Undefined",
    "date_year_month_sort": -1,

    "month_start_date": datetime(1900, 1, 1),
    "month_end_date": datetime(1900, 1, 1),

    "week_start_date": datetime(1900, 1, 1),
    "week_end_date": datetime(1900, 1, 1),

    "fiscal_year_month": "Undefined",

    "default_calendar_month": "Undefined",
    "default_fiscal_period": "Undefined",
    "default_calendar_day": "Undefined",

    "fiscal_quarter_name": "Undefined",

    "__sys_synced": datetime.now(),
    "__sys_deleted": 0
}

# Build row in exact schema order
undefined_values = [
    undefined_row.get(field.name, None)
    for field in df_source.schema.fields
]

undefined_spark_df = spark.createDataFrame(
    [tuple(undefined_values)],
    schema=df_source.schema
)

df_source = undefined_spark_df.unionByName(df_source)

df_source.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "True") \
    .save(f"{current_lakehouse_gold_abfss+path_target+table_target}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold dim_time

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_time"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="time_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql import functions as F

# -----------------------------
# Config (adjust office hours here if needed)
# -----------------------------
office_start_hour = 8   # inclusive (08:00:00 ...)
office_end_hour   = 17  # exclusive (... < 17:00:00)

# -----------------------------
# Build a 0..86399 seconds-of-day frame in pandas
# -----------------------------
seconds_in_day = 24 * 60 * 60  # 86400
s = pd.Series(np.arange(seconds_in_day), name="second_of_day")

# Break into H/M/S
hour = (s // 3600).astype(int)
minute = ((s % 3600) // 60).astype(int)
second = (s % 60).astype(int)

# 24h padded strings
hh24 = hour.map("{:02d}".format)
mm   = minute.map("{:02d}".format)
ss   = second.map("{:02d}".format)

# 24h displays (also the natural PK string)
time_military_display = hh24 + ":" + mm + ":" + ss
time_military_display_minute = hh24 + ":" + mm
time_24 = time_military_display  # "time" column per your spec

# am/pm and 12h hour
am_pm = np.where(hour >= 12, "pm", "am")
hour12_vals = (hour % 12)
hour12_vals = np.where(hour12_vals == 0, 12, hour12_vals)  # 0 -> 12

# 12h display strings
time_display = pd.Series(hour12_vals).astype(str) + ":" + mm + ":" + ss + " " + am_pm
time_display_minute = pd.Series(hour12_vals).astype(str) + ":" + mm + " " + am_pm

# Surrogate key (int HHMMSS)
time_key = (hour * 10000 + minute * 100 + second).astype(int)

# Office hours flag (Yes for 08:00:00 <= t < 17:00:00)
office_hours_flag = np.where((hour >= office_start_hour) & (hour < office_end_hour), "Yes", "No")

# Assemble pandas DataFrame
df = pd.DataFrame({
    "time_key": time_key,
    "time": time_24,  # "HH:MM:SS" (24h)
    "time_display": time_display,  # "h:mm:ss am/pm" (no leading zero on hour)
    "time_display_minute": time_display_minute,  # "h:mm am/pm"
    "time_military_display": time_military_display,  # "HH:MM:SS"
    "time_military_display_minute": time_military_display_minute,  # "HH:MM"
    "hour_of_day": hour.astype(int),            # 0..23
    "minute_of_hour": minute.astype(int),       # 0..59
    "second_of_minute": second.astype(int),     # 0..59
    "am_pm": am_pm,
    "office_hours_flag": office_hours_flag,
    "__sys_synced": pd.Timestamp("now"),
    "__sys_deleted": 0
})

# -----------------------------
# Create Spark DataFrame First
# -----------------------------
column_order = list(df.columns)

df_source = spark.createDataFrame(df).select(*column_order)

# -----------------------------
# Add Undefined Record Using Spark Schema
# -----------------------------

undefined_row = {
    "time_key": -1,
    "time": "Undefined",
    "time_display": "Undefined",
    "time_display_minute": "Undefined",
    "time_military_display": "Undefined",
    "time_military_display_minute": "Undefined",

    "hour_of_day": -1,
    "minute_of_hour": -1,
    "second_of_minute": -1,

    "am_pm": "Undefined",
    "office_hours_flag": "Undefined",

    "__sys_synced": datetime.now(),
    "__sys_deleted": 0
}

# Build row using EXACT schema order
undefined_values = [
    undefined_row.get(field.name)
    for field in df_source.schema.fields
]

undefined_spark_df = spark.createDataFrame(
    [tuple(undefined_values)],
    schema=df_source.schema
)

# Union Undefined row
df_source = undefined_spark_df.unionByName(df_source)

# Write as Delta to the same ABFSS pattern as your dim_date
df_source.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "True") \
    .save(f"{current_lakehouse_gold_abfss + path_target + table_target}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold dim_job_function

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_job_function"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="date_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

job_function=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'job_function')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Query Full_

# CELL ********************

query_full= """
SELECT
    -1 AS job_function_key,
    'undefined' AS primary_key,
    'undefined' AS job_function_display,
    'undefined' AS job_function_3g,
    'undefined' AS job_family,
    CAST(0 AS BOOLEAN) AS tami_flag,
    TO_TIMESTAMP(current_timestamp(), 'M/d/y h:m:ss a') AS __sys_synced,
    CAST(0 AS BOOLEAN) AS __sys_deleted

UNION
SELECT 
    ROW_NUMBER() OVER (ORDER BY jf.job_function_name) AS job_function_key,
    CONCAT_WS('|', jf.job_function_name, COALESCE(jd.job_display_name,'')) AS primary_key, 
    COALESCE(jd.job_display_name, REGEXP_REPLACE(jf.job_function_name, '[(]TAMI ON[)]|[(]TAMI OFF[)]', '')) AS job_function_display,
    jf.job_function_name AS job_function_3g,
    CASE jf.job_function_name
        WHEN 'Sales Rep' THEN 'Customer Sales'
        WHEN 'Carrier Sales Rep' THEN 'Customer Sales'
        WHEN 'Responsible Tracker' THEN 'Operations'
        WHEN 'Traffix Billing Rep' THEN 'Finance'
        WHEN 'AR Rep' THEN 'Finance'
        WHEN 'AP Rep' THEN 'Finance'
        WHEN 'Credit Rep' THEN 'Finance'
        WHEN 'After Hours Support' THEN 'Operations'
        WHEN 'Order Entry' THEN 'Operations'
        WHEN 'Bond Provider' THEN 'Finance'
        WHEN 'Factor' THEN 'Finance'
        WHEN 'Team Account Manager (TAMI ON)'  THEN 'Customer Sales'
        WHEN 'Backup Account Manager (TAMI ON)' THEN 'Customer Sales'
        WHEN 'Backup Account Manager (TAMI OFF)' THEN 'Customer Sales'
        WHEN 'Logistics Coordinator (TAMI ON)' THEN 'Operations'
        WHEN 'Logistics Coordinator (TAMI OFF)' THEN 'Operations'
        WHEN 'Primary Account Manager (TAMI ON)' THEN 'Customer Sales'
        ELSE 'Other'
    END AS job_family,
    CASE 
        WHEN jf.job_function_name LIKE '%TAMI ON%' THEN CAST(1 AS BOOLEAN)
        ELSE CAST(0 AS BOOLEAN)
    END  AS tami_flag,
    TO_TIMESTAMP(current_timestamp(), 'M/d/y h:m:ss a') AS __sys_synced,
    CAST(0 AS BOOLEAN) AS __sys_deleted
FROM
    {job_function} AS jf
    LEFT JOIN 
    (
        SELECT 
            'Primary Sales Rep' AS job_display_name,
            'Sales Rep' AS job_function_name
        UNION
        SELECT
            'Sales Rep' AS job_display_name,
            'Sales Rep' AS job_function_name
    ) AS jd
        ON jf.job_function_name = jd.job_function_name
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full write_

# CELL ********************

print(f"{table_target} table full load started...")

# --------------------------------------------------
# ✅ Build target dataframe from SQL
# --------------------------------------------------
df_source = spark.sql(
    query_full,
    job_function=job_function
)

# --------------------------------------------------
# ✅ Write Gold table (Fabric-optimized)
# --------------------------------------------------
write_gold_table(
    df=df_source,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,      # ✅ do NOT force partitioning
    zorder_cols=None,         # ✅ dimension table; skip unless needed
    mode="overwrite",
    run_optimize=True
)

print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{table_target} table full load started...")

# create target table dataframe. make sure to modify parameters per target table
df_source=spark.sql(query_full, job_function=job_function)

# convert dataframe to temporary view first in order to save it later as delta managed table
df_source.createOrReplaceTempView(f"{table_source}")
df_source=spark.sql(f"select * from {table_source}")
spark.catalog.dropTempView(f"{table_source}")

# save target dataframe
df_source.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{current_lakehouse_gold_abfss+path_target+table_target}")
print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold dim_Equipment

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_equipment"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="date_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Query Full_

# CELL ********************

query_full="""

Select 

ROW_NUMBER() OVER(ORDER BY primary_key) AS equipment_key,
equipment_id,
primary_key,
rated_equipment_flag,
equipment_name,
equipment_code_3g,
equipment_category,
max_weight,
max_base_weight,
max_volume,
max_base_volume,
max_pallets,
__sys_synced,
__sys_deleted

from
(

Select

'-1'                                as equipment_id,
'-1'||'|'|| NULL                    as primary_key,
NULL                                as rated_equipment_flag,
'Undefined'                         as equipment_name,
'Undefined'                         as equipment_code_3g,
'Undefined'                         as equipment_category,
0                                   as max_weight,
0                                   as max_base_weight,
0                                   as max_volume,
0                                   as max_base_volume,
0                                   as max_pallets,
to_timestamp(current_timestamp(),'M/d/y h:m:ss a') as __sys_synced,
cast(0 as boolean)                  as __sys_deleted

union

select 

e1.available_equipment_id           as equipment_id,
e1.primary_key||'|'||'N'            as primary_key,
'N'                                 as rated_equipment_flag,
e1.equipment_name                   as equipment_name,
e1.equipment_code                   as equipment_code_3g,
(case 
    when e1.equipment_name like any ('Dry Van%','Van Or Reefer%','Partial Dry Van%','Team Van%','Plated Trailer%')
    then 'Van'
    when e1.equipment_name like any ('Heated%','Reefer%','Partial Reefer%','Multi Temp%')
    then 'Reefer'
    when e1.equipment_name like 'Container%'
    then 'Container'
    when e1.equipment_name like 'Power Only%'
    then 'Power Only'
    else 'Specialized' end
)                                   as equipment_category,
e1.maximum_weight                   as max_weight,
e1.maximum_base_weight              as max_base_weight,
e1.maximum_volume                   as max_volume,
e1.maximum_base_volume              as max_base_volume,
e1.maximum_pallets                  as max_pallets,
to_timestamp(current_timestamp(),'M/d/y h:m:ss a') as __sys_synced,
cast(0 as boolean)                  as __sys_deleted

from {available_equipment} e1
where e1.__sys_deleted=0

Union

Select
e2.available_equipment_id           as equipment_id,
e2.primary_key||'|'||'Y'            as primary_key,
'Y'                                 as rated_equipment_flag,
e2.equipment_name                   as equipment_name,
e2.equipment_code                   as equipment_code_3g,
(case 
    when e2.equipment_name like any ('Dry Van%','Van Or Reefer%','Partial Dry Van%','Team Van%','Plated Trailer%')
    then 'Van'
    when e2.equipment_name like any ('Heated%','Reefer%','Partial Reefer%','Multi Temp%')
    then 'Reefer'
    when e2.equipment_name like 'Container%'
    then 'Container'
    when e2.equipment_name like 'Power Only%'
    then 'Power Only'
    else 'Specialized' end
)                                   as equipment_category,
e2.maximum_weight                   as max_weight,
e2.maximum_base_weight              as max_base_weight,
e2.maximum_volume                   as max_volume,
e2.maximum_base_volume              as max_base_volume,
e2.maximum_pallets                  as max_pallets,
to_timestamp(current_timestamp(),'M/d/y h:m:ss a') as __sys_synced,
cast(0 as boolean)                  as __sys_deleted

from {available_equipment} e2
where e2.__sys_deleted=0

)

"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

available_equipment=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'available_equipment')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Write_

# CELL ********************

print(f"{table_target} table full load started...")

# --------------------------------------------------
# ✅ Build target dataframe from SQL
# --------------------------------------------------
df_source = spark.sql(
    query_full,
    available_equipment=available_equipment
)

# --------------------------------------------------
# ✅ Write Gold table (Fabric-optimized)
# --------------------------------------------------
write_gold_table(
    df=df_source,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,          # ✅ Do NOT force partitioning
    zorder_cols=["primary_key"],  # ✅ Optional; remove if unneeded
    mode="overwrite",
    run_optimize=True
)

print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{table_target} table full load started...")

# create target table dataframe. make sure to modify parameters per target table
df_source=spark.sql(query_full, available_equipment=available_equipment)

# convert dataframe to temporary view first in order to save it later as delta managed table
df_source.createOrReplaceTempView(f"{table_source}")
df_source=spark.sql(f"select * from {table_source}")
spark.catalog.dropTempView(f"{table_source}")

# save target dataframe
df_source.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{current_lakehouse_gold_abfss+path_target+table_target}")
print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ## Gold dim_customer

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_customer"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="date_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Query Full_

# CELL ********************

query_full="""
WITH RankedTradingPartners AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY client_number
               ORDER BY created_by_date DESC
           ) AS rn
    FROM {trading_partner_client}
    WHERE __sys_deleted = 0
      AND is_active = 1
      AND created_by_date >= CAST({history_start_date} AS TIMESTAMP) 
),
CleanedTradingPartner AS (
    SELECT *
    FROM RankedTradingPartners
    WHERE rn = 1
),
last_two_order_dates_records as
(
    select
        trading_partner_client_number as customer_number,
        max(case when rn = 1 then created_date end) as latest_created_date,
        max(case when rn = 2 then created_date end) as second_latest_created_date,
        datediff(
            day,
            max(case when rn = 2 then created_date end),
            max(case when rn = 1 then created_date end)
        ) as gap_between_latest_orders,

        max(case when rn = 1 then created_by_username end) as latest_created_by_username,
        max(case when rn = 2 then created_by_username end) as second_latest_created_by_username,

        case
            when max(case when rn = 1 then created_by_username end) <>
                max(case when rn = 2 then created_by_username end)
            then 1
            else 0
        end as sales_rep_changed_from_most_recent_orders

    from (
        select
            trading_partner_client_number,
            created_date,
            created_by_username,
            row_number() over (
                partition by trading_partner_client_number
                order by created_date desc
            ) as rn
        from {order_header}
        where order_status not in ('Canceled', 'Unscheduled')        
    ) x
    where rn <= 2
    group by trading_partner_client_number
),
first_last_order as 
(
    SELECT 
    trading_partner_client_number as customer_number,
    cast(min(created_date) as date) as first_order,
    cast(max(created_date) as date) as last_order,
    datediff(
            day,
            cast(min(created_date) as date),
            cast(max(created_date) as date)
        ) as gap_between_first_and_last_order_from_one_year_to_now
    FROM {order_header}
    where order_status not in ('Canceled', 'Unscheduled')
    and created_date >= add_months(current_date(), -12)
    group by trading_partner_client_number
), 
sales_rep AS (
    SELECT concat(first_name,' ',last_name) AS primary_sales_rep,
           customer_number,email
    FROM {customer_salesteam}
    WHERE role = '1 - Sales Rep' AND primary = 'True'
),
primary_account_manager AS (
    SELECT concat_ws(', ', collect_list(concat(first_name,' ',last_name))) AS current_primary_account_manager_name,
           customer_number,email
    FROM {customer_salesteam}
    WHERE role IN ('2 - Primary Account Manager (TAMI ON)')
    GROUP BY customer_number,email
),
customer_sales_team AS (
    SELECT sr.customer_number,
           sr.primary_sales_rep,
           sr.email as primary_sales_rep_email,
           am.current_primary_account_manager_name,
           am.email as primary_account_manager_email
    FROM sales_rep sr
    LEFT JOIN primary_account_manager am ON am.customer_number = sr.customer_number
),
customer_biller_3gtms AS (
    -- First get all possible billers
    WITH raw_billers AS (
        SELECT
            tp.client_number AS customer_id,
            fullname AS biller_name,
            tpjf.created_date
        FROM
            {trading_partner_client} tp
        LEFT JOIN
            {trading_partner_job_function} tpjf ON tp.client_id = tpjf.trading_partner_id
        WHERE
             tpjf.job_function_id = 76  -- Biller job function
            AND tp.__sys_deleted = 0
            AND tpjf.__sys_deleted = 0 
            And tp.created_by_date >= CAST({history_start_date} AS TIMESTAMP)          
            AND tp.is_active = 1
    ),
    -- Then aggregate to get one per customer (most recent)
    unique_billers AS (
        SELECT 
            customer_id,
            MAX_BY(biller_name, created_date) AS biller_name
        FROM raw_billers
        GROUP BY customer_id
    )
    SELECT
        customer_id,
        INITCAP(biller_name) AS customer_biller,
        '3GTMS' AS source_system
    FROM unique_billers
),


customer_biller_tmw AS (
    SELECT
        customer_id,
        customer_biller,
        'TMW' AS source_system
    FROM {tmw_client} c 
),


customer_biller AS (
    SELECT 
        customer_id, 
        customer_biller
    FROM customer_biller_3gtms
    
    UNION ALL
    
    -- Only include TMW records that don't exist in 3GTMS
    SELECT 
        tmw.customer_id, 
        tmw.customer_biller
    FROM customer_biller_tmw tmw
    WHERE NOT EXISTS (
        SELECT 1 
        FROM customer_biller_3gtms tms 
        WHERE tms.customer_id = tmw.customer_id
    )
),
unioned_customers as (
SELECT
    c1.customer_number,
    c1.tmw_customer_number,
    c1.customer_name,
    c1.customer_type,
    c1.customer_status,
    c1.credit_status,
    c1.is_active,
    c1.billing_method,
    c1.created_date,
    c1.account_date,
    c1.closed_date,
    c1.postal_code,
    c1.city,
    c1.state,
    c1.country,
    c1.industry_name,
    c1.db_revenue,
    c1.db_transportation_spend,
    c1.db_employee_count,
    c1.db_primary_us_sic_code,
    c1.db_industry_name_ussicv4,
    c1.db_primary_business_name,
    c1.customer_collector as customer_collector,
    COALESCE(cst.primary_sales_rep, 'Undefined') AS primary_sales_rep_name,
    COALESCE(cst.primary_sales_rep_email, 'Undefined') AS primary_sales_rep_email,
    COALESCE(cst.current_primary_account_manager_name, 'Undefined') AS primary_account_manager_name,
    COALESCE(cst.primary_account_manager_email, 'Undefined') AS primary_account_manager_email,
    COALESCE(cb.customer_biller, 'Undefined') AS customer_biller,
    l1.last_follow_up_date AS last_comment_date,
    l1.last_follow_up_comment AS last_comment,
    c1.hyperlink_customer_id,

    flo.first_order,
    flo.last_order,

    case
        when datediff(current_date(), flo.last_order) <= 30 then 'active [<=30 days]'
        when datediff(current_date(), flo.last_order) <= 90 then 'passive [<=90 days]'
        else 'lost [>90 days]'
    end as retention_status,

    flo.gap_between_first_and_last_order_from_one_year_to_now,

    case when ltodr.gap_between_latest_orders > 90 then 'new' else 'existing' end as new_existing_customer,
    case when ltodr.gap_between_latest_orders > 180 then 'new' else 'existing' end as new_existing_customer_r6m,
    case when ltodr.gap_between_latest_orders > 365 then 'new' else 'existing' end as new_existing_customer_r12m,

    case when flo.gap_between_first_and_last_order_from_one_year_to_now > 90 then 'new' else 'existing' end as new_existing_customer_one_year_check,
    case when flo.gap_between_first_and_last_order_from_one_year_to_now > 180 then 'new' else 'existing' end as new_existing_customer_r6m_one_year_check,
    case when flo.gap_between_first_and_last_order_from_one_year_to_now > 365 then 'new' else 'existing' end as new_existing_customer_r12m_one_year_check,

    ltodr.latest_created_date,
    ltodr.second_latest_created_date,  
    ltodr.gap_between_latest_orders,

    ltodr.latest_created_by_username,
    ltodr.second_latest_created_by_username,
    ltodr.sales_rep_changed_from_most_recent_orders,

    TO_TIMESTAMP(current_timestamp(), 'M/d/y h:m:ss a') AS __sys_synced,
    CAST(0 AS BOOLEAN) AS __sys_deleted

FROM {customer} c1
LEFT JOIN customer_sales_team cst ON cst.customer_number = c1.customer_key
LEFT JOIN customer_biller cb ON UPPER(c1.customer_key) = cb.customer_id
--LEFT JOIN customer_invoicing_requirements cir ON UPPER(c1.customer_key) = cir.customer_id
LEFT JOIN {customer_last_follow_up} l1 ON l1.customer_id = c1.customer_id
left join first_last_order flo on flo.customer_number = c1.customer_number
left join last_two_order_dates_records ltodr on ltodr.customer_number = c1.customer_number

WHERE c1.__sys_deleted = 0

UNION ALL

-- TMS-only customers (cleaned)
SELECT 
    UPPER(t1.client_number) AS customer_number,
    NULL AS tmw_customer_number,
    INITCAP(t1.client_name) AS customer_name,
    'TMS Only' AS customer_type,
    (CASE WHEN t1.is_active = 1 THEN 'Active' ELSE 'Terminated' END) AS customer_status,
    NULL AS credit_status,
    CAST(t1.is_active = 0 AS BOOLEAN) AS is_active, 
    NULL AS billing_method,
    TO_DATE(t1.created_by_date) AS created_date,
    NULL AS account_date,
    NULL AS closed_date,
    NULL AS postal_code,
    NULL AS city,
    NULL AS state,
    NULL AS country,
    NULL AS industry_name,
    NULL AS db_revenue,
    NULL AS db_transportation_spend,
    NULL AS db_employee_count,
    NULL AS db_primary_us_sic_code,
    NULL AS db_industry_name_ussicv4,
    NULL AS db_primary_business_name,
    'Undefined' AS customer_collector,
    'Undefined' AS primary_sales_rep_name, 
    'Undefined' AS primary_sales_rep_email, 
    'Undefined' AS primary_account_manager_name, 
    'Undefined' AS primary_account_manager_email, 
    COALESCE(cb.customer_biller, 'Undefined') AS customer_biller,
    NULL AS last_comment_date,
    NULL AS last_comment,
    NULL AS hyperlink_customer_id,

    NULL as first_order,
    NULL as last_order,
    NULL as retention_status,

    NULL as gap_between_first_and_last_order_from_one_year_to_now,

    NULL as new_existing_customer,
    NULL as new_existing_customer_r6m,
    NULL as new_existing_customer_r12m,

    NULL as new_existing_customer_one_year_check,
    NULL as new_existing_customer_r6m_one_year_check,
    NULL as new_existing_customer_r12m_one_year_check,

    NULL as latest_created_date,
    NULL as second_latest_created_date,  
    NULL as gap_between_latest_orders,

    NULL as latest_created_by_username,
    NULL as second_latest_created_by_username,
    NULL as sales_rep_changed_from_most_recent_orders,

    TO_TIMESTAMP(current_timestamp(), 'M/d/y h:m:ss a') AS __sys_synced,
    CAST(0 AS BOOLEAN) AS __sys_deleted

FROM CleanedTradingPartner t1
LEFT JOIN customer_biller cb
    ON cb.customer_id = t1.client_id

WHERE NOT EXISTS (
    SELECT 1
    FROM {customer} c
    WHERE c.customer_number = UPPER(t1.client_number)
)



)
select 
    ROW_NUMBER() OVER (ORDER BY customer_number) AS customer_key,
    *
from unioned_customers

UNION ALL

Select

    -1                                as customer_key,
    -1                                as customer_number,
    NULL                              as tmw_customer_number,
    'Undefined'                         as customer_name,
    'Undefined'  as customer_type,
    NULL as customer_status,
    NULL AS credit_status,
    NULL AS is_active, 
    NULL AS billing_method,
    NULL AS created_date,
    NULL AS account_date,
    NULL AS closed_date,
    NULL AS postal_code,
    NULL AS city,
    NULL AS state,
    NULL AS country,
    NULL AS industry_name,
    NULL AS db_revenue,
    NULL AS db_transportation_spend,
    NULL AS db_employee_count,
    NULL AS db_primary_us_sic_code,
    NULL AS db_industry_name_ussicv4,
    NULL AS db_primary_business_name,
    'Undefined' AS customer_collector,
    'Undefined' AS primary_sales_rep_name, 
    'Undefined' AS primary_sales_rep_email, 
    'Undefined' AS primary_account_manager_name, 
    'Undefined' AS primary_account_manager_email, 

    'Undefined' AS customer_biller,
    NULL AS last_comment_date,
    NULL AS last_comment,
    NULL AS hyperlink_customer_id,

    NULL as first_order,
    NULL as last_order,
    NULL as retention_status,

    NULL as gap_between_first_and_last_order_from_one_year_to_now,

    NULL as new_existing_customer,
    NULL as new_existing_customer_r6m,
    NULL as new_existing_customer_r12m,

    NULL as new_existing_customer_one_year_check,
    NULL as new_existing_customer_r6m_one_year_check,
    NULL as new_existing_customer_r12m_one_year_check,

    NULL as latest_created_date,
    NULL as second_latest_created_date,  
    NULL as gap_between_latest_orders,

    NULL as latest_created_by_username,
    NULL as second_latest_created_by_username,
    NULL as sales_rep_changed_from_most_recent_orders,

    TO_TIMESTAMP(current_timestamp(), 'M/d/y h:m:ss a') AS __sys_synced,
    CAST(0 AS BOOLEAN) AS __sys_deleted
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

customer=spark.read.load(production_lakehouse_silver_abfss+path_source_netsuite+'customer')
customer_last_follow_up=spark.read.load(production_lakehouse_silver_abfss+path_source_netsuite+'customer_last_follow_up')
customer_salesteam=spark.read.load(production_lakehouse_gold_abfss+path_source_dimensions+'customer_salesteam')
trading_partner_client = spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'trading_partner_client')
trading_partner_job_function = spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'trading_partner_job_function')
tmw_client = spark.read.load(production_lakehouse_silver_abfss+path_source_truckmate+'client')

order_header = spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'order_header')
company_sales_people = spark.read.load(production_lakehouse_silver_abfss + path_source_hubspot + 'company_sales_people')
hubspot_companies = spark.read.load(production_lakehouse_silver_abfss + path_source_hubspot + 'companies')
house_accounts_map = spark.read.load(production_lakehouse_silver_abfss + path_source_lookup +"house_accounts_email_dos_mapping")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Enhanced: primary_sales_rep_name and primary_account_manager_name from hubspot_


# CELL ********************

from pyspark.sql import functions as F
df_source_base=spark.sql(query_full
                            ,customer=customer
                            ,customer_last_follow_up=customer_last_follow_up
                            ,customer_salesteam=customer_salesteam
                            ,trading_partner_client=trading_partner_client
                            ,trading_partner_job_function=trading_partner_job_function
                            ,tmw_client=tmw_client
                            ,order_header=order_header
                            ,history_start_date=history_start_date)

# enhance primary_sales_rep_name and primary_account_manager_name from HubSpot Silver
df_hubspot = company_sales_people.filter(F.col("__sys_deleted") == False).select(
    F.col("customer_number"),
    F.col("primary_sales_rep_name").alias("hs_primary_sales_rep_name"),
    F.col("primary_sales_rep_email").alias("hs_primary_sales_rep_email"),
    F.col("primary_account_manager_name").alias("hs_primary_account_manager_name")
)

df_source = df_source_base.join(df_hubspot, "customer_number", "left")

df_source = df_source.withColumn("primary_sales_rep_name",
                                    F.coalesce(F.col("hs_primary_sales_rep_name"), F.col("primary_sales_rep_name"))
                                ).withColumn("primary_sales_rep_email",
                                    F.coalesce(F.col("hs_primary_sales_rep_email"), F.col("hs_primary_sales_rep_email"))
                                ).withColumn("primary_account_manager_name",
                                    F.coalesce(F.col("hs_primary_account_manager_name"), F.col("primary_account_manager_name"))
                                ).drop("hs_primary_sales_rep_name","hs_primary_sales_rep_email", "hs_primary_account_manager_name")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_base_clean = (
    df_source_base
    .filter(
        F.col("primary_sales_rep_email").isNotNull() &
        (F.trim(F.col("primary_sales_rep_email")) != "")
    )
    .dropDuplicates(["customer_number"])
    .select(
        "customer_number",
        F.col("primary_sales_rep_email").alias("base_email"),
        F.col("primary_sales_rep_name").alias("base_name")
    )
)


df_enriched = df_source.join(
    df_base_clean,
    on="customer_number",
    how="left"
)

df_final = df_enriched \
    .withColumn(
        "primary_sales_rep_email",
        F.when(
            F.col("primary_sales_rep_email").isNull() |
            (F.trim(F.col("primary_sales_rep_email")) == ""),
            F.col("base_email")
        ).otherwise(F.col("primary_sales_rep_email"))
    ) \
    .withColumn(
        "primary_sales_rep_name",
        F.when(
            F.col("primary_sales_rep_name").isNull() |
            (F.trim(F.col("primary_sales_rep_name")) == ""),
            F.col("base_name")
        ).otherwise(F.col("primary_sales_rep_name"))
    )


df_final = df_final.drop("base_email", "base_name")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_final.filter(df_final.customer_number == 'NS-1551368')
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql import functions as F

df_missing = df_final.filter(
    F.col("primary_sales_rep_email").isNull() |
    (F.trim(F.col("primary_sales_rep_email")) == "")
)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# MARKDOWN ********************

# ### _Enhanced: hubspot companies with additional properties

# CELL ********************

from pyspark.sql import functions as F

df_hubspot_companies = (
    hubspot_companies
        .drop("hubspot_customer_address")
        .select(
            # Join key
            F.col("netsuite_customer_id").alias("customer_number"),

            # Addresses
            F.col("customer_address"),
            F.col("customer_billing_address"),

            # Cross-border & indicators
            F.col("cross_border_canada"),
            F.col("cross_border_mexico"),
            F.col("exporter_indicator"),
            F.col("fortune_1000_indicator"),
            F.col("company_exists_in_hubspot_indicator"),

            # Credit / risk
            F.col("assessment_credit_limit_rcc"),
            F.col("assessment_credit_limit_rct"),
            F.col("assessment_delinq_score_np"),
            F.col("assessment_failure_score_np"),
            F.col("assessment_std_rating_risk_sg"),

            # Financials
            F.col("sales_revenue_currency"),
            F.col("global_ultimate_asr_currency"),

            # Digital & social
            F.col("organization_telephone_num"),
            F.col("organization_website_addr_url"),
            F.col("website"),
            F.col("logo_url"),
            F.col("linkedin_handle"),
            F.col("linkedin_company_page"),
            F.col("facebook_company_page"),
            F.col("twitter_handle"),
            F.col("web_technologies"),

            # Analytics & lifecycle
            F.col("hs_analytics_source_data"),
            F.col("hs_date_entered_marketing_qualified_lead"),
            F.col("hs_date_entered_opportunity"),

            # Notes
            F.col("hubspot_notes_last_activity"),
            F.col("hubspot_notes_next_activity"),
            F.col("hubspot_notes_next_activity_type"),
            F.col("hubspot_comments"),

            # Ownership / hierarchy
            F.col("hubspot_parent_company_id"),
            F.col("hubspot_owner_id"),

            # Enrichment / classification
            F.col("employee_range"),
            F.col("naics_codes"),
            F.col("sic_codes"),
            F.col("time_zone"),
            F.col("enrich_status"),
            F.col("sec_checked_website_domain"),

            # Spend estimates
            F.col("est_ltl_spend"),
            F.col("est_na_transportation_spend"),
            F.col("est_spend_canada"),
            F.col("est_spend_mexico"),

            # Revenue estimate
            F.col("estimated_customer_revenue"),

            # Identifiers
            F.col("duns_number"),
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

from pyspark.sql import functions as F

df_joined = (
    df_final.alias("src")
    .join(
        df_hubspot_companies.alias("hs"),
        on="customer_number",
        how="left"
    )
)

match_stats = (
    df_joined
    .select(
        F.when(F.col("hs.customer_number").isNotNull(), "matched")
         .otherwise("not_matched")
         .alias("hubspot_match_status")
    )
    .groupBy("hubspot_match_status")
    .count()
)

match_stats.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_source = (
    df_final.alias("src")
    .join(
        df_hubspot_companies.alias("hs"),
        on="customer_number",
        how="left"
    )
    .select(

        F.col("src.customer_number").alias("primary_key"),
        # ✅ customer_number_sha256 key: SHA-256 hash of customer_number
        F.sha2(
            F.concat_ws(
                "||",
                F.lit(SALT),
                F.lower(F.trim(F.col("src.customer_number").cast("string")))
            ),
            256
        ).alias("customer_number_sha256")  ,

          


            



        # ✅ Keep everything from source
        "src.*",

        # ✅ HubSpot overrides (same names replace src)
        F.col("hs.customer_address").alias("customer_address"),
        F.col("hs.customer_billing_address").alias("customer_billing_address"),

        F.col("hs.cross_border_canada").alias("cross_border_canada"),
        F.col("hs.cross_border_mexico").alias("cross_border_mexico"),
        F.col("hs.exporter_indicator").alias("exporter_indicator"),
        F.col("hs.fortune_1000_indicator").alias("fortune_1000_indicator"),
        F.col("hs.company_exists_in_hubspot_indicator")
            .alias("company_exists_in_hubspot_indicator"),

        F.col("hs.assessment_credit_limit_rcc")
            .alias("assessment_credit_limit_rcc"),
        F.col("hs.assessment_credit_limit_rct")
            .alias("assessment_credit_limit_rct"),
        F.col("hs.assessment_delinq_score_np")
            .alias("assessment_delinq_score_np"),
        F.col("hs.assessment_failure_score_np")
            .alias("assessment_failure_score_np"),
        F.col("hs.assessment_std_rating_risk_sg")
            .alias("assessment_std_rating_risk_sg"),

        # ✅ Financials
        F.col("hs.sales_revenue_currency")
            .alias("sales_revenue_currency"),
        F.col("hs.global_ultimate_asr_currency")
            .alias("global_ultimate_asr_currency"),
        F.col("hs.estimated_customer_revenue")
            .alias("estimated_customer_revenue"),

        # ✅ Digital & web
        F.col("hs.website").alias("website"),
        F.col("hs.organization_website_addr_url")
            .alias("organization_website_addr_url"),
        F.col("hs.logo_url").alias("logo_url"),
        F.col("hs.linkedin_handle").alias("linkedin_handle"),
        F.col("hs.linkedin_company_page").alias("linkedin_company_page"),
        F.col("hs.facebook_company_page").alias("facebook_company_page"),
        F.col("hs.twitter_handle").alias("twitter_handle"),
        F.col("hs.web_technologies").alias("web_technologies"),

        # ✅ HS-only operational fields
        F.col("hs.organization_telephone_num")
            .alias("organization_telephone_num"),
        F.col("hs.hs_analytics_source_data")
            .alias("hs_analytics_source_data"),
        F.col("hs.hs_date_entered_marketing_qualified_lead")
            .alias("hs_date_entered_marketing_qualified_lead"),
        F.col("hs.hs_date_entered_opportunity")
            .alias("hs_date_entered_opportunity"),
        F.col("hs.hubspot_notes_last_activity")
            .alias("hubspot_notes_last_activity"),
        F.col("hs.hubspot_notes_next_activity")
            .alias("hubspot_notes_next_activity"),
        F.col("hs.hubspot_notes_next_activity_type")
            .alias("hubspot_notes_next_activity_type"),
        F.col("hs.hubspot_comments")
            .alias("hubspot_comments"),
        F.col("hs.hubspot_parent_company_id")
            .alias("hubspot_parent_company_id"),

        # ✅ Ownership / identifiers
        F.col("hs.hubspot_owner_id").alias("company_id"),
        F.col("hs.hubspot_owner_id").alias("hubspot_owner_id"),
        F.col("hs.duns_number").alias("duns_number"),

        # ✅ Classification / enrichment
        F.col("hs.employee_range").alias("employee_range"),
        F.col("hs.naics_codes").alias("naics_codes"),
        F.col("hs.sic_codes").alias("sic_codes"),
        F.col("hs.time_zone").alias("time_zone"),
        F.col("hs.enrich_status").alias("enrich_status"),
        F.col("hs.sec_checked_website_domain")
            .alias("sec_checked_website_domain"),

        # ✅ Estimated spend
        F.col("hs.est_ltl_spend").alias("est_ltl_spend"),
        F.col("hs.est_na_transportation_spend")
            .alias("est_na_transportation_spend"),
        F.col("hs.est_spend_canada").alias("est_spend_canada"),
        F.col("hs.est_spend_mexico").alias("est_spend_mexico"),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Window
from pyspark.sql import functions as F

window_spec = Window.partitionBy("customer_number") \
                    .orderBy(F.col("latest_created_date").desc_nulls_last())

df_dedup = (
    df_source
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

df_dedup_clean = df_dedup.withColumn(
    "email_normalized",
    F.lower(F.trim(F.col("primary_sales_rep_email")))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Mapping the House Accoutn Lookup

# CELL ********************

from pyspark.sql import functions as F

house_accounts_map_clean = (
    house_accounts_map
    .select(
        F.lower(F.trim(F.col("dos_email"))).alias("dos_email"),
        F.lower(F.trim(F.col("house_account_email"))).alias("house_account_email")
    )
)

df_joined = df_dedup_clean.join(
    house_accounts_map_clean,
    df_dedup_clean.email_normalized == house_accounts_map_clean.dos_email,
    "left"
)

df_fixed = df_joined.withColumn(
    "primary_sales_rep_email",
    F.when(
        F.col("house_account_email").isNotNull(),
        F.col("house_account_email")   # ✅ replace
    ).otherwise(
        F.col("primary_sales_rep_email")  # ✅ keep original
    )
)

df_final = df_fixed.drop(
    "email_normalized",
    "dos_email",
    "house_account_email"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_final
    .filter(F.col("primary_sales_rep_email").contains("house"))
    .select("primary_sales_rep_email")
    .distinct()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ### _Full Write_

# CELL ********************

print(f"{table_target} table full load started...")

write_gold_table(
    df=df_final,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,                     # ✅ FIXED
    zorder_cols=["customer_number"],         # ✅ GOOD
    mode="overwrite",
    run_optimize=True
)


print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold dim_division

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_division"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="date_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Query Full_

# CELL ********************

query_full="""

SELECT
    -1                                              as division_key,
    'Undefined'                                     as primary_key,
    'Undefined'                                     as division_name,
    'Undefined'                                     as netsuite_division_name,
    'Undefined'                                     as division_code,
    'Undefined'                                     as division_subsidiary_name,
    'Undefined'                                     as division_currency,
    'Undefined'                                     as division_organization_name,
    'Undefined'                                     as sales_commission_hierarchy,
    'Undefined'                                     as carrier_commission_hierarchy,
    current_timestamp()                             as __sys_synced,
    cast(0 as boolean)                              as __sys_deleted

UNION ALL

SELECT 

ROW_NUMBER() OVER(ORDER BY d.division_code)         as division_key,
d.division_code                                     as primary_key,
d.division_name                                     as division_name,
d.division_name_netsuite                            as netsuite_division_name,
d.division_code                                     as division_code,
d.subsidiary_name                                   as division_subsidiary_name,
d.division_currency                                 as division_currency,
d.organization_name                                 as division_organization_name,
d.division_name_netsuite                            as sales_commission_hierarchy,
COALESCE(b.division_mapped,d.division_name_netsuite) as carrier_commission_hierarchy,
to_timestamp(current_timestamp(),'M/d/y h:m:ss a')  as __sys_synced,
cast(0 as boolean)                                  as __sys_deleted
                             
FROM {division} d
LEFT JOIN (
    SELECT  3G_division_code,
            division_mapped
    FROM {branch_coalition_carrier_sales_team}  -- Mapping the Branch Coalition Carrier Sales Team Mapping
    ) b ON b.3G_division_code = d.division_code

where d.__sys_deleted=0


"""


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

division=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'division')
branch_coalition_carrier_sales_team=spark.read.load(production_lakehouse_silver_abfss+path_source_lookup+'branch_coalition_carrier_sales_team')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Write_

# CELL ********************

print(f"{table_target} table full load started...")

# --------------------------------------------------
# ✅ Build target dataframe from SQL
# --------------------------------------------------
df_source = spark.sql(
    query_full,
    division=division,
    branch_coalition_carrier_sales_team=branch_coalition_carrier_sales_team
)

# --------------------------------------------------
# ✅ Write Gold table (Fabric-optimized)
# --------------------------------------------------
write_gold_table(
    df=df_source,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,              # ✅ Do NOT force partitioning
    zorder_cols=["division_code"],     # ✅ Optional; remove if unnecessary
    mode="overwrite",
    run_optimize=True
)

print(f"{table_target} table full load completed")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{table_target} table full load started...")

# create target table dataframe. make sure to modify parameters per target table
df_source=spark.sql(query_full, division=division, branch_coalition_carrier_sales_team=branch_coalition_carrier_sales_team)

# convert dataframe to temporary view first in order to save it later as delta managed table
df_source.createOrReplaceTempView(f"{table_source}")
df_source=spark.sql(f"select * from {table_source}")
spark.catalog.dropTempView(f"{table_source}")

# save target dataframe
df_source.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{current_lakehouse_gold_abfss+path_target+table_target}")
print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ## Gold dim_deals

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_deals"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="date_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Query Full_

# CELL ********************

query_full="""

SELECT
    -1                                          AS deal_key,
    'Undefined'                                 AS deal_name,
    'Undefined'                                 AS deal_stage,
    -1                                          AS days_to_close,
    'Undefined'                                 AS deal_type,
    CAST('1900-01-01' AS DATE)                  AS estimated_decision_date,
    'Undefined'                                 AS deal_priority,
    'Undefined'                                 AS deal_pipeline,
    'Undefined'                                 AS deal_service_line,
    'Undefined'                                 AS deal_record_id,
    'Undefined'                                 AS last_meeting_followups,
    'Undefined'                                 AS last_meeting_activity,
    'Undefined'                                 AS pain_points,
    'Undefined'                                 AS payment_terms,
    current_timestamp()                         AS __sys_synced,
    CAST(0 AS BOOLEAN)                          AS __sys_deleted

UNION ALL

select 
ROW_NUMBER() OVER(ORDER BY deal_record_id)      as deal_key,
dealname                                        as deal_name,
dealstage                                       as deal_stage,
days_to_close                                   as days_to_close,
dealtype                                        as deal_type,
estimated_decision_date                         as estimated_decision_date,
hs_priority                                     as deal_priority,
pipeline                                        as deal_pipeline,
service_line                                    as deal_service_line,
deal_record_id                                  as deal_record_id,
last_meeting_followups                          as last_meeting_followups,
last_meeting_activity                           as last_meeting_activity,
pain_points                                     as pain_points,
payment_terms                                   as payment_terms,

to_timestamp(current_timestamp(),'M/d/y h:m:ss a')  as __sys_synced,
cast(0 as boolean)                              as __sys_deleted

from {deals} d

"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

deals=spark.read.load(production_lakehouse_silver_abfss+path_source_hubspot+'deals')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Write_

# CELL ********************

print(f"{table_target} table full load started...")

# --------------------------------------------------
# ✅ Build target dataframe from SQL
# --------------------------------------------------
df_source = spark.sql(
    query_full, 
    deals=deals
)

# --------------------------------------------------
# ✅ Write Gold table (Fabric-optimized)
# --------------------------------------------------
write_gold_table(
    df=df_source,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,                # ✅ Do NOT force partitioning
    zorder_cols=["deal_record_id"],     # ✅ Optional, remove if unnecessary
    mode="overwrite",
    run_optimize=True
)

print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{table_target} table full load started...")

# create target table dataframe. make sure to modify parameters per target table
df_source=spark.sql(
    query_full, 
    deals=deals
    )

# convert dataframe to temporary view first in order to save it later as delta managed table
df_source.createOrReplaceTempView(f"{table_source}")
df_source=spark.sql(f"select * from {table_source}")
spark.catalog.dropTempView(f"{table_source}")

# save target dataframe
df_source.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{current_lakehouse_gold_abfss+path_target+table_target}")
print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ## Gold dim_leads

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_leads"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="date_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Query Full_

# CELL ********************

query_full="""

SELECT
    -1                                                          AS lead_key,
    'Undefined'                                                 AS lead_associated_company_domain,
    'Undefined'                                                 AS lead_associated_company_name,
    'Undefined'                                                 AS lead_source_company_lifecycle_stage,
    'Undefined'                                                 AS lead_source_lifecycle_stage,
    'Undefined'                                                 AS lead_type,
    'Undefined'                                                 AS lead_pipeline,
    'Undefined'                                                 AS lead_pipeline_stage,
    'Undefined'                                                 AS lead_pipeline_stage_category,
    current_timestamp()                                         AS __sys_synced,
    CAST(0 AS BOOLEAN)                                          AS __sys_deleted

UNION ALL

select 
ROW_NUMBER() OVER(ORDER BY lead_associated_company_domain)      as lead_key,
*
from
(
    select 
    distinct    
    hs_associated_company_domain                                as lead_associated_company_domain,
    hs_associated_company_name                                  as lead_associated_company_name,
    hs_lead_source_company_lifecycle_stage                      as lead_source_company_lifecycle_stage,
    hs_lead_source_lifecycle_stage                              as lead_source_lifecycle_stage,
    hs_lead_type                                                as lead_type,
    hs_pipeline                                                 as lead_pipeline,
    hs_pipeline_stage                                           as lead_pipeline_stage,
    hs_pipeline_stage_category                                  as lead_pipeline_stage_category,

    to_timestamp(current_timestamp(),'M/d/y h:m:ss a')          as __sys_synced,
    cast(0 as boolean)                                          as __sys_deleted

    from {leads} l
) as tab
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

leads=spark.read.load(production_lakehouse_silver_abfss+path_source_hubspot+'leads')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Write_

# CELL ********************

print(f"{table_target} table full load started...")

# --------------------------------------------------
# ✅ Build target dataframe from SQL
# --------------------------------------------------
df_source = spark.sql(
    query_full,
    leads=leads
)

# --------------------------------------------------
# ✅ Write Gold table (Fabric-optimized)
# --------------------------------------------------
write_gold_table(
    df=df_source,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,                          # ✅ Do NOT force partitioning
    zorder_cols=["lead_associated_company_domain"],  # ✅ Optional
    mode="overwrite",
    run_optimize=True
)

print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{table_target} table full load started...")

# create target table dataframe. make sure to modify parameters per target table
df_source=spark.sql(
    query_full, 
    leads=leads
    )

# convert dataframe to temporary view first in order to save it later as delta managed table
df_source.createOrReplaceTempView(f"{table_source}")
df_source=spark.sql(f"select * from {table_source}")
spark.catalog.dropTempView(f"{table_source}")

# save target dataframe
df_source.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{current_lakehouse_gold_abfss+path_target+table_target}")
print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# MARKDOWN ********************

# ## Gold dim_ar

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_ar"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="ar_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query_full = """

SELECT
    -1                                              AS ar_key,
    'Undefined'                                     AS ar_header_id,
    'Undefined'                                     AS organization_name,
    'Undefined'                                     AS ar_num,
    CAST('1900-01-01' AS DATE)                      AS created_date,
    'Undefined'                                     AS route_number,
    'Undefined'                                     AS ar_status,
    'Undefined'                                     AS ar_entity_rating_rule,
    current_timestamp()                             AS __sys_synced,
    CAST(0 AS BOOLEAN)                              AS __sys_deleted

UNION ALL

SELECT 

    ROW_NUMBER() OVER (ORDER BY ar.ar_header_id)     AS ar_key,

    ar.ar_header_id                                  AS ar_header_id,
    org.organization_name                            AS organization_name,
    ar.ar_number                                     AS ar_num,
    ar.created_date                                  AS created_date,
    ar.route_number                                  AS route_number,
    ar.ar_status                                     AS ar_status,
    ar.ar_entity_rating_rule                         AS ar_entity_rating_rule,

    -- System Fields
    to_timestamp(current_timestamp(),'M/d/y h:m:ss a')   AS __sys_synced,
    CAST(0 AS BOOLEAN)                                   AS __sys_deleted

FROM {mrt_ar} ar
LEFT JOIN {organization} org
    ON org.organization_id = ar.organization_id

WHERE ar.sys_deleted = 0

"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

mrt_ar=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'mrt_ar')
organization=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'organization')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Write_

# CELL ********************

print(f"{table_target} table full load started...")

# --------------------------------------------------
# ✅ Build target dataframe from SQL
# --------------------------------------------------
df_source = spark.sql(
    query_full,
    mrt_ar=mrt_ar,
    organization=organization
)

# --------------------------------------------------
# ✅ Write Gold table (Fabric-optimized)
# --------------------------------------------------
write_gold_table(
    df=df_source,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,              # ✅ Do NOT force partitioning
    zorder_cols=["ar_header_id"],      # ✅ Optional; remove if unnecessary
    mode="overwrite",
    run_optimize=True
)

print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{table_target} table full load started...")

# create target table dataframe. make sure to modify parameters per target table
df_source=spark.sql(query_full, mrt_ar=mrt_ar, organization=organization)

# convert dataframe to temporary view first in order to save it later as delta managed table
df_source.createOrReplaceTempView(f"{table_source}")
df_source=spark.sql(f"select * from {table_source}")
spark.catalog.dropTempView(f"{table_source}")

# save target dataframe
df_source.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{current_lakehouse_gold_abfss+path_target+table_target}")
print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ## Gold dim_invoice

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_invoice"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="invoice_number"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _df_invoice_load_

# CELL ********************

query_invoice_load = """

Select  invoice_number,
        concat_ws(', ',
                collect_list(t.load_number))        as concat_load
from 
(
        SELECT  distinct 
                i.invoice_number,
                l.load_number
        FROM {invoice} i
        Left join {invoice_ord_header} ioh on ioh.invoice_id = i.invoice_id
        left join {order_header} oh on ioh.order_header_id = oh.order_header_id
        left join {load_quick_search} lqs on oh.order_header_id = lqs.entity_id
        left join {load} l on l.load_id = lqs.load_id
        where lqs.load_quick_search_field like 'OrdNum'
) t

group by invoice_number

"""

### Source Table Properties
invoice=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'invoice')
invoice_ord_header=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'invoice_ord_header')
order_header=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'order_header')
load_quick_search=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'load_quick_search')
load=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'load')

### Data Frame
df_invoice_load = spark.sql(query_invoice_load, 
                        invoice=invoice,
                        invoice_ord_header=invoice_ord_header,
                        order_header=order_header,
                        load_quick_search=load_quick_search,
                        load=load)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Query Full_

# CELL ********************

query_full = """

SELECT 
    cast(-1 as Int)                                         AS invoice_key,
    cast(-1 as Int)                                         AS primary_key,
    to_timestamp(current_timestamp(),'M/d/y h:m:ss a')      AS invoice_date_last_modified,
    to_timestamp(current_timestamp(),'M/d/y h:m:ss a')      AS invoice_date_created,
    cast(-1 as Int)                                         AS invoice_number,
    'Undefined'                                             AS invoice_status,
    'Undefined'                                             AS invoice_type,
    to_timestamp(current_timestamp(),'M/d/y h:m:ss a')      AS invoice_date,
    cast(-1 as Int)                                         AS invoice_manifest_number,
    'Undefined'                                             AS invoice_manifest_type,
    'Undefined'                                             AS invoice_load_number_list,
    to_timestamp(current_timestamp(),'M/d/y h:m:ss a')      AS __sys_synced,
    CAST(0 AS BOOLEAN)                                      AS __sys_deleted

Union All

SELECT 

    ROW_NUMBER() OVER (ORDER BY i.invoice_number)           AS invoice_key,
    i.invoice_number                                        AS primary_key,
    i.invoice_date_last_modified,
    i.invoice_date_created,
    i.invoice_number,
    i.invoice_status,
    i.invoice_type,
    i.invoice_date,
    i.invoice_manifest_number,
    i.invoice_manifest_type,
    dil.concat_load                                         AS invoice_load_number_list,

    -- System Fields
    to_timestamp(current_timestamp(),'M/d/y h:m:ss a')      AS __sys_synced,
    CAST(0 AS BOOLEAN)                                      AS __sys_deleted

FROM {invoice} i
left join {df_invoice_load} dil on dil.invoice_number = i.invoice_number

WHERE i.__sys_deleted = 0

"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

invoice=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'invoice')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Write_

# CELL ********************

print(f"{table_target} table full load started...")

# create target table dataframe. make sure to modify parameters per target table
df_source=spark.sql(query_full, invoice=invoice,df_invoice_load=df_invoice_load)

# convert dataframe to temporary view first in order to save it later as delta managed table
df_source.createOrReplaceTempView(f"{table_source}")
df_source=spark.sql(f"select * from {table_source}")
spark.catalog.dropTempView(f"{table_source}")

# save target dataframe
df_source.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{current_lakehouse_gold_abfss+path_target+table_target}")
print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold dim_freight_bill

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_freight_bill"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="date_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Query Full_

# CELL ********************

query_full="""

SELECT
    -1                                                      AS freight_bill_key,
    -1                                                      AS freight_bill_id,
    'Undefined'                                             AS freight_bill_number,
    CAST('1900-01-01' AS DATE)                              AS freight_bill_created_date,
    CAST('1900-01-01' AS DATE)                              AS freight_bill_date,
    'Undefined'                                             AS freight_bill_type,
    'Undefined'                                             AS freight_bill_status,
    'Undefined'                                             AS freight_bill_currency,
    'Undefined'                                             AS freightbill_var_reason,
    'Undefined'                                             AS freightbill_approve_rejected_reason,
    'Undefined'                                             AS freight_bill_revision_auto_approved_flag,
    'Undefined'                                             AS freight_bill_revision_status,
    'Undefined'                                             AS freight_bill_revision_type,
    current_timestamp()                                     AS __sys_synced,
    CAST(0 AS BOOLEAN)                                      AS __sys_deleted

UNION ALL

Select 

    ROW_NUMBER() OVER(ORDER BY freight_bill_id)             as freight_bill_key,
    freight_bill_id,
    freight_bill_number,
    freight_bill_created_date,
    freight_bill_date,
    freight_bill_type,
    freight_bill_status,
    freight_bill_currency,
    freightbill_var_reason,
	freightbill_approve_rejected_reason,
	freight_bill_revision_auto_approved_flag,
	freight_bill_revision_status,
    freight_bill_revision_type,
    __sys_synced,
    __sys_deleted

from
(

Select      
    freight_bill_id                                         as freight_bill_id,
    freight_bill_number                                     as freight_bill_number,
    created_date                                            as freight_bill_created_date,
    freight_bill_date                                       as freight_bill_date,
    freight_bill_type                                       as freight_bill_type,
    freight_bill_status                                     as freight_bill_status,
    currency_code                                           as freight_bill_currency,
    freightbill_var_reason                                  as freightbill_var_reason,
	freightbill_approve_rejected_reason                     as freightbill_approve_rejected_reason,
	freight_bill_revision_auto_approved_flag                as freight_bill_revision_auto_approved_flag,
	freight_bill_revision_status                            as freight_bill_revision_status,
	freight_bill_revision_type                              as freight_bill_revision_type,
    to_timestamp(current_timestamp(),'M/d/y h:m:ss a')      as __sys_synced,
    cast(0 as boolean)                                      as __sys_deleted

from {freight_bill} e1
where e1.__sys_deleted=0

)

"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

freight_bill=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'freight_bill')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Write_

# CELL ********************

print(f"{table_target} table full load started...")

# --------------------------------------------------
# ✅ Build target dataframe from SQL
# --------------------------------------------------
df_source = spark.sql(
    query_full,
    freight_bill=freight_bill
)

# --------------------------------------------------
# ✅ Write Gold table (Fabric-optimized)
# --------------------------------------------------
write_gold_table(
    df=df_source,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,                 # ✅ Do NOT force partitioning
    zorder_cols=["freight_bill_id"],     # ✅ Optional; remove if unnecessary
    mode="overwrite",
    run_optimize=True
)

print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{table_target} table full load started...")

# create target table dataframe. make sure to modify parameters per target table
df_source=spark.sql(query_full, freight_bill=freight_bill)

# convert dataframe to temporary view first in order to save it later as delta managed table
df_source.createOrReplaceTempView(f"{table_source}")
df_source=spark.sql(f"select * from {table_source}")
spark.catalog.dropTempView(f"{table_source}")

# save target dataframe
df_source.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{current_lakehouse_gold_abfss+path_target+table_target}")
print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ## Gold dim_accessorial_category

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

from pyspark.sql import functions as F, Window
# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_accessorial_category"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="primary_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def norm_text(c: Column | str) -> Column:
    """Uppercase, remove NBSP/&nbsp;/&amp;nbsp;, collapse spaces, trim."""
    col = _as_col(c)
    col = F.regexp_replace(col, "\u00A0", " ")
    col = F.regexp_replace(col, "&nbsp;", " ")
    col = F.regexp_replace(col, "&amp;nbsp;", " ")
    col = F.regexp_replace(col, r"\s+", " ")
    col = F.upper(col)
    col = F.trim(col)
    return col

def _as_col(c) -> Column:
    return F.col(c) if isinstance(c, str) else c

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

# Minimal inline mapping using your curated CODE_TO_CATEGORY dictionary
CODE_TO_CATEGORY = {
    # Fuel / surcharges
    "FSC":"FUEL","HFSC":"FUEL","BTF":"FUEL","BTFEE":"FUEL","LHS":"LINEHAUL_SURCHARGE","HND":"LINEHAUL_SURCHARGE",
    # Detention / dwell / waiting
    "DET":"DETENTION","DEP":"DETENTION","DWL":"DETENTION","DWT":"DETENTION",
    # TONU
    "TONU":"TONU",
    # Stop / drop / additional
    "STOP":"STOP","ADRP":"STOP","TR-DRP":"STOP","DRPF":"STOP",
    # Layover
    "LAY":"LAYOVER",
    # Redelivery
    "RED":"REDLVRY",
    # Appointment
    "APPT":"APPT",
    # Liftgate
    "LFT":"LIFTGATE","LG1":"LIFTGATE","LG2":"LIFTGATE",
    # Inside
    "INSP":"INSIDE","INSD":"INSIDE",
    # Residential
    "RES":"RESIDENTIAL","PRD":"RESIDENTIAL","RESP":"RESIDENTIAL",
    # Permits / escorts
    "PERM":"PERMIT_ESCORT",
    # Drayage / port / terminal / wharfage / demurrage
    "DRA":"DRAYAGE_PORT","CONGST":"DRAYAGE_PORT","THC":"DRAYAGE_PORT","WHF":"DRAYAGE_PORT",
    "DMGRE":"DRAYAGE_PORT","DMRGE":"DRAYAGE_PORT","PRPSS":"DRAYAGE_PORT","PDM":"DRAYAGE_PORT","CTNLFT":"DRAYAGE_PORT",
    # Border crossing / customs
    "BCF":"BORDER_CROSSING","BND-CH":"BORDER_CROSSING","BD-OFF":"BORDER_CROSSING","BONDAUDIT":"BORDER_CROSSING",
    # Hazmat
    "HAZ":"HAZMAT",
    # Pallet
    "PLT":"PALLET",
    # Tolls / scale
    "TOLLS":"TOLLS_SCALE","SCALE":"TOLLS_SCALE",
    # Warehousing / storage / yard
    "WHS":"WAREHOUSING_STORAGE","STR":"WAREHOUSING_STORAGE","YSF":"WAREHOUSING_STORAGE",
    # Reconsignment / BOL correction
    "REC":"RECONSIGNMENT","RECD":"RECONSIGNMENT","CBOL":"RECONSIGNMENT","RECO":"RECONSIGNMENT",
    # Chassis
    "CHSLFT":"CHASSIS","CHASIS2":"CHASSIS","TAC":"CHASSIS","CHSPLT":"CHASSIS",
    # Deadhead / dry run
    "DED":"DEADHEAD","DRYRN":"DEADHEAD",
    # Taxes
    "QST":"TAX","GST":"TAX","HST":"TAX",
    # High cost areas
    "HCA":"HIGH_COST_AREA","HCD":"HIGH_COST_AREA","HCP":"HIGH_COST_AREA",
    # After hours / weekend
    "AHR":"AFTER_HOURS","SAT":"AFTER_HOURS",
    # Misc / operational
    "LUM":"MISC","LUMP":"MISC","LUMPER":"MISC","LUMPER FEE":"MISC","LUMPER SERVICE":"MISC",
    "ADHS":"MISC","HAN":"MISC","DH":"MISC","TARP":"MISC","RFV":"MISC","RFRFEE":"MISC",
    "BLS":"MISC","TRACKING":"MISC","BRKFEE":"MISC","PROCESSING FEE":"MISC","MISC":"MISC",
    "OVERSIZE":"MISC","OVRWGT":"MISC","EXIN":"MISC","TEM":"MISC","FDA":"MISC","NCH":"MISC",
    "DPTTRM":"MISC","DPTPU":"MISC","ACC_TOTAL":"MISC","OFF-RT":"MISC","CPU":"MISC","PUC":"MISC",
    "PRO":"MISC","GUR":"MISC","WMSF":"MISC","OBH":"MISC","IBH":"MISC","HMS":"MISC",
    "HBC":"MISC","CAD":"MISC","EXC":"MISC","FED-EX":"MISC","XAC":"MISC","REVIEWCHARGE":"MISC",
}

acc_raw = (
    spark.read.format("delta")
            .load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'accessorial')
            .select("code_id","code","description","code_edi", "__sys_deleted")
)
acc = (
    acc_raw.withColumn("code_u", norm_text("code"))
            .withColumn("desc_u", norm_text("description"))
            .withColumn("edi_u",  norm_text("code_edi"))
)
mapping_df = (
    spark.createDataFrame([(k,v) for k,v in CODE_TO_CATEGORY.items()], ["raw_code","category"])
            .withColumn("code_u", norm_text("raw_code"))
            .select("code_u","category")
)
acc_map = (
    acc.join(mapping_df, on="code_u", how="left")
    .withColumn(
        "category",
        F.when(F.col("category").isNotNull(), F.col("category"))
        .when(
            F.col("desc_u").contains("FUEL") |
            F.col("desc_u").contains("FSC") |
            F.col("desc_u").contains("SURCHARGE"),
            F.lit("FUEL")
        )
        .otherwise(F.lit("MISC"))
    )
    .filter(F.col("__sys_deleted") == 0)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_spec = Window.orderBy("code_id")

df_target = (
    acc_map.select(
        F.row_number().over(window_spec).alias("accessorial_category_key"),
        F.col("code_id").alias("primary_key"),
        "category",
        "code",
        F.col("code_edi").alias("edi_code"),
        "description"
    )
)

# ---------------------------------------
# Undefined record
# ---------------------------------------
undefined_df = spark.createDataFrame(
    [
        (
            -1,                 # accessorial_category_key
            -1,                 # primary_key
            "Undefined",        # category
            "Undefined",        # code
            "Undefined",        # edi_code
            "Undefined"         # description
        )
    ],
    [
        "accessorial_category_key",
        "primary_key",
        "category",
        "code",
        "edi_code",
        "description"
    ]
)

# ---------------------------------------
# Add Undefined record
# ---------------------------------------
df_target = undefined_df.unionByName(df_target)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{current_lakehouse_gold_abfss + path_target + table_target}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Full Write

# CELL ********************

print(f"{table_target} table full load started...")

write_gold_table(
    df=df_target,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,     # ✅ Do NOT force partitioning
    zorder_cols=None,        # ✅ Optional; add only if it helps queries
    mode="overwrite",
    run_optimize=True
)

print(f"{table_target} table full load completed")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{table_target} table full load started...")

df_target.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "True") \
    .save(f"{current_lakehouse_gold_abfss + path_target + table_target}")

print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ## Gold Dim Voucher

# MARKDOWN ********************

# ### Target Table Properties

# CELL ********************

from pyspark.sql import functions as F, Window
# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_voucher"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="primary_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load source tables
voucher = spark.read.load(
    production_lakehouse_silver_abfss + path_source_tgtms_hub + 'voucher'
)

transaction_vendor = spark.read.load(
    production_lakehouse_silver_abfss + path_source_netsuite + 'transaction_vendor'
)

# Prepare join condition: strip last 3 chars
transaction_vendor = transaction_vendor.withColumn(
    'voucher_number_3g_trim',
    F.expr("substring(voucher_number_3g, 1, length(voucher_number_3g)-3)")
)

freight_bill = spark.read.load(
    production_lakehouse_silver_abfss + path_source_tgtms_hub + 'freight_bill'
)

load = spark.read.load(
    production_lakehouse_silver_abfss + path_source_tgtms_hub + 'load'
)

# Window to aggregate load per voucher
w_l = Window.partitionBy("v.voucher_id")

# ==============================
# Build target dataframe
# ==============================
df_target = (
    voucher.alias('v')
        .join(
            transaction_vendor.alias('tv'),
            [
                F.col('v.voucher_number') == F.col('tv.voucher_number_3g_trim'),
                F.col('tv.transaction_type') == F.lit('VendPymt'),
                F.col('tv.status_code') != F.lit('V')
            ],
            how='left'
        )
         .join(
            freight_bill.alias('fb'),
            F.col('v.freight_bill_id') == F.col('fb.freight_bill_id'),
            how='left'
        )
         .join(
            load.alias('l'),
            F.col('l.load_id') == F.col('fb.load_id'),
            how='left'
        )
        # Add concatenated load numbers
        .withColumn(
            "load_numbers",
            F.concat_ws(
                ", ",
                F.collect_list(F.col("l.load_number")).over(w_l)
            )
        )
        .select(
            F.row_number().over(
                Window.orderBy(F.col('v.voucher_id'))
            ).alias('voucher_key'),
            F.col('v.primary_key'),
            F.col('v.voucher_number'),
            F.col('v.voucher_id'),
            F.col('v.created_date').alias('voucher_created_date'),
            F.col('v.currency_code').alias('voucher_currency'),
            F.col('v.integration_status').alias('voucher_status'),
            F.col('tv.transaction_date').alias('voucher_payment_date'),
            F.col('tv.transaction_type').alias('voucher_payment_method'),
            F.col('tv.title').alias('voucher_payment_number'),
            F.col('v.transmitted_date').alias('voucher_transmitted_date'),
            F.col('v.created_by_username').alias('voucher_created_by'),
            F.col("load_numbers").alias('voucher_load_number_list')
        )
)

# ==============================
# Default (-1) Undefined row ✅
# ==============================
default_row_df = (
    spark.range(1)
        .select(
            F.lit(-1).cast("bigint").alias("voucher_key"),
            F.lit("-1").alias("primary_key"),
            F.lit("Undefined").alias("voucher_number"),
            F.lit(-1).cast("bigint").alias("voucher_id"),
            F.lit(None).cast("timestamp").alias("voucher_created_date"),
            F.lit("Undefined").alias("voucher_currency"),
            F.lit("Undefined").alias("voucher_status"),
            F.lit(None).cast("timestamp").alias("voucher_payment_date"),
            F.lit("Undefined").alias("voucher_payment_method"),
            F.lit("Undefined").alias("voucher_payment_number"),
            F.lit(None).cast("timestamp").alias("voucher_transmitted_date"),
            F.lit("Undefined").alias("voucher_created_by"),
            F.lit("Undefined").alias("voucher_load_number_list")
        )
)

# ==============================
# Union default row safely
# ==============================
df_target = (
    default_row_df
        .unionByName(df_target)
        .dropDuplicates(["voucher_key"])
)

df_target

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{current_lakehouse_gold_abfss + path_target + table_target}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Full Write 

# CELL ********************

print(f"{table_target} table full load started...")

write_gold_table(
    df=df_target,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,     # ✅ Do NOT force partitioning
    zorder_cols=None,        # ✅ Optional; add only if it helps queries
    mode="overwrite",
    run_optimize=True
)

print(f"{table_target} table full load completed")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold dim_calls

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_calls"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="date_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Query Full_

# CELL ********************

query_full = """

-- =========================
-- Default (-1) Undefined row
-- =========================
select
    cast(-1 as bigint)           as call_key,
    cast(-1 as bigint)           as hubspot_call_id,
    'Undefined'                  as call_type,
    'Undefined'                  as call_title,
    current_timestamp()          as __sys_synced,
    cast(0 as boolean)           as __sys_deleted

union all

-- =========================
-- Real call records
-- =========================
select 
    ROW_NUMBER() OVER (ORDER BY hubspot_call_id) as call_key,
    *
from
(
    select 
        distinct    
        c.hubspot_call_id,
        c.call_type,
        c.call_title,
        current_timestamp()      as __sys_synced,
        cast(0 as boolean)       as __sys_deleted
    from {calls} c
) as tab

"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

calls=spark.read.load(production_lakehouse_silver_abfss+path_source_hubspot+'calls')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Write_

# CELL ********************

print(f"{table_target} table full load started...")

# --------------------------------------------------
# ✅ Build target dataframe from SQL
# --------------------------------------------------
df_source = spark.sql(
    query_full, 
    calls=calls
)

# --------------------------------------------------
# ✅ Write Gold table (Fabric-optimized)
# --------------------------------------------------
write_gold_table(
    df=df_source,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,                # ✅ Do NOT force partitioning
    zorder_cols=["hubspot_call_id"],     # ✅ Optional, remove if unnecessary
    mode="overwrite",
    run_optimize=True
)

print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold dim_order

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_order"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="date_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Query Full_

# CELL ********************

query_full = """

-- =========================
-- Default (-1) Undefined row
-- =========================
SELECT
    CAST(-1 AS BIGINT)                                   AS order_key,
    CAST(-1 AS BIGINT)                                   AS order_header_key,
    '-1'                                                  AS primary_key,
    'Undefined'                                           AS order_number,
    'Undefined'                                           AS order_origin_name,
    'Undefined'                                           AS trading_partner_id_division,
    'Undefined'                                           AS order_origin_full_address,
    'Undefined'                                           AS order_origin_city,
    'Undefined'                                           AS order_origin_state,
    'Undefined'                                           AS order_origin_zip,
    'Undefined'                                           AS order_origin_country,
    'Undefined'                                           AS order_destination_name,
    'Undefined'                                           AS order_destination_full_address,
    'Undefined'                                           AS order_destination_city,
    'Undefined'                                           AS order_destination_state,
    'Undefined'                                           AS order_destination_zip,
    'Undefined'                                           AS order_destination_country,
    0                                                     AS order_total_handling_unit_count,
    0                                                     AS order_net_weight,
    'Undefined'                                           AS order_origin_city_state_country,
    'Undefined'                                           AS order_destination_city_state_country,
    'Undefined'                                           AS order_state_lane,
    0                                                     AS order_piece_count,
    NULL                                                  AS order_delivery_date,
    NULL                                                  AS order_early_pickup_date,
    NULL                                                  AS order_early_delivery_date,
    'Undefined'                                           AS order_status,
    NULL                                                  AS order_created_date,
    'Undefined'                                           AS order_highest_freight_class,
    NULL                                                  AS load_number_list,
    'Undefined'                                           AS freight_terms,
    'Undefined'                                           AS order_division,
    'Undefined'                                           AS order_direction,
    'Undefined'                                           AS rga,
    'Undefined'                                           AS order_bol_number,
    'Undefined'                                           AS order_container_number,
    NULL                                                  AS cost_center,
    current_timestamp()                                  AS __sys_synced,
    CAST(0 AS BOOLEAN)                                   AS __sys_deleted

UNION ALL

-- =========================
-- Real orders
-- =========================
SELECT 

    ROW_NUMBER() OVER (ORDER BY o.order_header_key)     AS order_key,
    o.order_header_key,
    o.primary_key,
    o.order_number,
    o.order_origin_name,
    o.trading_partner_id_division,
    o.order_origin_full_address,
    o.order_origin_city,
    o.order_origin_state,
    o.order_origin_zip,
    o.order_origin_country,
    o.order_destination_name,
    o.order_destination_full_address,
    o.order_destination_city,
    o.order_destination_state,
    o.order_destination_zip,
    o.order_destination_country,
    o.order_total_handling_unit_count,
    o.order_net_weight,
    o.order_origin_city_state_country,
    o.order_destination_city_state_country,
    o.order_state_lane,
    o.order_piece_count,
    o.order_delivery_date,
    o.order_early_pickup_date,
    o.order_early_delivery_date,
    o.order_status,
    o.order_created_date,
    o.order_highest_freight_class,
    l.load_number_list,
    o.freight_terms,
    o.order_division,
    ref.order_direction,
    ref.rga,
    ref.order_bol_number,
    ref.order_container_number,

    CASE 
        WHEN o.order_division LIKE '%Toyo Canada%' THEN '13107'
        WHEN o.order_division LIKE '%Toyo Tire Holdings%' 
             AND o.order_origin_name LIKE '%Plant 710%' THEN '8010'
        WHEN ref.order_direction LIKE '%Return%' 
             AND o.order_destination_state = 'GA' THEN '9070'
        WHEN o.order_origin_city LIKE '%WHITE%' 
             AND o.order_origin_state = 'GA' THEN '9070'
        WHEN o.order_origin_city LIKE '%SHIPPENSBURG%' 
             AND o.order_origin_state = 'PA' THEN '9095'
        WHEN ref.order_direction LIKE '%Return%' 
             AND o.order_destination_state = 'CA' THEN '9025'
        WHEN o.order_origin_city LIKE '%ONTARIO%' 
             AND o.order_origin_state = 'CA' THEN '9025'
        WHEN o.order_origin_city LIKE '%ROANOKE%' 
             AND o.order_origin_state = 'TX' THEN '9091'
        WHEN o.order_origin_city LIKE '%LEBANON%' 
             AND o.order_origin_state = 'TN' THEN '9090'
        WHEN o.order_origin_city LIKE '%NASHVILLE%' 
             AND o.order_origin_state = 'IL' THEN '9060'
        WHEN o.order_origin_city LIKE '%MARION%' 
             AND o.order_origin_state = 'IL' THEN '9092'
        WHEN o.order_origin_city LIKE '%HERRIN%' 
             AND o.order_origin_state = 'IL' THEN '9061'
        WHEN ref.order_direction LIKE '%INBOUND%' THEN '9010'
        ELSE NULL
    END                                                   AS cost_center,

    o.__sys_synced,
    o.__sys_deleted

FROM
(
    SELECT      
        o1.order_header_id                              AS order_header_key,
        o1.primary_key,
        o1.order_number,
        o1.trading_partner_id_division                  AS order_division,
        o1.origin_name                                  AS order_origin_name,
        o1.trading_partner_id_division                  AS trading_partner_id_division,         
        o1.origin_address                               AS order_origin_full_address,
        o1.origin_city                                  AS order_origin_city,
        o1.origin_state                                 AS order_origin_state,
        o1.origin_postal_code                           AS order_origin_zip,
        o1.origin_country                               AS order_origin_country,
        o1.destination_name                             AS order_destination_name,
        o1.destination_address                          AS order_destination_full_address,
        o1.destination_city                             AS order_destination_city,
        o1.destination_state                            AS order_destination_state,
        o1.destination_postal_code                      AS order_destination_zip,
        o1.destination_country                          AS order_destination_country,
        o1.total_handling_unit_count                    AS order_total_handling_unit_count,
        o1.net_weight                                   AS order_net_weight,
        o1.origin_city_state_country                    AS order_origin_city_state_country,
        o1.destination_city_state_country               AS order_destination_city_state_country,
        o1.order_state_lane                             AS order_state_lane,
        o1.order_piece_count                            AS order_piece_count,
        o1.order_delivery_date                          AS order_delivery_date, 
        o1.early_pickup_date                            AS order_early_pickup_date,
        o1.early_delivery_date                          AS order_early_delivery_date,
        o1.order_status                                 AS order_status,
        o1.created_date                                 AS order_created_date,
        o1.highest_freight_class                        AS order_highest_freight_class,
        o1.freight_terms                                AS freight_terms,
        current_timestamp()                             AS __sys_synced,
        CAST(0 AS BOOLEAN)                              AS __sys_deleted
    FROM {order_header} o1
    WHERE o1.__sys_deleted = 0
) o

LEFT JOIN
(
    SELECT 
        order_key,
        concat_ws(', ', collect_list(load_number)) AS load_number_list
    FROM {mrt_load}
    GROUP BY order_key
) l
ON o.order_header_key = l.order_key

LEFT JOIN
(
    SELECT 
        order_header_id,
        MAX(CASE WHEN UPPER(qualifier_name) LIKE '%DIRECTION%' THEN reference_number_value END) AS order_direction,
        MAX(CASE WHEN UPPER(qualifier_name) LIKE '%RGA%' THEN reference_number_value END)       AS rga,
        MAX(CASE WHEN UPPER(qualifier_name) LIKE '%BOL%' THEN reference_number_value END)       AS order_bol_number,
        MAX(CASE WHEN UPPER(qualifier_name) LIKE '%CONTAINER%' THEN reference_number_value END) AS order_container_number
    FROM {order_ref_num}
    WHERE __sys_deleted = 0
    GROUP BY order_header_id
) ref
ON o.order_header_key = ref.order_header_id

"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

order_header=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'order_header')
mrt_load=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'mrt_load')
order_ref_num=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'order_ref_num')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{current_lakehouse_gold_abfss + path_target + table_target}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Write_

# CELL ********************

print(f"{table_target} table full load started...")

# create target table dataframe. make sure to modify parameters per target table
df_source=spark.sql(query_full, 
                    order_header=order_header, 
                    mrt_load=mrt_load,
                    order_ref_num=order_ref_num)

# convert dataframe to temporary view first in order to save it later as delta managed table
df_source.createOrReplaceTempView(f"{table_source}")
df_source=spark.sql(f"select * from {table_source}")
spark.catalog.dropTempView(f"{table_source}")

# save target dataframe
#df_source.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{current_lakehouse_gold_abfss+path_target+table_target}")
#print(f"{table_target} table full load completed")

write_gold_table(
    df=df_source,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,                 # ✅ NO partition for dim
    zorder_cols=["order_key"],        # ✅ best column
    mode="overwrite",
    run_optimize=True
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Disable broadcast joins globally for this session
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold dim_functional_group

# CELL ********************

from pyspark.sql import functions as F, Window
# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_functional_group"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="primary_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Load source tables (adjust paths as appropriate)
functional_group_src_df = spark.read.load(production_lakehouse_silver_abfss + path_source_lookup + 'entra_hierarchy')



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ==============================
# 1. Read source table
# ==============================
src_df = functional_group_src_df


# ==============================
# 2. Prepare source
# ==============================
src_prepared_df = (
    src_df
        .withColumn("subgroup", F.coalesce(F.col("Subgroup"), F.lit("General")))
        .withColumn(
            "primary_key",
            F.concat_ws("|", F.col("FunctionalGroup"), F.col("subgroup"))
        )
        .select(
            "primary_key",
            F.col("Department").alias("department"),
            F.col("FunctionalGroup").alias("functional_group"),
            "subgroup",
            "sales_country",
            "operations_country"
        )
        .dropDuplicates(["primary_key"])
        .repartition("primary_key")
)


# ==============================
# 3. Default (-1) Undefined row ✅ FIXED
# ==============================
default_row_df = (
    spark.range(1)
        .select(
            F.lit(-1).cast("int").alias("functional_group_key"),
            F.lit("-1").alias("primary_key"),
            F.lit("Undefined").alias("department"),
            F.lit("Undefined").alias("functional_group"),
            F.lit("Undefined").alias("subgroup"),
            F.lit("Undefined").alias("sales_country"),
            F.lit("Undefined").alias("operations_country"),
            F.current_date().alias("__sys_sync")
        )
)


# ==============================
# 4. Check if target exists
# ==============================
try:
    dim_existing_df = spark.read.load(
        current_lakehouse_gold_abfss + path_target + table_target
    )
    dim_exists = True
except Exception:
    dim_existing_df = None
    dim_exists = False


# ==============================
# 5. Build dimension
# ==============================
if not dim_exists:
    # ---------- FIRST LOAD ----------
    w = Window.orderBy("primary_key")

    dim_functional_group_df = (
        src_prepared_df
            .withColumn("functional_group_key", F.row_number().over(w))
            .withColumn("__sys_sync", F.current_date())
            .select(
                "functional_group_key",
                "primary_key",
                "department",
                "functional_group",
                "subgroup",
                "sales_country",
                "operations_country",
                "__sys_sync"
            )
    )

else:
    # ---------- INCREMENTAL ----------
    max_key = (
        dim_existing_df
            .filter(F.col("functional_group_key") > 0)
            .agg(F.max("functional_group_key"))
            .collect()[0][0]
        or 0
    )

    dim_lookup_df = (
        dim_existing_df
            .select("primary_key", "functional_group_key")
            .dropDuplicates(["primary_key"])
            .repartition("primary_key")
            .hint("shuffle_hash")
    )

    joined_df = src_prepared_df.join(
        dim_lookup_df, on="primary_key", how="left"
    )

    existing_rows = joined_df.filter(F.col("functional_group_key").isNotNull())
    new_rows = joined_df.filter(F.col("functional_group_key").isNull())

    w = Window.orderBy("primary_key")

    new_rows_with_keys = (
        new_rows
            .withColumn(
                "functional_group_key",
                F.row_number().over(w) + F.lit(max_key)
            )
    )

    dim_functional_group_df = (
        existing_rows
            .unionByName(new_rows_with_keys)
            .withColumn("__sys_sync", F.current_date())
            .select(
                "functional_group_key",
                "primary_key",
                "department",
                "functional_group",
                "subgroup",
                "sales_country",
                "operations_country",
                "__sys_sync"
            )
    )


# ==============================
# 6. Add default row safely
# ==============================
dim_functional_group_df = (
    default_row_df
        .unionByName(dim_functional_group_df)
        .dropDuplicates(["functional_group_key"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ==============================
# 1. Read source table
# ==============================
src_df = functional_group_src_df


# ==============================
# 2. Prepare source (business key + attributes)
# ==============================
src_prepared_df = (
    src_df
        .withColumn("subgroup", F.coalesce(F.col("Subgroup"), F.lit("General")))
        .withColumn(
            "primary_key",
            F.concat_ws("|", F.col("FunctionalGroup"), F.col("subgroup"))
        )
        .select(
            "primary_key",
            F.col("Department").alias("department"),
            F.col("FunctionalGroup").alias("functional_group"),
            "subgroup",
            "sales_country",
            "operations_country"
        )
        .dropDuplicates(["primary_key"])          # ✅ Prevent join explosion
        .repartition("primary_key")               # ✅ Balanced shuffle
)


# ==============================
# 3. Check if target dimension exists
# ==============================
try:
    dim_existing_df = spark.read.load(
        current_lakehouse_gold_abfss + path_target + table_target
    )
    dim_exists = True
except Exception:
    dim_existing_df = None
    dim_exists = False


# ==============================
# 4. Build dimension dataframe
# ==============================
if not dim_exists:
    # ---------- FIRST LOAD ----------
    w = Window.orderBy("primary_key")

    dim_functional_group_df = (
        src_prepared_df
            .withColumn("functional_group_key", F.row_number().over(w))
            .withColumn("__sys_sync", F.current_timestamp())
            .select(
                "functional_group_key",
                "primary_key",
                "department",
                "functional_group",
                "subgroup",
                "sales_country",
                "operations_country",
                "__sys_sync"
            )
    )

else:
    # ---------- INCREMENTAL LOAD (SCD TYPE 1) ----------

    # Current max surrogate key
    max_key = (
        dim_existing_df
            .agg(F.max("functional_group_key"))
            .collect()[0][0]
        or 0
    )

    # ✅ Slim & safe lookup dimension
    dim_lookup_df = (
        dim_existing_df
            .select("primary_key", "functional_group_key")
            .dropDuplicates(["primary_key"])
            .repartition("primary_key")
            .hint("shuffle_hash")
    )

    # ✅ Stable left join
    joined_df = (
        src_prepared_df
            .join(dim_lookup_df, on="primary_key", how="left")
    )

    # ✅ Split rows
    existing_rows = joined_df.filter(F.col("functional_group_key").isNotNull())
    new_rows = joined_df.filter(F.col("functional_group_key").isNull())

    # ✅ Assign keys ONLY to new rows
    w = Window.orderBy("primary_key")

    new_rows_with_keys = (
        new_rows
            .withColumn(
                "functional_group_key",
                F.row_number().over(w) + F.lit(max_key)
            )
    )

    # ✅ Recombine safely
    dim_functional_group_df = (
        existing_rows
            .unionByName(new_rows_with_keys)
            .withColumn("__sys_sync", F.current_timestamp())
            .select(
                "functional_group_key",
                "primary_key",
                "department",
                "functional_group",
                "subgroup",
                "sales_country",
                "operations_country",
                "__sys_sync"
            )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

print(f"{table_target} table full load started...")

# --------------------------------------------------
# ✅ Write Gold table (Fabric-optimized)
# --------------------------------------------------
write_gold_table(
    df=dim_functional_group_df,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,                    # ✅ No partitioning for dimensions
    zorder_cols=["functional_group_key"],   # ✅ FIX: valid dimension key
    mode="overwrite",
    run_optimize=False
)

print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold dim_carrier 

# MARKDOWN ********************

# ### _Target table properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_carrier"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="date_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Query Full_

# CELL ********************


query_full = """

-- =========================
-- Default (-1) Undefined row
-- =========================
select
    cast(-1 as int) as primary_key,
    cast(-1 as int) as carrier_key,
    cast(-1 as int) as trading_partner_carrier_id,
    cast(-1 as int) as trading_partner_id,
    'Undefined' as carrier_name,
    'Undefined' as carrier_number,
    'Undefined' as carrier_organization_name,
    'Undefined' as mcc_number_type,
    'Undefined' as mcc_number,
    'Undefined' as mcc_number_merge,
    'Undefined' as country_code,
    'Undefined' as postal_code,
    'Undefined' as external_system_carrier_number,
    'Undefined' as ap_vendor_number,
    'Undefined' as alternate_ap_vendor_number,
    'Undefined' as hazmat_certification_number,
    cast(0 as boolean) as w9_flag,
    cast(0 as boolean) as railway_flag,
    cast(0 as boolean) as rail_drayage_flag,
    cast(0 as boolean) as port_drayage_flag,
    cast(0 as boolean) as services_airport_flag,
    cast(0 as boolean) as has_tia_watchdog_report_flag,
    cast(0 as boolean) as uiaa_certified_flag,
    cast(0 as boolean) as carb_compliant_flag,
    cast(0 as boolean) as smart_way_certified_flag,
    cast(0 as boolean) as ctpat_certified_flag,
    cast(0 as boolean) as twic_drivers_flag,
    cast(0 as boolean) as tsa_drivers_flag,
    cast(0 as boolean) as fast_certified_flag,
    cast(0 as boolean) as teams_flag,
    cast(0 as boolean) as reefer_equipment_flag,
    cast(0 as boolean) as handles_overweight_flag,
    cast(0 as boolean) as handles_oversize_flag,
    cast(0 as boolean) as tanker_endorsement_flag,
    cast(0 as boolean) as hazmat_certified_flag,
    cast(0 as boolean) as bonded_flag,
    cast(0 as boolean) as lock_do_not_assign_flag,
    cast(0 as boolean) as lock_is_active_flag,
    cast(0 as boolean) as freight_forwarder_flag,
    cast(0 as boolean) as carb_tru_compliant_flag,
    
    current_timestamp() as __sys_synced,
    cast(0 as boolean) as __sys_deleted

union all

-- =========================
-- Real carrier records
-- =========================
select 
    t1.carrier_id as primary_key,
    t1.carrier_id as carrier_key,
    t1.carrier_id as trading_partner_carrier_id,
    t1.id as trading_partner_id,
    t1.carrier_name,
    t1.carrier_number,
    t1.organization_name as carrier_organization_name,
    t1.mcc_number_type,
    t1.mcc_number,
    cast(t1.mcc_number_type as string) || '-' || cast(t1.mcc_number as string) as mcc_number_merge,
    t1.country_code,
    t1.postal_code,
    t1.external_system_carrier_number,
    t1.ap_vendor_number,
    t1.alternate_ap_vendor_number,
    t1.hazmat_certification_number,
    t1.has_w9 as w9_flag,
    t1.railway_flag,
    t1.rail_drayage_flag,
    t1.port_drayage_flag,
    t1.services_airport_flag,
    t1.has_tia_watchdog_report_flag,
    t1.uiaa_certified_flag,
    t1.carb_compliant_flag,
    t1.smart_way_certified_flag,
    t1.ctpat_certified_flag,
    t1.twic_drivers_flag,
    t1.tsa_drivers_flag,
    t1.fast_certified_flag,
    t1.teams_flag,
    t1.reefer_equipment_flag,
    t1.handles_overweight_flag,
    t1.handles_oversize_flag,
    t1.tanker_endorsement_flag,
    t1.hazmat_certified_flag,
    t1.bonded_flag,
    t1.lock_donot_assign as lock_do_not_assign_flag,
    t1.lock_is_active as lock_is_active_flag,
    t1.freight_forwarder_flag,
    t1.carb_tru_compliant_flag,
    current_timestamp() as __sys_synced,
    cast(0 as boolean) as __sys_deleted
from {trading_partner_carrier} t1
where t1.__sys_deleted = 0

"""



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

trading_partner_carrier=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'trading_partner_carrier')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Write_

# CELL ********************

print(f"{table_target} table full load started...")

# create target table dataframe. make sure to modify parameters per target table
df_source=spark.sql(query_full, trading_partner_carrier=trading_partner_carrier)

# convert dataframe to temporary view first in order to save it later as delta managed table
df_source.createOrReplaceTempView(f"{table_source}")
df_source=spark.sql(f"select * from {table_source}")
spark.catalog.dropTempView(f"{table_source}")

# save target dataframe
df_source.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{current_lakehouse_gold_abfss+path_target+table_target}")
print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold dim_organizaition

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

try:
    # notebookutils.fs.ls(current_lakehouse_gold_abfss+path_target+table_target)
    table_target = "dim_organization"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="date_key"
    date_modified="__sys_synced"
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

organization=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'organization')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{current_lakehouse_gold_abfss + path_target + table_target}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Query Full_

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

window_spec = Window.orderBy("organization_name")

dim_org = (
    organization
    .select(
        F.col("organization_name").cast(T.StringType()),
        F.col("created_date").cast(T.DateType()),
        F.col("last_modified_date").cast(T.DateType()),
    )
    .dropDuplicates(["organization_name"])
    .withColumn(
        "organization_key",
        F.row_number().over(window_spec).cast(T.IntegerType())
    ).select(
        F.col("organization_key"),
        F.col("organization_name"),
        F.col("created_date"),
        F.col("last_modified_date"),
    )
)

# ---------------------------------------
# Undefined record
# ---------------------------------------
undefined_df = spark.createDataFrame(
    [
        (
            -1,                     # organization_key
            "Undefined",            # organization_name
            "1900-01-01",           # created_date
            "1900-01-01"            # last_modified_date
        )
    ],
    schema=T.StructType([
        T.StructField("organization_key", T.IntegerType(), False),
        T.StructField("organization_name", T.StringType(), True),
        T.StructField("created_date", T.StringType(), True),
        T.StructField("last_modified_date", T.StringType(), True)
    ])
).select(
    F.col("organization_key"),
    F.col("organization_name"),
    F.to_date("created_date").alias("created_date"),
    F.to_date("last_modified_date").alias("last_modified_date")
)

# ---------------------------------------
# Add Undefined record
# ---------------------------------------
dim_org = undefined_df.unionByName(dim_org)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Write_

# CELL ********************

print(f"{table_target} table full load started...")

# Build dim_organization and save as delta managed table
dim_org.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{current_lakehouse_gold_abfss+path_target+table_target}")

print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold dim_talent_time_off

# MARKDOWN ********************

# ### _Target table properties_

# CELL ********************

# declare target table properties

try:
    table_target = "dim_talent_time_off"
    table_source = f"{table_target}_source"
    table_target_abfss = spark.read.load(
        current_lakehouse_gold_abfss + path_target + table_target
    )
    primary_key = "user_key"
except Exception as e:
    print(f"{table_target} full load will start since: ", e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Soruce Table Properties_

# CELL ********************

talent_time_off = spark.read.load(production_lakehouse_gold_abfss + path_source_hr + "talent_time_off")
dim_user = spark.read.load(production_lakehouse_gold_abfss + path_target + "dim_user")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Query_

# CELL ********************

query_full = """

SELECT
    -1 AS user_key,
    -1 AS time_off_date_key,
    'Undefined' AS time_off_policy

UNION ALL


SELECT DISTINCT
    du.user_key,
    CAST(DATE_FORMAT(TO_DATE(TRIM(tt.time_off_date)), 'yyyyMMdd') AS INT) AS time_off_date_key,
    TRIM(tt.time_off_policy) AS time_off_policy
FROM {talent_time_off} tt
INNER JOIN {dim_user} du
  ON LOWER(TRIM(SUBSTRING(TRIM(tt.primary_key), 1, LENGTH(TRIM(tt.primary_key)) - 10)))
     = LOWER(TRIM(SUBSTRING_INDEX(du.primary_key, '@', 1)))
WHERE LENGTH(TRIM(tt.primary_key)) > 10
  AND COALESCE(CAST(tt.__sys_deleted AS INT), 0) = 0
  AND COALESCE(CAST(du.__sys_deleted AS INT), 0) = 0
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Merge_

# CELL ********************

print(f"{table_target} table full load started...")
df_source = spark.sql(query_full, talent_time_off=talent_time_off, dim_user=dim_user)

write_gold_table(
    df=df_source,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,
    mode="overwrite",
    run_optimize=False,
)
print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
