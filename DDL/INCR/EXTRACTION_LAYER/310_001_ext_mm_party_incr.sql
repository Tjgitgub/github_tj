CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_mm_party_incr"() 
RETURNS void 
LANGUAGE 'plpgsql' 

AS $function$ 
/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.16, generation date: 2025/01/16 15:01:59
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/16 14:56:23, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46
 */


BEGIN 

BEGIN -- ext_tgt

	TRUNCATE TABLE "moto_mktg_scn01_ext"."party"  CASCADE;

	INSERT INTO "moto_mktg_scn01_ext"."party"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"party_number"
		,"parent_party_number"
		,"address_number"
		,"name_bk"
		,"birthdate_bk"
		,"gender_bk"
		,"party_type_code_bk"
		,"name"
		,"birthdate"
		,"gender"
		,"party_type_code"
		,"comments"
		,"update_timestamp"
		,"error_code_cust_cust_parentpartynumber"
		,"error_code_cust_addr"
	)
	WITH "calculate_variables" AS 
	( 
		SELECT 
			  "lci_src"."load_cycle_id" AS "load_cycle_id"
			, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "tdfv_src"."party_number"  ORDER BY  "tdfv_src"."trans_timestamp")
				* interval'2 microsecond'   AS "load_date"
			, "tdfv_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("tdfv_src"."operation","mex_src"."attribute_varchar") AS "operation"
			, "tdfv_src"."record_type" AS "record_type"
			, "tdfv_src"."party_number" AS "party_number"
			, "tdfv_src"."parent_party_number" AS "parent_party_number"
			, "tdfv_src"."address_number" AS "address_number"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."parent_party_number" = TO_NUMBER('-1', 
				'999999999999D999999999'::varchar) THEN 0 ELSE 1 END AS "ch_parent_party_number"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."address_number" = TO_NUMBER('-1', 
				'999999999999D999999999'::varchar) THEN 0 ELSE 1 END AS "ch_address_number"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."parent_party_number" = TO_NUMBER('-1', 
				'999999999999D999999999'::varchar) THEN 1 ELSE 0 END AS "nc_parent_party_number"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."address_number" = TO_NUMBER('-1', 
				'999999999999D999999999'::varchar) THEN 1 ELSE 0 END AS "nc_address_number"
			, "tdfv_src"."name" AS "name"
			, "tdfv_src"."birthdate" AS "birthdate"
			, "tdfv_src"."gender" AS "gender"
			, "tdfv_src"."party_type_code" AS "party_type_code"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."name" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_name"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."birthdate" = TO_DATE('01/01/1970', 'DD/MM/YYYY'::varchar)
				THEN 0 ELSE 1 END AS "ch_birthdate"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."gender" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_gender"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."party_type_code" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_party_type_code"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."name" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_name"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."birthdate" = TO_DATE('01/01/1970', 'DD/MM/YYYY'::varchar)
				THEN 1 ELSE 0 END AS "nc_birthdate"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."gender" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_gender"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."party_type_code" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_party_type_code"
			, "tdfv_src"."comments" AS "comments"
			, "tdfv_src"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."comments" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_comments"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."update_timestamp" = TO_TIMESTAMP('01/01/1970 00:00:00',
				'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN 0 ELSE 1 END AS "ch_update_timestamp"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."comments" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_comments"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."update_timestamp" = TO_TIMESTAMP('01/01/1970 00:00:00',
				'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN 1 ELSE 0 END AS "nc_update_timestamp"
		FROM "moto_mktg_scn01_dfv"."vw_party" "tdfv_src"
		INNER JOIN "moto_mktg_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	, "change_index" AS 
	( 
		SELECT 
			  "calculate_variables"."load_cycle_id" AS "load_cycle_id"
			, "calculate_variables"."load_date" AS "load_date"
			, "calculate_variables"."trans_timestamp" AS "trans_timestamp"
			, "calculate_variables"."operation" AS "operation"
			, "calculate_variables"."record_type" AS "record_type"
			, "calculate_variables"."party_number" AS "party_number"
			, "calculate_variables"."parent_party_number" AS "parent_party_number"
			, "calculate_variables"."address_number" AS "address_number"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_parent_party_number")
				OVER(PARTITION BY"calculate_variables"."party_number" ORDER BY "calculate_variables"."load_date")END AS "ch_index_parent_party_number"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_address_number")
				OVER(PARTITION BY"calculate_variables"."party_number" ORDER BY "calculate_variables"."load_date")END AS "ch_index_address_number"
			, "calculate_variables"."nc_parent_party_number" AS "nc_parent_party_number"
			, "calculate_variables"."nc_address_number" AS "nc_address_number"
			, "calculate_variables"."name" AS "name"
			, "calculate_variables"."birthdate" AS "birthdate"
			, "calculate_variables"."gender" AS "gender"
			, "calculate_variables"."party_type_code" AS "party_type_code"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_name")
				OVER(PARTITION BY"calculate_variables"."party_number" ORDER BY "calculate_variables"."load_date")END AS "ch_index_name"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_birthdate")
				OVER(PARTITION BY"calculate_variables"."party_number" ORDER BY "calculate_variables"."load_date")END AS "ch_index_birthdate"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_gender")
				OVER(PARTITION BY"calculate_variables"."party_number" ORDER BY "calculate_variables"."load_date")END AS "ch_index_gender"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_party_type_code")
				OVER(PARTITION BY"calculate_variables"."party_number" ORDER BY "calculate_variables"."load_date")END AS "ch_index_party_type_code"
			, "calculate_variables"."nc_name" AS "nc_name"
			, "calculate_variables"."nc_birthdate" AS "nc_birthdate"
			, "calculate_variables"."nc_gender" AS "nc_gender"
			, "calculate_variables"."nc_party_type_code" AS "nc_party_type_code"
			, "calculate_variables"."comments" AS "comments"
			, "calculate_variables"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_comments")
				OVER(PARTITION BY"calculate_variables"."party_number" ORDER BY "calculate_variables"."load_date")END AS "ch_index_comments"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_update_timestamp")
				OVER(PARTITION BY"calculate_variables"."party_number" ORDER BY "calculate_variables"."load_date")END AS "ch_index_update_timestamp"
			, "calculate_variables"."nc_comments" AS "nc_comments"
			, "calculate_variables"."nc_update_timestamp" AS "nc_update_timestamp"
		FROM "calculate_variables" "calculate_variables"
	)
	, "all_time_slices" AS 
	( 
		SELECT 
			  "sat_src1"."load_date" AS "load_date"
			, "sat_src1"."party_number" AS "party_number"
			, 'T' AS "record_type"
		FROM "change_index" "change_index"
		INNER JOIN "moto_scn01_fl"."sat_mm_customers" "sat_src1" ON  "change_index"."party_number" = "sat_src1"."party_number"
	)
	, "previous_version" AS 
	( 
		SELECT 
			  "all_time_slices"."load_date" AS "load_date"
			, "all_time_slices"."party_number" AS "party_number"
			, ROW_NUMBER()OVER(PARTITION BY "all_time_slices"."party_number" ORDER BY "all_time_slices"."load_date" DESC) AS "dummy"
		FROM "all_time_slices" "all_time_slices"
	)
	, "create_set_nc_values" AS 
	( 
		SELECT 
			  "change_index"."load_date" AS "load_date"
			, "change_index"."load_cycle_id" AS "load_cycle_id"
			, "change_index"."trans_timestamp" AS "trans_timestamp"
			, "change_index"."operation" AS "operation"
			, CASE WHEN "change_index"."operation" = 'I' THEN 1 WHEN "change_index"."operation" = 'U' THEN 2 WHEN "change_index"."operation" =
				'D' THEN 3 ELSE 9999 END AS "order_operation"
			, "change_index"."record_type" AS "record_type"
			, "change_index"."party_number" AS "party_number"
			, "change_index"."parent_party_number" AS "parent_party_number"
			, "change_index"."address_number" AS "address_number"
			, CASE WHEN "change_index"."nc_parent_party_number" = 0 THEN 0 ELSE SUM("change_index"."nc_parent_party_number")
				OVER(PARTITION BY"change_index"."party_number","change_index"."ch_index_parent_party_number" ORDER BY "change_index"."load_date")END AS "lag_index_parent_party_number"
			, CASE WHEN "change_index"."nc_address_number" = 0 THEN 0 ELSE SUM("change_index"."nc_address_number")
				OVER(PARTITION BY"change_index"."party_number","change_index"."ch_index_address_number" ORDER BY "change_index"."load_date")END AS "lag_index_address_number"
			, "change_index"."name" AS "name"
			, "change_index"."birthdate" AS "birthdate"
			, "change_index"."gender" AS "gender"
			, "change_index"."party_type_code" AS "party_type_code"
			, CASE WHEN "change_index"."nc_name" = 0 THEN 0 ELSE SUM("change_index"."nc_name")OVER(PARTITION BY "change_index"."party_number",
				"change_index"."ch_index_name" ORDER BY "change_index"."load_date")END AS "lag_index_name"
			, CASE WHEN "change_index"."nc_birthdate" = 0 THEN 0 ELSE SUM("change_index"."nc_birthdate")OVER(PARTITION BY "change_index"."party_number",
				"change_index"."ch_index_birthdate" ORDER BY "change_index"."load_date")END AS "lag_index_birthdate"
			, CASE WHEN "change_index"."nc_gender" = 0 THEN 0 ELSE SUM("change_index"."nc_gender")OVER(PARTITION BY "change_index"."party_number",
				"change_index"."ch_index_gender" ORDER BY "change_index"."load_date")END AS "lag_index_gender"
			, CASE WHEN "change_index"."nc_party_type_code" = 0 THEN 0 ELSE SUM("change_index"."nc_party_type_code")
				OVER(PARTITION BY"change_index"."party_number","change_index"."ch_index_party_type_code" ORDER BY "change_index"."load_date")END AS "lag_index_party_type_code"
			, "change_index"."comments" AS "comments"
			, "change_index"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "change_index"."nc_comments" = 0 THEN 0 ELSE SUM("change_index"."nc_comments")OVER(PARTITION BY "change_index"."party_number",
				"change_index"."ch_index_comments" ORDER BY "change_index"."load_date")END AS "lag_index_comments"
			, CASE WHEN "change_index"."nc_update_timestamp" = 0 THEN 0 ELSE SUM("change_index"."nc_update_timestamp")
				OVER(PARTITION BY"change_index"."party_number","change_index"."ch_index_update_timestamp" ORDER BY "change_index"."load_date")END AS "lag_index_update_timestamp"
			, 1 AS "error_code_cust_cust_parentpartynumber"
			, 1 AS "error_code_cust_addr"
		FROM "change_index" "change_index"
		UNION ALL 
		SELECT 
			  "result_sat_values"."load_date" AS "load_date"
			, "result_sat_values"."load_cycle_id" AS "load_cycle_id"
			, "result_sat_values"."trans_timestamp" AS "trans_timestamp"
			, 'SAT' AS "operation"
			, 0 AS "order_operation"
			, 'T' AS "record_type"
			, "result_sat_values"."party_number" AS "party_number"
			, "result_sat_values"."parent_party_number" AS "parent_party_number"
			, "result_sat_values"."address_number" AS "address_number"
			, 0 AS "lag_index_parent_party_number"
			, 0 AS "lag_index_address_number"
			, "result_sat_values"."name" AS "name"
			, "result_sat_values"."birthdate" AS "birthdate"
			, "result_sat_values"."gender" AS "gender"
			, "result_sat_values"."party_type_code" AS "party_type_code"
			, 0 AS "lag_index_name"
			, 0 AS "lag_index_birthdate"
			, 0 AS "lag_index_gender"
			, 0 AS "lag_index_party_type_code"
			, "result_sat_values"."comments" AS "comments"
			, "result_sat_values"."update_timestamp" AS "update_timestamp"
			, 0 AS "lag_index_comments"
			, 0 AS "lag_index_update_timestamp"
			, 1 AS "error_code_cust_cust_parentpartynumber"
			, 1 AS "error_code_cust_addr"
		FROM "previous_version" "previous_version"
		INNER JOIN "moto_scn01_fl"."sat_mm_customers" "result_sat_values" ON  "previous_version"."party_number" = "result_sat_values"."party_number" AND "previous_version"."load_date" = 
			"result_sat_values"."load_date" AND "previous_version"."dummy" = 1
		UNION ALL 
		SELECT 
			  "ext_err_src"."load_date" AS "load_date"
			, "err_lci_src"."load_cycle_id" AS "load_cycle_id"
			, "ext_err_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("ext_err_src"."operation",'U') AS "operation"
			, 0 AS "order_operation"
			, 'E' AS "record_type"
			, "ext_err_src"."party_number" AS "party_number"
			, "ext_err_src"."parent_party_number" AS "parent_party_number"
			, "ext_err_src"."address_number" AS "address_number"
			, 0 AS "lag_index_parent_party_number"
			, 0 AS "lag_index_address_number"
			, "ext_err_src"."name" AS "name"
			, "ext_err_src"."birthdate" AS "birthdate"
			, "ext_err_src"."gender" AS "gender"
			, "ext_err_src"."party_type_code" AS "party_type_code"
			, 0 AS "lag_index_name"
			, 0 AS "lag_index_birthdate"
			, 0 AS "lag_index_gender"
			, 0 AS "lag_index_party_type_code"
			, "ext_err_src"."comments" AS "comments"
			, "ext_err_src"."update_timestamp" AS "update_timestamp"
			, 0 AS "lag_index_comments"
			, 0 AS "lag_index_update_timestamp"
			, CASE WHEN "ext_err_src"."error_code_cust_cust_parentpartynumber" = 0 THEN 2 ELSE "ext_err_src"."error_code_cust_cust_parentpartynumber" END AS "error_code_cust_cust_parentpartynumber"
			, CASE WHEN "ext_err_src"."error_code_cust_addr" = 0 THEN 2 ELSE "ext_err_src"."error_code_cust_addr" END AS "error_code_cust_addr"
		FROM "moto_mktg_scn01_ext"."party_err" "ext_err_src"
		INNER JOIN "moto_mktg_scn01_mtd"."load_cycle_info" "err_lci_src" ON  1 = 1
		WHERE  "ext_err_src"."error_code_cust_cust_parentpartynumber" < 4 AND  "ext_err_src"."error_code_cust_addr" < 4
	)
	, "result_set_nc_values" AS 
	( 
		SELECT 
			  "create_set_nc_values"."load_cycle_id" AS "load_cycle_id"
			, "create_set_nc_values"."load_date" AS "load_date"
			, "create_set_nc_values"."trans_timestamp" AS "trans_timestamp"
			, "create_set_nc_values"."operation" AS "operation"
			, "create_set_nc_values"."record_type" AS "record_type"
			, "create_set_nc_values"."party_number" AS "party_number"
			, LAG("create_set_nc_values"."parent_party_number", "create_set_nc_values"."lag_index_parent_party_number"::int)
				OVER(PARTITION BY"create_set_nc_values"."party_number" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "parent_party_number"
			, LAG("create_set_nc_values"."address_number", "create_set_nc_values"."lag_index_address_number"::int)
				OVER(PARTITION BY"create_set_nc_values"."party_number" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "address_number"
			, LAG("create_set_nc_values"."name", "create_set_nc_values"."lag_index_name"::int)OVER(PARTITION BY "create_set_nc_values"."party_number" ORDER BY "create_set_nc_values"."load_date",
				"create_set_nc_values"."order_operation") AS "name"
			, LAG("create_set_nc_values"."birthdate", "create_set_nc_values"."lag_index_birthdate"::int)OVER(PARTITION BY "create_set_nc_values"."party_number" ORDER BY "create_set_nc_values"."load_date",
				"create_set_nc_values"."order_operation") AS "birthdate"
			, LAG("create_set_nc_values"."gender", "create_set_nc_values"."lag_index_gender"::int)OVER(PARTITION BY "create_set_nc_values"."party_number" ORDER BY "create_set_nc_values"."load_date",
				"create_set_nc_values"."order_operation") AS "gender"
			, LAG("create_set_nc_values"."party_type_code", "create_set_nc_values"."lag_index_party_type_code"::int)
				OVER(PARTITION BY"create_set_nc_values"."party_number" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "party_type_code"
			, LAG("create_set_nc_values"."comments", "create_set_nc_values"."lag_index_comments"::int)OVER(PARTITION BY "create_set_nc_values"."party_number" ORDER BY "create_set_nc_values"."load_date",
				"create_set_nc_values"."order_operation") AS "comments"
			, LAG("create_set_nc_values"."update_timestamp", "create_set_nc_values"."lag_index_update_timestamp"::int)
				OVER(PARTITION BY"create_set_nc_values"."party_number" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "update_timestamp"
			, "create_set_nc_values"."error_code_cust_cust_parentpartynumber" AS "error_code_cust_cust_parentpartynumber"
			, "create_set_nc_values"."error_code_cust_addr" AS "error_code_cust_addr"
		FROM "create_set_nc_values" "create_set_nc_values"
	)
	, "calculate_bk" AS 
	( 
		SELECT 
			  "result_set_nc_values"."load_cycle_id" AS "load_cycle_id"
			, "result_set_nc_values"."load_date" AS "load_date"
			, "result_set_nc_values"."trans_timestamp" AS "trans_timestamp"
			, "result_set_nc_values"."operation" AS "operation"
			, "result_set_nc_values"."record_type" AS "record_type"
			, "result_set_nc_values"."party_number" AS "party_number"
			, "result_set_nc_values"."parent_party_number" AS "parent_party_number"
			, "result_set_nc_values"."address_number" AS "address_number"
			, COALESCE(UPPER(REPLACE(TRIM("result_set_nc_values"."name"),'#','\' || '#')),"mex_src_bk"."key_attribute_varchar") AS "name_bk"
			, COALESCE(UPPER( TO_CHAR("result_set_nc_values"."birthdate", 'DD/MM/YYYY'::varchar)),"mex_src_bk"."key_attribute_date") AS "birthdate_bk"
			, COALESCE(UPPER(REPLACE(TRIM("result_set_nc_values"."gender"),'#','\' || '#')),"mex_src_bk"."key_attribute_character") AS "gender_bk"
			, COALESCE(UPPER(REPLACE(TRIM("result_set_nc_values"."party_type_code"),'#','\' || '#')),"mex_src_bk"."key_attribute_character") AS "party_type_code_bk"
			, "result_set_nc_values"."name" AS "name"
			, "result_set_nc_values"."birthdate" AS "birthdate"
			, "result_set_nc_values"."gender" AS "gender"
			, "result_set_nc_values"."party_type_code" AS "party_type_code"
			, "result_set_nc_values"."comments" AS "comments"
			, "result_set_nc_values"."update_timestamp" AS "update_timestamp"
			, "result_set_nc_values"."error_code_cust_cust_parentpartynumber" AS "error_code_cust_cust_parentpartynumber"
			, "result_set_nc_values"."error_code_cust_addr" AS "error_code_cust_addr"
		FROM "result_set_nc_values" "result_set_nc_values"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src_bk" ON  "mex_src_bk"."record_type" = 'N'
		WHERE  "result_set_nc_values"."record_type" != 'T'
	)
	SELECT 
		  "calculate_bk"."load_cycle_id" AS "load_cycle_id"
		, CASE WHEN "calculate_bk"."record_type" != 'E' THEN CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "calculate_bk"."name_bk" ,
			"calculate_bk"."birthdate_bk" ,  "calculate_bk"."gender_bk" ,  "calculate_bk"."party_type_code_bk"  ORDER BY  "calculate_bk"."trans_timestamp") * interval'2 microsecond'   ELSE "calculate_bk"."load_date" END AS "load_date"
		, "calculate_bk"."trans_timestamp" AS "trans_timestamp"
		, "calculate_bk"."operation" AS "operation"
		, "calculate_bk"."record_type" AS "record_type"
		, "calculate_bk"."party_number" AS "party_number"
		, "calculate_bk"."parent_party_number" AS "parent_party_number"
		, "calculate_bk"."address_number" AS "address_number"
		, "calculate_bk"."name_bk" AS "name_bk"
		, "calculate_bk"."birthdate_bk" AS "birthdate_bk"
		, "calculate_bk"."gender_bk" AS "gender_bk"
		, "calculate_bk"."party_type_code_bk" AS "party_type_code_bk"
		, "calculate_bk"."name" AS "name"
		, "calculate_bk"."birthdate" AS "birthdate"
		, "calculate_bk"."gender" AS "gender"
		, "calculate_bk"."party_type_code" AS "party_type_code"
		, "calculate_bk"."comments" AS "comments"
		, "calculate_bk"."update_timestamp" AS "update_timestamp"
		, "calculate_bk"."error_code_cust_cust_parentpartynumber" AS "error_code_cust_cust_parentpartynumber"
		, "calculate_bk"."error_code_cust_addr" AS "error_code_cust_addr"
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 
