CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_mm_camppartcont_incr"() 
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

	TRUNCATE TABLE "moto_mktg_scn01_ext"."camp_part_cont"  CASCADE;

	INSERT INTO "moto_mktg_scn01_ext"."camp_part_cont"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"party_number"
		,"campaign_code"
		,"campaign_start_date"
		,"contact_id"
		,"contact_id_fk_contactid_bk"
		,"campaign_code_fk_campaignstartdate_bk"
		,"campaign_start_date_fk_campaignstartdate_bk"
		,"marketing_program_code"
		,"marketing_program_name"
		,"update_timestamp"
		,"error_code_camp_cust_cont"
	)
	WITH "calculate_variables" AS 
	( 
		SELECT 
			  "lci_src"."load_cycle_id" AS "load_cycle_id"
			, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "tdfv_src"."party_number" ,  "tdfv_src"."campaign_code" ,
				"tdfv_src"."campaign_start_date" ,  "tdfv_src"."contact_id"  ORDER BY  "tdfv_src"."trans_timestamp") * interval'2 microsecond'   AS "load_date"
			, "tdfv_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("tdfv_src"."operation","mex_src"."attribute_varchar") AS "operation"
			, "tdfv_src"."record_type" AS "record_type"
			, "tdfv_src"."party_number" AS "party_number"
			, "tdfv_src"."campaign_code" AS "campaign_code"
			, "tdfv_src"."campaign_start_date" AS "campaign_start_date"
			, "tdfv_src"."contact_id" AS "contact_id"
			, "tdfv_src"."marketing_program_code" AS "marketing_program_code"
			, "tdfv_src"."marketing_program_name" AS "marketing_program_name"
			, "tdfv_src"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."marketing_program_code" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_marketing_program_code"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."marketing_program_name" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_marketing_program_name"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."update_timestamp" = TO_TIMESTAMP('01/01/1970 00:00:00',
				'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN 0 ELSE 1 END AS "ch_update_timestamp"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."marketing_program_code" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_marketing_program_code"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."marketing_program_name" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_marketing_program_name"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."update_timestamp" = TO_TIMESTAMP('01/01/1970 00:00:00',
				'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN 1 ELSE 0 END AS "nc_update_timestamp"
		FROM "moto_mktg_scn01_dfv"."vw_camp_part_cont" "tdfv_src"
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
			, "calculate_variables"."campaign_code" AS "campaign_code"
			, "calculate_variables"."campaign_start_date" AS "campaign_start_date"
			, "calculate_variables"."contact_id" AS "contact_id"
			, "calculate_variables"."marketing_program_code" AS "marketing_program_code"
			, "calculate_variables"."marketing_program_name" AS "marketing_program_name"
			, "calculate_variables"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_marketing_program_code")
				OVER(PARTITION BY"calculate_variables"."party_number","calculate_variables"."campaign_code","calculate_variables"."campaign_start_date","calculate_variables"."contact_id" ORDER BY "calculate_variables"."load_date")END AS "ch_index_marketing_program_code"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_marketing_program_name")
				OVER(PARTITION BY"calculate_variables"."party_number","calculate_variables"."campaign_code","calculate_variables"."campaign_start_date","calculate_variables"."contact_id" ORDER BY "calculate_variables"."load_date")END AS "ch_index_marketing_program_name"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_update_timestamp")
				OVER(PARTITION BY"calculate_variables"."party_number","calculate_variables"."campaign_code","calculate_variables"."campaign_start_date","calculate_variables"."contact_id" ORDER BY "calculate_variables"."load_date")END AS "ch_index_update_timestamp"
			, "calculate_variables"."nc_marketing_program_code" AS "nc_marketing_program_code"
			, "calculate_variables"."nc_marketing_program_name" AS "nc_marketing_program_name"
			, "calculate_variables"."nc_update_timestamp" AS "nc_update_timestamp"
		FROM "calculate_variables" "calculate_variables"
	)
	, "all_time_slices" AS 
	( 
		SELECT 
			  "sat_src1"."load_date" AS "load_date"
			, "sat_src1"."party_number" AS "party_number"
			, "sat_src1"."campaign_code" AS "campaign_code"
			, "sat_src1"."campaign_start_date" AS "campaign_start_date"
			, "sat_src1"."contact_id" AS "contact_id"
			, 'T' AS "record_type"
		FROM "change_index" "change_index"
		INNER JOIN "moto_scn01_fl"."lds_mm_camp_cust_cont" "sat_src1" ON  "change_index"."party_number" = "sat_src1"."party_number" AND  "change_index"."campaign_code" = "sat_src1"."campaign_code" AND  
			"change_index"."campaign_start_date" = "sat_src1"."campaign_start_date" AND  "change_index"."contact_id" = "sat_src1"."contact_id"
	)
	, "previous_version" AS 
	( 
		SELECT 
			  "all_time_slices"."load_date" AS "load_date"
			, "all_time_slices"."party_number" AS "party_number"
			, "all_time_slices"."campaign_code" AS "campaign_code"
			, "all_time_slices"."campaign_start_date" AS "campaign_start_date"
			, "all_time_slices"."contact_id" AS "contact_id"
			, ROW_NUMBER()OVER(PARTITION BY "all_time_slices"."party_number" ,  "all_time_slices"."campaign_code" ,  
				"all_time_slices"."campaign_start_date" ,  "all_time_slices"."contact_id" ORDER BY "all_time_slices"."load_date" DESC) AS "dummy"
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
			, "change_index"."campaign_code" AS "campaign_code"
			, "change_index"."campaign_start_date" AS "campaign_start_date"
			, "change_index"."contact_id" AS "contact_id"
			, "change_index"."marketing_program_code" AS "marketing_program_code"
			, "change_index"."marketing_program_name" AS "marketing_program_name"
			, "change_index"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "change_index"."nc_marketing_program_code" = 0 THEN 0 ELSE SUM("change_index"."nc_marketing_program_code")
				OVER(PARTITION BY"change_index"."party_number","change_index"."campaign_code","change_index"."campaign_start_date","change_index"."contact_id","change_index"."ch_index_marketing_program_code" ORDER BY "change_index"."load_date")END AS "lag_index_marketing_program_code"
			, CASE WHEN "change_index"."nc_marketing_program_name" = 0 THEN 0 ELSE SUM("change_index"."nc_marketing_program_name")
				OVER(PARTITION BY"change_index"."party_number","change_index"."campaign_code","change_index"."campaign_start_date","change_index"."contact_id","change_index"."ch_index_marketing_program_name" ORDER BY "change_index"."load_date")END AS "lag_index_marketing_program_name"
			, CASE WHEN "change_index"."nc_update_timestamp" = 0 THEN 0 ELSE SUM("change_index"."nc_update_timestamp")
				OVER(PARTITION BY"change_index"."party_number","change_index"."campaign_code","change_index"."campaign_start_date","change_index"."contact_id","change_index"."ch_index_update_timestamp" ORDER BY "change_index"."load_date")END AS "lag_index_update_timestamp"
			, 1 AS "error_code_camp_cust_cont"
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
			, "result_sat_values"."campaign_code" AS "campaign_code"
			, "result_sat_values"."campaign_start_date" AS "campaign_start_date"
			, "result_sat_values"."contact_id" AS "contact_id"
			, "result_sat_values"."marketing_program_code" AS "marketing_program_code"
			, "result_sat_values"."marketing_program_name" AS "marketing_program_name"
			, "result_sat_values"."update_timestamp" AS "update_timestamp"
			, 0 AS "lag_index_marketing_program_code"
			, 0 AS "lag_index_marketing_program_name"
			, 0 AS "lag_index_update_timestamp"
			, 1 AS "error_code_camp_cust_cont"
		FROM "previous_version" "previous_version"
		INNER JOIN "moto_scn01_fl"."lds_mm_camp_cust_cont" "result_sat_values" ON  "previous_version"."party_number" = "result_sat_values"."party_number" AND  "previous_version"."campaign_code" =
			"result_sat_values"."campaign_code" AND  "previous_version"."campaign_start_date" = "result_sat_values"."campaign_start_date" AND  "previous_version"."contact_id" = "result_sat_values"."contact_id" AND "previous_version"."load_date" = "result_sat_values"."load_date" AND "previous_version"."dummy" = 1
		UNION ALL 
		SELECT 
			  "ext_err_src"."load_date" AS "load_date"
			, "err_lci_src"."load_cycle_id" AS "load_cycle_id"
			, "ext_err_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("ext_err_src"."operation",'U') AS "operation"
			, 0 AS "order_operation"
			, 'E' AS "record_type"
			, "ext_err_src"."party_number" AS "party_number"
			, "ext_err_src"."campaign_code" AS "campaign_code"
			, "ext_err_src"."campaign_start_date" AS "campaign_start_date"
			, "ext_err_src"."contact_id" AS "contact_id"
			, "ext_err_src"."marketing_program_code" AS "marketing_program_code"
			, "ext_err_src"."marketing_program_name" AS "marketing_program_name"
			, "ext_err_src"."update_timestamp" AS "update_timestamp"
			, 0 AS "lag_index_marketing_program_code"
			, 0 AS "lag_index_marketing_program_name"
			, 0 AS "lag_index_update_timestamp"
			, CASE WHEN "ext_err_src"."error_code_camp_cust_cont" = 0 THEN 2 ELSE "ext_err_src"."error_code_camp_cust_cont" END AS "error_code_camp_cust_cont"
		FROM "moto_mktg_scn01_ext"."camp_part_cont_err" "ext_err_src"
		INNER JOIN "moto_mktg_scn01_mtd"."load_cycle_info" "err_lci_src" ON  1 = 1
		WHERE  "ext_err_src"."error_code_camp_cust_cont" < 4
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
			, "create_set_nc_values"."campaign_code" AS "campaign_code"
			, "create_set_nc_values"."campaign_start_date" AS "campaign_start_date"
			, "create_set_nc_values"."contact_id" AS "contact_id"
			, LAG("create_set_nc_values"."marketing_program_code", "create_set_nc_values"."lag_index_marketing_program_code"::int)
				OVER(PARTITION BY"create_set_nc_values"."party_number","create_set_nc_values"."campaign_code","create_set_nc_values"."campaign_start_date","create_set_nc_values"."contact_id" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "marketing_program_code"
			, LAG("create_set_nc_values"."marketing_program_name", "create_set_nc_values"."lag_index_marketing_program_name"::int)
				OVER(PARTITION BY"create_set_nc_values"."party_number","create_set_nc_values"."campaign_code","create_set_nc_values"."campaign_start_date","create_set_nc_values"."contact_id" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "marketing_program_name"
			, LAG("create_set_nc_values"."update_timestamp", "create_set_nc_values"."lag_index_update_timestamp"::int)
				OVER(PARTITION BY"create_set_nc_values"."party_number","create_set_nc_values"."campaign_code","create_set_nc_values"."campaign_start_date","create_set_nc_values"."contact_id" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "update_timestamp"
			, "create_set_nc_values"."error_code_camp_cust_cont" AS "error_code_camp_cust_cont"
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
			, "result_set_nc_values"."campaign_code" AS "campaign_code"
			, "result_set_nc_values"."campaign_start_date" AS "campaign_start_date"
			, "result_set_nc_values"."contact_id" AS "contact_id"
			, COALESCE(UPPER( "result_set_nc_values"."contact_id"::text),"mex_src_bk"."key_attribute_numeric") AS "contact_id_fk_contactid_bk"
			, COALESCE(UPPER(REPLACE(TRIM("result_set_nc_values"."campaign_code"),'#','\' || '#')),"mex_src_bk"."key_attribute_varchar") AS "campaign_code_fk_campaignstartdate_bk"
			, COALESCE(UPPER( TO_CHAR("result_set_nc_values"."campaign_start_date", 'DD/MM/YYYY'::varchar)),"mex_src_bk"."key_attribute_date") AS "campaign_start_date_fk_campaignstartdate_bk"
			, "result_set_nc_values"."marketing_program_code" AS "marketing_program_code"
			, "result_set_nc_values"."marketing_program_name" AS "marketing_program_name"
			, "result_set_nc_values"."update_timestamp" AS "update_timestamp"
			, "result_set_nc_values"."error_code_camp_cust_cont" AS "error_code_camp_cust_cont"
		FROM "result_set_nc_values" "result_set_nc_values"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src_bk" ON  "mex_src_bk"."record_type" = 'N'
		WHERE  "result_set_nc_values"."record_type" != 'T'
	)
	SELECT 
		  "calculate_bk"."load_cycle_id" AS "load_cycle_id"
		, CASE WHEN "calculate_bk"."record_type" != 'E' THEN TO_TIMESTAMP(NULL , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
			ELSE"calculate_bk"."load_date" END AS "load_date"
		, "calculate_bk"."trans_timestamp" AS "trans_timestamp"
		, "calculate_bk"."operation" AS "operation"
		, "calculate_bk"."record_type" AS "record_type"
		, "calculate_bk"."party_number" AS "party_number"
		, "calculate_bk"."campaign_code" AS "campaign_code"
		, "calculate_bk"."campaign_start_date" AS "campaign_start_date"
		, "calculate_bk"."contact_id" AS "contact_id"
		, "calculate_bk"."contact_id_fk_contactid_bk" AS "contact_id_fk_contactid_bk"
		, "calculate_bk"."campaign_code_fk_campaignstartdate_bk" AS "campaign_code_fk_campaignstartdate_bk"
		, "calculate_bk"."campaign_start_date_fk_campaignstartdate_bk" AS "campaign_start_date_fk_campaignstartdate_bk"
		, "calculate_bk"."marketing_program_code" AS "marketing_program_code"
		, "calculate_bk"."marketing_program_name" AS "marketing_program_name"
		, "calculate_bk"."update_timestamp" AS "update_timestamp"
		, "calculate_bk"."error_code_camp_cust_cont" AS "error_code_camp_cust_cont"
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 
