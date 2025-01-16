CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_mm_contacts_incr"() 
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

	TRUNCATE TABLE "moto_mktg_scn01_ext"."contacts"  CASCADE;

	INSERT INTO "moto_mktg_scn01_ext"."contacts"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"contact_id"
		,"contact_id_bk"
		,"contact_type"
		,"contact_type_desc"
		,"update_timestamp"
	)
	WITH "calculate_variables" AS 
	( 
		SELECT 
			  "lci_src"."load_cycle_id" AS "load_cycle_id"
			, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "tdfv_src"."contact_id"  ORDER BY  "tdfv_src"."trans_timestamp")
				* interval'2 microsecond'   AS "load_date"
			, "tdfv_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("tdfv_src"."operation","mex_src"."attribute_varchar") AS "operation"
			, "tdfv_src"."record_type" AS "record_type"
			, "tdfv_src"."contact_id" AS "contact_id"
			, "tdfv_src"."contact_type" AS "contact_type"
			, "tdfv_src"."contact_type_desc" AS "contact_type_desc"
			, "tdfv_src"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."contact_type" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_contact_type"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."contact_type_desc" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_contact_type_desc"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."update_timestamp" = TO_TIMESTAMP('01/01/1970 00:00:00',
				'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN 0 ELSE 1 END AS "ch_update_timestamp"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."contact_type" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_contact_type"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."contact_type_desc" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_contact_type_desc"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."update_timestamp" = TO_TIMESTAMP('01/01/1970 00:00:00',
				'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN 1 ELSE 0 END AS "nc_update_timestamp"
		FROM "moto_mktg_scn01_dfv"."vw_contacts" "tdfv_src"
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
			, "calculate_variables"."contact_id" AS "contact_id"
			, "calculate_variables"."contact_type" AS "contact_type"
			, "calculate_variables"."contact_type_desc" AS "contact_type_desc"
			, "calculate_variables"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_contact_type")
				OVER(PARTITION BY"calculate_variables"."contact_id" ORDER BY "calculate_variables"."load_date")END AS "ch_index_contact_type"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_contact_type_desc")
				OVER(PARTITION BY"calculate_variables"."contact_id" ORDER BY "calculate_variables"."load_date")END AS "ch_index_contact_type_desc"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_update_timestamp")
				OVER(PARTITION BY"calculate_variables"."contact_id" ORDER BY "calculate_variables"."load_date")END AS "ch_index_update_timestamp"
			, "calculate_variables"."nc_contact_type" AS "nc_contact_type"
			, "calculate_variables"."nc_contact_type_desc" AS "nc_contact_type_desc"
			, "calculate_variables"."nc_update_timestamp" AS "nc_update_timestamp"
		FROM "calculate_variables" "calculate_variables"
	)
	, "all_time_slices" AS 
	( 
		SELECT 
			  "sat_src1"."load_date" AS "load_date"
			, "sat_src1"."contact_id" AS "contact_id"
			, 'T' AS "record_type"
		FROM "change_index" "change_index"
		INNER JOIN "moto_scn01_fl"."sat_mm_contacts" "sat_src1" ON  "change_index"."contact_id" = "sat_src1"."contact_id"
	)
	, "previous_version" AS 
	( 
		SELECT 
			  "all_time_slices"."load_date" AS "load_date"
			, "all_time_slices"."contact_id" AS "contact_id"
			, ROW_NUMBER()OVER(PARTITION BY "all_time_slices"."contact_id" ORDER BY "all_time_slices"."load_date" DESC) AS "dummy"
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
			, "change_index"."contact_id" AS "contact_id"
			, "change_index"."contact_type" AS "contact_type"
			, "change_index"."contact_type_desc" AS "contact_type_desc"
			, "change_index"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "change_index"."nc_contact_type" = 0 THEN 0 ELSE SUM("change_index"."nc_contact_type")
				OVER(PARTITION BY"change_index"."contact_id","change_index"."ch_index_contact_type" ORDER BY "change_index"."load_date")END AS "lag_index_contact_type"
			, CASE WHEN "change_index"."nc_contact_type_desc" = 0 THEN 0 ELSE SUM("change_index"."nc_contact_type_desc")
				OVER(PARTITION BY"change_index"."contact_id","change_index"."ch_index_contact_type_desc" ORDER BY "change_index"."load_date")END AS "lag_index_contact_type_desc"
			, CASE WHEN "change_index"."nc_update_timestamp" = 0 THEN 0 ELSE SUM("change_index"."nc_update_timestamp")
				OVER(PARTITION BY"change_index"."contact_id","change_index"."ch_index_update_timestamp" ORDER BY "change_index"."load_date")END AS "lag_index_update_timestamp"
		FROM "change_index" "change_index"
		UNION ALL 
		SELECT 
			  "result_sat_values"."load_date" AS "load_date"
			, "result_sat_values"."load_cycle_id" AS "load_cycle_id"
			, "result_sat_values"."trans_timestamp" AS "trans_timestamp"
			, 'SAT' AS "operation"
			, 0 AS "order_operation"
			, 'T' AS "record_type"
			, "result_sat_values"."contact_id" AS "contact_id"
			, "result_sat_values"."contact_type" AS "contact_type"
			, "result_sat_values"."contact_type_desc" AS "contact_type_desc"
			, "result_sat_values"."update_timestamp" AS "update_timestamp"
			, 0 AS "lag_index_contact_type"
			, 0 AS "lag_index_contact_type_desc"
			, 0 AS "lag_index_update_timestamp"
		FROM "previous_version" "previous_version"
		INNER JOIN "moto_scn01_fl"."sat_mm_contacts" "result_sat_values" ON  "previous_version"."contact_id" = "result_sat_values"."contact_id" AND "previous_version"."load_date" = 
			"result_sat_values"."load_date" AND "previous_version"."dummy" = 1
		INNER JOIN "moto_scn01_fl"."hub_contacts" "hub_src" ON  "result_sat_values"."contacts_hkey" = "hub_src"."contacts_hkey"
	)
	, "result_set_nc_values" AS 
	( 
		SELECT 
			  "create_set_nc_values"."load_cycle_id" AS "load_cycle_id"
			, "create_set_nc_values"."load_date" AS "load_date"
			, "create_set_nc_values"."trans_timestamp" AS "trans_timestamp"
			, "create_set_nc_values"."operation" AS "operation"
			, "create_set_nc_values"."record_type" AS "record_type"
			, "create_set_nc_values"."contact_id" AS "contact_id"
			, LAG("create_set_nc_values"."contact_type", "create_set_nc_values"."lag_index_contact_type"::int)
				OVER(PARTITION BY"create_set_nc_values"."contact_id" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "contact_type"
			, LAG("create_set_nc_values"."contact_type_desc", "create_set_nc_values"."lag_index_contact_type_desc"::int)
				OVER(PARTITION BY"create_set_nc_values"."contact_id" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "contact_type_desc"
			, LAG("create_set_nc_values"."update_timestamp", "create_set_nc_values"."lag_index_update_timestamp"::int)
				OVER(PARTITION BY"create_set_nc_values"."contact_id" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "update_timestamp"
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
			, "result_set_nc_values"."contact_id" AS "contact_id"
			, COALESCE(UPPER( "result_set_nc_values"."contact_id"::text),"mex_src_bk"."key_attribute_numeric") AS "contact_id_bk"
			, "result_set_nc_values"."contact_type" AS "contact_type"
			, "result_set_nc_values"."contact_type_desc" AS "contact_type_desc"
			, "result_set_nc_values"."update_timestamp" AS "update_timestamp"
		FROM "result_set_nc_values" "result_set_nc_values"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src_bk" ON  "mex_src_bk"."record_type" = 'N'
		WHERE  "result_set_nc_values"."record_type" != 'T'
	)
	SELECT 
		  "calculate_bk"."load_cycle_id" AS "load_cycle_id"
		, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "calculate_bk"."contact_id_bk"  ORDER BY  "calculate_bk"."trans_timestamp")
			* interval'2 microsecond'   AS "load_date"
		, "calculate_bk"."trans_timestamp" AS "trans_timestamp"
		, "calculate_bk"."operation" AS "operation"
		, "calculate_bk"."record_type" AS "record_type"
		, "calculate_bk"."contact_id" AS "contact_id"
		, "calculate_bk"."contact_id_bk" AS "contact_id_bk"
		, "calculate_bk"."contact_type" AS "contact_type"
		, "calculate_bk"."contact_type_desc" AS "contact_type_desc"
		, "calculate_bk"."update_timestamp" AS "update_timestamp"
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 
