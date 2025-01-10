CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_mm_channels_incr"() 
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

Vaultspeed version: 5.7.2.14, generation date: 2025/01/09 12:48:54
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/09 09:40:46, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51
 */


BEGIN 

BEGIN -- ext_tgt

	TRUNCATE TABLE "moto_mktg_scn01_ext"."channels"  CASCADE;

	INSERT INTO "moto_mktg_scn01_ext"."channels"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"channel_id"
		,"channel_code_bk"
		,"channel_description"
		,"update_timestamp"
	)
	WITH "calculate_variables" AS 
	( 
		SELECT 
			  "lci_src"."load_cycle_id" AS "load_cycle_id"
			, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "tdfv_src"."channel_id"  ORDER BY  "tdfv_src"."trans_timestamp")
				* interval'2 microsecond'   AS "load_date"
			, "tdfv_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("tdfv_src"."operation","mex_src"."attribute_varchar") AS "operation"
			, "tdfv_src"."record_type" AS "record_type"
			, "tdfv_src"."channel_id" AS "channel_id"
			, "tdfv_src"."channel_code" AS "channel_code"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."channel_code" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_channel_code"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."channel_code" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_channel_code"
			, "tdfv_src"."channel_description" AS "channel_description"
			, "tdfv_src"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."channel_description" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_channel_description"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."update_timestamp" = TO_TIMESTAMP('01/01/1970 00:00:00',
				 'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN 0 ELSE 1 END AS "ch_update_timestamp"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."channel_description" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_channel_description"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."update_timestamp" = TO_TIMESTAMP('01/01/1970 00:00:00',
				 'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN 1 ELSE 0 END AS "nc_update_timestamp"
		FROM "moto_mktg_scn01_dfv"."vw_channels" "tdfv_src"
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
			, "calculate_variables"."channel_id" AS "channel_id"
			, "calculate_variables"."channel_code" AS "channel_code"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_channel_code")
				OVER(PARTITION BY "calculate_variables"."channel_id" ORDER BY "calculate_variables"."load_date")END AS "ch_index_channel_code"
			, "calculate_variables"."nc_channel_code" AS "nc_channel_code"
			, "calculate_variables"."channel_description" AS "channel_description"
			, "calculate_variables"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_channel_description")
				OVER(PARTITION BY "calculate_variables"."channel_id" ORDER BY "calculate_variables"."load_date")END AS "ch_index_channel_description"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_update_timestamp")
				OVER(PARTITION BY "calculate_variables"."channel_id" ORDER BY "calculate_variables"."load_date")END AS "ch_index_update_timestamp"
			, "calculate_variables"."nc_channel_description" AS "nc_channel_description"
			, "calculate_variables"."nc_update_timestamp" AS "nc_update_timestamp"
		FROM "calculate_variables" "calculate_variables"
	)
	, "all_time_slices" AS 
	( 
		SELECT 
			  "sat_src1"."load_date" AS "load_date"
			, "sat_src1"."channel_id" AS "channel_id"
			, 'T' AS "record_type"
		FROM "change_index" "change_index"
		INNER JOIN "moto_scn01_fl"."sat_mm_channels" "sat_src1" ON  "change_index"."channel_id" = "sat_src1"."channel_id"
	)
	, "previous_version" AS 
	( 
		SELECT 
			  "all_time_slices"."load_date" AS "load_date"
			, "all_time_slices"."channel_id" AS "channel_id"
			, ROW_NUMBER()OVER(PARTITION BY "all_time_slices"."channel_id" ORDER BY "all_time_slices"."load_date" DESC) AS "dummy"
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
			, "change_index"."channel_id" AS "channel_id"
			, "change_index"."channel_code" AS "channel_code"
			, CASE WHEN "change_index"."nc_channel_code" = 0 THEN 0 ELSE SUM("change_index"."nc_channel_code")
				OVER(PARTITION BY "change_index"."channel_id","change_index"."ch_index_channel_code" ORDER BY "change_index"."load_date")END AS "lag_index_channel_code"
			, "change_index"."channel_description" AS "channel_description"
			, "change_index"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "change_index"."nc_channel_description" = 0 THEN 0 ELSE SUM("change_index"."nc_channel_description")
				OVER(PARTITION BY "change_index"."channel_id","change_index"."ch_index_channel_description" ORDER BY "change_index"."load_date")END AS "lag_index_channel_description"
			, CASE WHEN "change_index"."nc_update_timestamp" = 0 THEN 0 ELSE SUM("change_index"."nc_update_timestamp")
				OVER(PARTITION BY "change_index"."channel_id","change_index"."ch_index_update_timestamp" ORDER BY "change_index"."load_date")END AS "lag_index_update_timestamp"
		FROM "change_index" "change_index"
		UNION ALL 
		SELECT 
			  "result_sat_values"."load_date" AS "load_date"
			, "result_sat_values"."load_cycle_id" AS "load_cycle_id"
			, "result_sat_values"."trans_timestamp" AS "trans_timestamp"
			, 'SAT' AS "operation"
			, 0 AS "order_operation"
			, 'T' AS "record_type"
			, "result_sat_values"."channel_id" AS "channel_id"
			, "hub_src"."channel_code_bk"::text AS "channel_code"
			, 0 AS "lag_index_channel_code"
			, "result_sat_values"."channel_description" AS "channel_description"
			, "result_sat_values"."update_timestamp" AS "update_timestamp"
			, 0 AS "lag_index_channel_description"
			, 0 AS "lag_index_update_timestamp"
		FROM "previous_version" "previous_version"
		INNER JOIN "moto_scn01_fl"."sat_mm_channels" "result_sat_values" ON  "previous_version"."channel_id" = "result_sat_values"."channel_id" AND "previous_version"."load_date" = 
			"result_sat_values"."load_date" AND "previous_version"."dummy" = 1
		INNER JOIN "moto_scn01_fl"."hub_channels" "hub_src" ON  "result_sat_values"."channels_hkey" = "hub_src"."channels_hkey"
	)
	, "result_set_nc_values" AS 
	( 
		SELECT 
			  "create_set_nc_values"."load_cycle_id" AS "load_cycle_id"
			, "create_set_nc_values"."load_date" AS "load_date"
			, "create_set_nc_values"."trans_timestamp" AS "trans_timestamp"
			, "create_set_nc_values"."operation" AS "operation"
			, "create_set_nc_values"."record_type" AS "record_type"
			, "create_set_nc_values"."channel_id" AS "channel_id"
			, LAG("create_set_nc_values"."channel_code", "create_set_nc_values"."lag_index_channel_code"::int)
				OVER(PARTITION BY "create_set_nc_values"."channel_id" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "channel_code"
			, LAG("create_set_nc_values"."channel_description", "create_set_nc_values"."lag_index_channel_description"::int)
				OVER(PARTITION BY "create_set_nc_values"."channel_id" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "channel_description"
			, LAG("create_set_nc_values"."update_timestamp", "create_set_nc_values"."lag_index_update_timestamp"::int)
				OVER(PARTITION BY "create_set_nc_values"."channel_id" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "update_timestamp"
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
			, "result_set_nc_values"."channel_id" AS "channel_id"
			, COALESCE(UPPER(REPLACE(TRIM("result_set_nc_values"."channel_code"),'#','\' || '#')),"mex_src_bk"."key_attribute_varchar") AS "channel_code_bk"
			, "result_set_nc_values"."channel_description" AS "channel_description"
			, "result_set_nc_values"."update_timestamp" AS "update_timestamp"
		FROM "result_set_nc_values" "result_set_nc_values"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src_bk" ON  "mex_src_bk"."record_type" = 'N'
		WHERE  "result_set_nc_values"."record_type" != 'T'
	)
	SELECT 
		  "calculate_bk"."load_cycle_id" AS "load_cycle_id"
		, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "calculate_bk"."channel_code_bk"  ORDER BY  "calculate_bk"."trans_timestamp")
			* interval'2 microsecond'   AS "load_date"
		, "calculate_bk"."trans_timestamp" AS "trans_timestamp"
		, "calculate_bk"."operation" AS "operation"
		, "calculate_bk"."record_type" AS "record_type"
		, "calculate_bk"."channel_id" AS "channel_id"
		, "calculate_bk"."channel_code_bk" AS "channel_code_bk"
		, "calculate_bk"."channel_description" AS "channel_description"
		, "calculate_bk"."update_timestamp" AS "update_timestamp"
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 
