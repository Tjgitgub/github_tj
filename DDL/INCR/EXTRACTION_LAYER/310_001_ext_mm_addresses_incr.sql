CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_mm_addresses_incr"() 
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

	TRUNCATE TABLE "moto_mktg_scn01_ext"."addresses"  CASCADE;

	INSERT INTO "moto_mktg_scn01_ext"."addresses"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"address_number"
		,"street_name_bk"
		,"street_number_bk"
		,"postal_code_bk"
		,"city_bk"
		,"street_name"
		,"street_number"
		,"postal_code"
		,"city"
		,"province"
		,"update_timestamp"
	)
	WITH "calculate_variables" AS 
	( 
		SELECT 
			  "lci_src"."load_cycle_id" AS "load_cycle_id"
			, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "tdfv_src"."address_number"  ORDER BY  "tdfv_src"."trans_timestamp")
				* interval'2 microsecond'   AS "load_date"
			, "tdfv_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("tdfv_src"."operation","mex_src"."attribute_varchar") AS "operation"
			, "tdfv_src"."record_type" AS "record_type"
			, "tdfv_src"."address_number" AS "address_number"
			, "tdfv_src"."street_name" AS "street_name"
			, "tdfv_src"."street_number" AS "street_number"
			, "tdfv_src"."postal_code" AS "postal_code"
			, "tdfv_src"."city" AS "city"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."street_name" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_street_name"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."street_number" = TO_NUMBER('-1', 
				'999999999999D999999999'::varchar) THEN 0 ELSE 1 END AS "ch_street_number"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."postal_code" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_postal_code"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."city" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_city"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."street_name" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_street_name"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."street_number" = TO_NUMBER('-1', 
				'999999999999D999999999'::varchar) THEN 1 ELSE 0 END AS "nc_street_number"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."postal_code" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_postal_code"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."city" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_city"
			, "tdfv_src"."province" AS "province"
			, "tdfv_src"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."province" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_province"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."update_timestamp" = TO_TIMESTAMP('01/01/1970 00:00:00',
				 'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN 0 ELSE 1 END AS "ch_update_timestamp"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."province" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_province"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."update_timestamp" = TO_TIMESTAMP('01/01/1970 00:00:00',
				 'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN 1 ELSE 0 END AS "nc_update_timestamp"
		FROM "moto_mktg_scn01_dfv"."vw_addresses" "tdfv_src"
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
			, "calculate_variables"."address_number" AS "address_number"
			, "calculate_variables"."street_name" AS "street_name"
			, "calculate_variables"."street_number" AS "street_number"
			, "calculate_variables"."postal_code" AS "postal_code"
			, "calculate_variables"."city" AS "city"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_street_name")
				OVER(PARTITION BY "calculate_variables"."address_number" ORDER BY "calculate_variables"."load_date")END AS "ch_index_street_name"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_street_number")
				OVER(PARTITION BY "calculate_variables"."address_number" ORDER BY "calculate_variables"."load_date")END AS "ch_index_street_number"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_postal_code")
				OVER(PARTITION BY "calculate_variables"."address_number" ORDER BY "calculate_variables"."load_date")END AS "ch_index_postal_code"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_city")
				OVER(PARTITION BY "calculate_variables"."address_number" ORDER BY "calculate_variables"."load_date")END AS "ch_index_city"
			, "calculate_variables"."nc_street_name" AS "nc_street_name"
			, "calculate_variables"."nc_street_number" AS "nc_street_number"
			, "calculate_variables"."nc_postal_code" AS "nc_postal_code"
			, "calculate_variables"."nc_city" AS "nc_city"
			, "calculate_variables"."province" AS "province"
			, "calculate_variables"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_province")
				OVER(PARTITION BY "calculate_variables"."address_number" ORDER BY "calculate_variables"."load_date")END AS "ch_index_province"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_update_timestamp")
				OVER(PARTITION BY "calculate_variables"."address_number" ORDER BY "calculate_variables"."load_date")END AS "ch_index_update_timestamp"
			, "calculate_variables"."nc_province" AS "nc_province"
			, "calculate_variables"."nc_update_timestamp" AS "nc_update_timestamp"
		FROM "calculate_variables" "calculate_variables"
	)
	, "all_time_slices" AS 
	( 
		SELECT 
			  "sat_src1"."load_date" AS "load_date"
			, "sat_src1"."address_number" AS "address_number"
			, 'T' AS "record_type"
		FROM "change_index" "change_index"
		INNER JOIN "moto_scn01_fl"."sat_mm_addresses" "sat_src1" ON  "change_index"."address_number" = "sat_src1"."address_number"
	)
	, "previous_version" AS 
	( 
		SELECT 
			  "all_time_slices"."load_date" AS "load_date"
			, "all_time_slices"."address_number" AS "address_number"
			, ROW_NUMBER()OVER(PARTITION BY "all_time_slices"."address_number" ORDER BY "all_time_slices"."load_date" DESC) AS "dummy"
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
			, "change_index"."address_number" AS "address_number"
			, "change_index"."street_name" AS "street_name"
			, "change_index"."street_number" AS "street_number"
			, "change_index"."postal_code" AS "postal_code"
			, "change_index"."city" AS "city"
			, CASE WHEN "change_index"."nc_street_name" = 0 THEN 0 ELSE SUM("change_index"."nc_street_name")
				OVER(PARTITION BY "change_index"."address_number","change_index"."ch_index_street_name" ORDER BY "change_index"."load_date")END AS "lag_index_street_name"
			, CASE WHEN "change_index"."nc_street_number" = 0 THEN 0 ELSE SUM("change_index"."nc_street_number")
				OVER(PARTITION BY "change_index"."address_number","change_index"."ch_index_street_number" ORDER BY "change_index"."load_date")END AS "lag_index_street_number"
			, CASE WHEN "change_index"."nc_postal_code" = 0 THEN 0 ELSE SUM("change_index"."nc_postal_code")
				OVER(PARTITION BY "change_index"."address_number","change_index"."ch_index_postal_code" ORDER BY "change_index"."load_date")END AS "lag_index_postal_code"
			, CASE WHEN "change_index"."nc_city" = 0 THEN 0 ELSE SUM("change_index"."nc_city")OVER(PARTITION BY "change_index"."address_number",
				"change_index"."ch_index_city" ORDER BY "change_index"."load_date")END AS "lag_index_city"
			, "change_index"."province" AS "province"
			, "change_index"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "change_index"."nc_province" = 0 THEN 0 ELSE SUM("change_index"."nc_province")OVER(PARTITION BY "change_index"."address_number",
				"change_index"."ch_index_province" ORDER BY "change_index"."load_date")END AS "lag_index_province"
			, CASE WHEN "change_index"."nc_update_timestamp" = 0 THEN 0 ELSE SUM("change_index"."nc_update_timestamp")
				OVER(PARTITION BY "change_index"."address_number","change_index"."ch_index_update_timestamp" ORDER BY "change_index"."load_date")END AS "lag_index_update_timestamp"
		FROM "change_index" "change_index"
		UNION ALL 
		SELECT 
			  "result_sat_values"."load_date" AS "load_date"
			, "result_sat_values"."load_cycle_id" AS "load_cycle_id"
			, "result_sat_values"."trans_timestamp" AS "trans_timestamp"
			, 'SAT' AS "operation"
			, 0 AS "order_operation"
			, 'T' AS "record_type"
			, "result_sat_values"."address_number" AS "address_number"
			, "result_sat_values"."street_name" AS "street_name"
			, "result_sat_values"."street_number" AS "street_number"
			, "result_sat_values"."postal_code" AS "postal_code"
			, "result_sat_values"."city" AS "city"
			, 0 AS "lag_index_street_name"
			, 0 AS "lag_index_street_number"
			, 0 AS "lag_index_postal_code"
			, 0 AS "lag_index_city"
			, "result_sat_values"."province" AS "province"
			, "result_sat_values"."update_timestamp" AS "update_timestamp"
			, 0 AS "lag_index_province"
			, 0 AS "lag_index_update_timestamp"
		FROM "previous_version" "previous_version"
		INNER JOIN "moto_scn01_fl"."sat_mm_addresses" "result_sat_values" ON  "previous_version"."address_number" = "result_sat_values"."address_number" AND "previous_version"."load_date" =
			 "result_sat_values"."load_date" AND "previous_version"."dummy" = 1
	)
	, "result_set_nc_values" AS 
	( 
		SELECT 
			  "create_set_nc_values"."load_cycle_id" AS "load_cycle_id"
			, "create_set_nc_values"."load_date" AS "load_date"
			, "create_set_nc_values"."trans_timestamp" AS "trans_timestamp"
			, "create_set_nc_values"."operation" AS "operation"
			, "create_set_nc_values"."record_type" AS "record_type"
			, "create_set_nc_values"."address_number" AS "address_number"
			, LAG("create_set_nc_values"."street_name", "create_set_nc_values"."lag_index_street_name"::int)
				OVER(PARTITION BY "create_set_nc_values"."address_number" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "street_name"
			, LAG("create_set_nc_values"."street_number", "create_set_nc_values"."lag_index_street_number"::int)
				OVER(PARTITION BY "create_set_nc_values"."address_number" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "street_number"
			, LAG("create_set_nc_values"."postal_code", "create_set_nc_values"."lag_index_postal_code"::int)
				OVER(PARTITION BY "create_set_nc_values"."address_number" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "postal_code"
			, LAG("create_set_nc_values"."city", "create_set_nc_values"."lag_index_city"::int)OVER(PARTITION BY "create_set_nc_values"."address_number" ORDER BY "create_set_nc_values"."load_date",
				"create_set_nc_values"."order_operation") AS "city"
			, LAG("create_set_nc_values"."province", "create_set_nc_values"."lag_index_province"::int)OVER(PARTITION BY "create_set_nc_values"."address_number" ORDER BY "create_set_nc_values"."load_date",
				"create_set_nc_values"."order_operation") AS "province"
			, LAG("create_set_nc_values"."update_timestamp", "create_set_nc_values"."lag_index_update_timestamp"::int)
				OVER(PARTITION BY "create_set_nc_values"."address_number" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "update_timestamp"
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
			, "result_set_nc_values"."address_number" AS "address_number"
			, COALESCE(UPPER(REPLACE(TRIM("result_set_nc_values"."street_name"),'#','\' || '#')),"mex_src_bk"."key_attribute_varchar") AS "street_name_bk"
			, COALESCE(UPPER( "result_set_nc_values"."street_number"::text),"mex_src_bk"."key_attribute_numeric") AS "street_number_bk"
			, COALESCE(UPPER(REPLACE(TRIM("result_set_nc_values"."postal_code"),'#','\' || '#')),"mex_src_bk"."key_attribute_varchar") AS "postal_code_bk"
			, COALESCE(UPPER(REPLACE(TRIM("result_set_nc_values"."city"),'#','\' || '#')),"mex_src_bk"."key_attribute_varchar") AS "city_bk"
			, "result_set_nc_values"."street_name" AS "street_name"
			, "result_set_nc_values"."street_number" AS "street_number"
			, "result_set_nc_values"."postal_code" AS "postal_code"
			, "result_set_nc_values"."city" AS "city"
			, "result_set_nc_values"."province" AS "province"
			, "result_set_nc_values"."update_timestamp" AS "update_timestamp"
		FROM "result_set_nc_values" "result_set_nc_values"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src_bk" ON  "mex_src_bk"."record_type" = 'N'
		WHERE  "result_set_nc_values"."record_type" != 'T'
	)
	SELECT 
		  "calculate_bk"."load_cycle_id" AS "load_cycle_id"
		, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "calculate_bk"."street_name_bk" ,  "calculate_bk"."street_number_bk" ,
			  "calculate_bk"."postal_code_bk" ,  "calculate_bk"."city_bk"  ORDER BY  "calculate_bk"."trans_timestamp") * interval'2 microsecond'   AS "load_date"
		, "calculate_bk"."trans_timestamp" AS "trans_timestamp"
		, "calculate_bk"."operation" AS "operation"
		, "calculate_bk"."record_type" AS "record_type"
		, "calculate_bk"."address_number" AS "address_number"
		, "calculate_bk"."street_name_bk" AS "street_name_bk"
		, "calculate_bk"."street_number_bk" AS "street_number_bk"
		, "calculate_bk"."postal_code_bk" AS "postal_code_bk"
		, "calculate_bk"."city_bk" AS "city_bk"
		, "calculate_bk"."street_name" AS "street_name"
		, "calculate_bk"."street_number" AS "street_number"
		, "calculate_bk"."postal_code" AS "postal_code"
		, "calculate_bk"."city" AS "city"
		, "calculate_bk"."province" AS "province"
		, "calculate_bk"."update_timestamp" AS "update_timestamp"
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 
