CREATE OR REPLACE FUNCTION "moto_scn01_proc"."sat_ms_prse_incr"() 
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

Vaultspeed version: 5.7.2.14, generation date: 2025/01/09 12:47:43
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/09 09:40:46, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04
 */


BEGIN 

BEGIN -- sat_temp_tgt

	TRUNCATE TABLE "moto_sales_scn01_stg"."sat_ms_product_sensors_tmp"  CASCADE;

	INSERT INTO "moto_sales_scn01_stg"."sat_ms_product_sensors_tmp"(
		 "product_sensors_hkey"
		,"load_date"
		,"load_end_date"
		,"load_cycle_id"
		,"record_type"
		,"source"
		,"equal"
		,"hash_diff"
		,"delete_flag"
		,"vehicle_number"
		,"subsequence_seq"
		,"sensor"
		,"sensor_value"
		,"unit_of_measurement"
	)
	WITH "dist_stg" AS 
	( 
		SELECT DISTINCT 
 			  "stg_dis_src"."product_sensors_hkey" AS "product_sensors_hkey"
			, "stg_dis_src"."load_cycle_id" AS "load_cycle_id"
		FROM "moto_sales_scn01_stg"."product_sensors" "stg_dis_src"
		WHERE  "stg_dis_src"."record_type" = 'S'
	)
	, "hash_diff_calc" AS 
	( 
		SELECT 
			  "stg_chd_src"."product_sensors_hkey" AS "product_sensors_hkey"
			, "stg_chd_src"."load_date" AS "load_date"
			, "stg_chd_src"."load_cycle_id" AS "load_cycle_id"
			, "stg_chd_src"."record_type" AS "record_type"
			, UPPER(ENCODE(DIGEST(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "stg_chd_src"."sensor"),'~'),'#','\' || '#')
				)|| '#' ||  UPPER(REPLACE(COALESCE(TRIM( "stg_chd_src"."sensor_value"::text),'~'),'#','\' || '#'))|| '#' ||  UPPER(REPLACE(COALESCE(TRIM( "stg_chd_src"."unit_of_measurement"),'~'),'#','\' || '#'))|| '#','#' || '~'),'~') ,'MD5'),'HEX')) AS "hash_diff"
			, CASE WHEN "stg_chd_src"."operation" = 'D' THEN 'Y'::text ELSE 'N'::text END AS "delete_flag"
			, "stg_chd_src"."vehicle_number" AS "vehicle_number"
			, "stg_chd_src"."subsequence_seq" AS "subsequence_seq"
			, "stg_chd_src"."sensor" AS "sensor"
			, "stg_chd_src"."sensor_value" AS "sensor_value"
			, "stg_chd_src"."unit_of_measurement" AS "unit_of_measurement"
		FROM "moto_sales_scn01_stg"."product_sensors" "stg_chd_src"
		WHERE  "stg_chd_src"."record_type" = 'S'
	)
	, "temp_table_set" AS 
	( 
		SELECT 
			  "hash_diff_calc"."product_sensors_hkey" AS "product_sensors_hkey"
			, "hash_diff_calc"."load_date" AS "load_date"
			, "hash_diff_calc"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, "hash_diff_calc"."record_type" AS "record_type"
			, 'STG' AS "source"
			, 1 AS "origin_id"
			, "hash_diff_calc"."hash_diff" AS "hash_diff"
			, COUNT(*)OVER(PARTITION BY "hash_diff_calc"."product_sensors_hkey") AS "count_multi_active"
			, "hash_diff_calc"."delete_flag" AS "delete_flag"
			, "hash_diff_calc"."vehicle_number" AS "vehicle_number"
			, "hash_diff_calc"."subsequence_seq" AS "subsequence_seq"
			, "hash_diff_calc"."hash_diff"::text AS "char_hash_diff"
			, "hash_diff_calc"."delete_flag"::text AS "char_delete_flag"
			, "hash_diff_calc"."sensor" AS "sensor"
			, "hash_diff_calc"."sensor_value" AS "sensor_value"
			, "hash_diff_calc"."unit_of_measurement" AS "unit_of_measurement"
		FROM "hash_diff_calc" "hash_diff_calc"
		UNION ALL 
		SELECT 
			  "sat_src"."product_sensors_hkey" AS "product_sensors_hkey"
			, "sat_src"."load_date" AS "load_date"
			, "sat_src"."load_cycle_id" AS "load_cycle_id"
			, "sat_src"."load_end_date" AS "load_end_date"
			, 'SAT' AS "record_type"
			, 'SAT' AS "source"
			, 0 AS "origin_id"
			, "sat_src"."hash_diff" AS "hash_diff"
			, COUNT(*)OVER(PARTITION BY "sat_src"."product_sensors_hkey") AS "count_multi_active"
			, "sat_src"."delete_flag" AS "delete_flag"
			, "sat_src"."vehicle_number" AS "vehicle_number"
			, "sat_src"."subsequence_seq" AS "subsequence_seq"
			, "sat_src"."hash_diff"::text AS "char_hash_diff"
			, "sat_src"."delete_flag"::text AS "char_delete_flag"
			, "sat_src"."sensor" AS "sensor"
			, "sat_src"."sensor_value" AS "sensor_value"
			, "sat_src"."unit_of_measurement" AS "unit_of_measurement"
		FROM "moto_scn01_fl"."sat_ms_product_sensors" "sat_src"
		INNER JOIN "dist_stg" "dist_stg" ON  "sat_src"."product_sensors_hkey" = "dist_stg"."product_sensors_hkey"
		WHERE  "sat_src"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
	)
	, "agg_hash_diff" AS 
	( 
		SELECT 
			  "temp_table_set"."product_sensors_hkey" AS "product_sensors_hkey"
			, "temp_table_set"."load_cycle_id" AS "load_cycle_id"
			, STRING_AGG("temp_table_set"."char_hash_diff" || "temp_table_set"."char_delete_flag", ',' ORDER BY "temp_table_set"."char_hash_diff" || 
				"temp_table_set"."char_delete_flag")   AS "agg_hash_diff"
		FROM "temp_table_set" "temp_table_set"
		GROUP BY  "temp_table_set"."product_sensors_hkey",  "temp_table_set"."load_cycle_id"
	)
	SELECT 
		  "temp_table_set"."product_sensors_hkey" AS "product_sensors_hkey"
		, "temp_table_set"."load_date" AS "load_date"
		, "temp_table_set"."load_end_date" AS "load_end_date"
		, "temp_table_set"."load_cycle_id" AS "load_cycle_id"
		, "temp_table_set"."record_type" AS "record_type"
		, "temp_table_set"."source" AS "source"
		, CASE WHEN 2 * "temp_table_set"."count_multi_active" = COUNT("agg_hash_diff"."agg_hash_diff")OVER(PARTITION BY "agg_hash_diff"."product_sensors_hkey",
			"agg_hash_diff"."agg_hash_diff")THEN 1 ELSE 0 END AS "equal"
		, "temp_table_set"."hash_diff" AS "hash_diff"
		, "temp_table_set"."delete_flag" AS "delete_flag"
		, "temp_table_set"."vehicle_number" AS "vehicle_number"
		, "temp_table_set"."subsequence_seq" AS "subsequence_seq"
		, "temp_table_set"."sensor" AS "sensor"
		, "temp_table_set"."sensor_value" AS "sensor_value"
		, "temp_table_set"."unit_of_measurement" AS "unit_of_measurement"
	FROM "temp_table_set" "temp_table_set"
	INNER JOIN "agg_hash_diff" "agg_hash_diff" ON  "agg_hash_diff"."product_sensors_hkey" = "temp_table_set"."product_sensors_hkey" AND "agg_hash_diff"."load_cycle_id" =
		 "temp_table_set"."load_cycle_id"
	;
END;


BEGIN -- sat_inur_tgt

	INSERT INTO "moto_scn01_fl"."sat_ms_product_sensors"(
		 "product_sensors_hkey"
		,"load_date"
		,"load_end_date"
		,"load_cycle_id"
		,"hash_diff"
		,"delete_flag"
		,"vehicle_number"
		,"subsequence_seq"
		,"sensor"
		,"sensor_value"
		,"unit_of_measurement"
	)
	SELECT 
		  "sat_temp_src_inur"."product_sensors_hkey" AS "product_sensors_hkey"
		, "sat_temp_src_inur"."load_date" AS "load_date"
		, "sat_temp_src_inur"."load_end_date" AS "load_end_date"
		, "sat_temp_src_inur"."load_cycle_id" AS "load_cycle_id"
		, "sat_temp_src_inur"."hash_diff" AS "hash_diff"
		, "sat_temp_src_inur"."delete_flag" AS "delete_flag"
		, "sat_temp_src_inur"."vehicle_number" AS "vehicle_number"
		, "sat_temp_src_inur"."subsequence_seq" AS "subsequence_seq"
		, "sat_temp_src_inur"."sensor" AS "sensor"
		, "sat_temp_src_inur"."sensor_value" AS "sensor_value"
		, "sat_temp_src_inur"."unit_of_measurement" AS "unit_of_measurement"
	FROM "moto_sales_scn01_stg"."sat_ms_product_sensors_tmp" "sat_temp_src_inur"
	WHERE  "sat_temp_src_inur"."source" = 'STG' AND "sat_temp_src_inur"."equal" = 0
	;
END;


BEGIN -- sat_ed_tgt

	WITH "end_dating" AS 
	( 
		SELECT 
			  "sat_temp_src_dk"."product_sensors_hkey" AS "product_sensors_hkey"
			, "sat_temp_src_dk"."load_date" AS "load_date"
			, "sat_temp_src_dk"."delete_flag" AS "delete_flag"
			, ROW_NUMBER()OVER(PARTITION BY "sat_temp_src_dk"."product_sensors_hkey" ORDER BY "sat_temp_src_dk"."load_date") AS "dummy"
		FROM "moto_sales_scn01_stg"."sat_ms_product_sensors_tmp" "sat_temp_src_dk"
		WHERE  "sat_temp_src_dk"."equal" = 0 AND "sat_temp_src_dk"."source" = 'STG'
	)
	, "calc_load_end_date" AS 
	( 
		SELECT 
			  "sat_temp_src_us"."product_sensors_hkey" AS "product_sensors_hkey"
			, "sat_temp_src_us"."load_date" AS "load_date"
			, "lci_src"."load_cycle_id" AS "load_cycle_id"
			, "end_dating"."load_date" AS "load_end_date"
			, "sat_temp_src_us"."subsequence_seq" AS "subsequence_seq"
		FROM "moto_sales_scn01_stg"."sat_ms_product_sensors_tmp" "sat_temp_src_us"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		INNER JOIN "end_dating" "end_dating" ON  "sat_temp_src_us"."product_sensors_hkey" = "end_dating"."product_sensors_hkey"
		WHERE  "sat_temp_src_us"."equal" = 0 AND "sat_temp_src_us"."source" = 'SAT' AND "end_dating"."dummy" = 1
	)
	, "filter_load_end_date" AS 
	( 
		SELECT 
			  "calc_load_end_date"."product_sensors_hkey" AS "product_sensors_hkey"
			, "calc_load_end_date"."load_date" AS "load_date"
			, "calc_load_end_date"."load_cycle_id" AS "load_cycle_id"
			, "calc_load_end_date"."subsequence_seq" AS "subsequence_seq"
			, "calc_load_end_date"."load_end_date" AS "load_end_date"
		FROM "calc_load_end_date" "calc_load_end_date"
		WHERE  "calc_load_end_date"."load_end_date" != TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
	)
	UPDATE "moto_scn01_fl"."sat_ms_product_sensors" "sat_ed_tgt"
	SET 
		 "load_end_date" =  "filter_load_end_date"."load_end_date"
		,"load_cycle_id" =  "filter_load_end_date"."load_cycle_id"
	FROM  "filter_load_end_date"
	WHERE "sat_ed_tgt"."product_sensors_hkey" =  "filter_load_end_date"."product_sensors_hkey"
	  AND "sat_ed_tgt"."load_date" =  "filter_load_end_date"."load_date"
	  AND "sat_ed_tgt"."subsequence_seq" =  "filter_load_end_date"."subsequence_seq"
	;
END;



END;
$function$;
 
 
