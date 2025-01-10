CREATE OR REPLACE FUNCTION "moto_scn01_proc"."sat_ms_prse_init"() 
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

BEGIN -- sat_tgt

	TRUNCATE TABLE "moto_scn01_fl"."sat_ms_product_sensors"  CASCADE;

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
	WITH "stg_src" AS 
	( 
		SELECT 
			  "stg_inr_src"."product_sensors_hkey" AS "product_sensors_hkey"
			, "stg_inr_src"."load_date" AS "load_date"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, "stg_inr_src"."load_cycle_id" AS "load_cycle_id"
			, UPPER(ENCODE(DIGEST(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "stg_inr_src"."sensor"),'~'),'#','\' || '#')
				)|| '#' ||  UPPER(REPLACE(COALESCE(TRIM( "stg_inr_src"."sensor_value"::text),'~'),'#','\' || '#'))|| '#' ||  UPPER(REPLACE(COALESCE(TRIM( "stg_inr_src"."unit_of_measurement"),'~'),'#','\' || '#'))|| '#','#' || '~'),'~') ,'MD5'),'HEX')) AS "hash_diff"
			, 'N'::text AS "delete_flag"
			, "stg_inr_src"."vehicle_number" AS "vehicle_number"
			, "stg_inr_src"."subsequence_seq" AS "subsequence_seq"
			, "stg_inr_src"."sensor" AS "sensor"
			, "stg_inr_src"."sensor_value" AS "sensor_value"
			, "stg_inr_src"."unit_of_measurement" AS "unit_of_measurement"
			, ROW_NUMBER()OVER(PARTITION BY "stg_inr_src"."product_sensors_hkey","stg_inr_src"."subsequence_seq" ORDER BY "stg_inr_src"."load_date") AS "dummy"
		FROM "moto_sales_scn01_stg"."product_sensors" "stg_inr_src"
	)
	SELECT 
		  "stg_src"."product_sensors_hkey" AS "product_sensors_hkey"
		, "stg_src"."load_date" AS "load_date"
		, "stg_src"."load_end_date" AS "load_end_date"
		, "stg_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_src"."hash_diff" AS "hash_diff"
		, "stg_src"."delete_flag" AS "delete_flag"
		, "stg_src"."vehicle_number" AS "vehicle_number"
		, "stg_src"."subsequence_seq" AS "subsequence_seq"
		, "stg_src"."sensor" AS "sensor"
		, "stg_src"."sensor_value" AS "sensor_value"
		, "stg_src"."unit_of_measurement" AS "unit_of_measurement"
	FROM "stg_src" "stg_src"
	WHERE  "stg_src"."dummy" = 1
	;
END;



END;
$function$;
 
 
