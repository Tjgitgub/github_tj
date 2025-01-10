CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_ms_productsensors_incr"() 
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

BEGIN -- ext_tgt

	TRUNCATE TABLE "moto_sales_scn01_ext"."product_sensors"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."product_sensors"(
		 "load_cycle_id"
		,"load_date"
		,"operation"
		,"record_type"
		,"vehicle_number"
		,"product_number"
		,"vehicle_number_bk"
		,"subsequence_seq"
		,"sensor"
		,"sensor_value"
		,"unit_of_measurement"
	)
	WITH "calculate_bk" AS 
	( 
		SELECT 
			  "mex_src"."attribute_varchar" AS "operation"
			, "tdfv_src"."record_type" AS "record_type"
			, "tdfv_src"."vehicle_number" AS "vehicle_number"
			, "tdfv_src"."product_number" AS "product_number"
			, CASE WHEN TRIM("tdfv_src"."vehicle_number")= '' OR "tdfv_src"."vehicle_number" IS NULL THEN "mex_src"."key_attribute_varchar" ELSE UPPER(REPLACE(TRIM( "tdfv_src"."vehicle_number")
				,'#','\' || '#'))END AS "vehicle_number_bk"
			, "tdfv_src"."sensor" AS "sensor"
			, "tdfv_src"."sensor_value" AS "sensor_value"
			, "tdfv_src"."unit_of_measurement" AS "unit_of_measurement"
		FROM "moto_sales_scn01_dfv"."vw_product_sensors" "tdfv_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	, "ext_union" AS 
	( 
		SELECT 
			  "lci_src"."load_cycle_id" AS "load_cycle_id"
			, "lci_src"."load_date" AS "load_date"
			, "calculate_bk"."operation" AS "operation"
			, "calculate_bk"."record_type" AS "record_type"
			, "calculate_bk"."vehicle_number" AS "vehicle_number"
			, "calculate_bk"."product_number" AS "product_number"
			, "calculate_bk"."vehicle_number_bk" AS "vehicle_number_bk"
			, ROW_NUMBER()OVER(PARTITION BY "calculate_bk"."vehicle_number_bk" ORDER BY "calculate_bk"."vehicle_number_bk") AS "subsequence_seq"
			, "calculate_bk"."sensor" AS "sensor"
			, "calculate_bk"."sensor_value" AS "sensor_value"
			, "calculate_bk"."unit_of_measurement" AS "unit_of_measurement"
		FROM "calculate_bk" "calculate_bk"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
	)
	SELECT 
		  "ext_union"."load_cycle_id" AS "load_cycle_id"
		, "ext_union"."load_date" AS "load_date"
		, "ext_union"."operation" AS "operation"
		, "ext_union"."record_type" AS "record_type"
		, "ext_union"."vehicle_number" AS "vehicle_number"
		, "ext_union"."product_number" AS "product_number"
		, "ext_union"."vehicle_number_bk" AS "vehicle_number_bk"
		, "ext_union"."subsequence_seq" AS "subsequence_seq"
		, "ext_union"."sensor" AS "sensor"
		, "ext_union"."sensor_value" AS "sensor_value"
		, "ext_union"."unit_of_measurement" AS "unit_of_measurement"
	FROM "ext_union" "ext_union"
	;
END;



END;
$function$;
 
 
