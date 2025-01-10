CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_ms_productsensors_init"() 
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
	WITH "load_init_data" AS 
	( 
		SELECT 
			  'I' ::text AS "operation"
			, 'S'::text AS "record_type"
			, CASE WHEN TRIM("ini_src"."vehicle_number")= '' THEN "mex_inr_src"."key_attribute_varchar"::text ELSE COALESCE("ini_src"."vehicle_number",
				 "mex_inr_src"."key_attribute_varchar"::text)END AS "vehicle_number"
			, COALESCE("ini_src"."product_number", TO_NUMBER("mex_inr_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "product_number"
			, "ini_src"."sensor" AS "sensor"
			, "ini_src"."sensor_value" AS "sensor_value"
			, "ini_src"."unit_of_measurement" AS "unit_of_measurement"
		FROM "moto_sales_scn01"."product_sensors" "ini_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_inr_src" ON  "mex_inr_src"."record_type" = 'N'
	)
	, "prep_excep" AS 
	( 
		SELECT 
			  "load_init_data"."operation" AS "operation"
			, "load_init_data"."record_type" AS "record_type"
			, NULL ::int AS "load_cycle_id"
			, "load_init_data"."vehicle_number" AS "vehicle_number"
			, "load_init_data"."product_number" AS "product_number"
			, "load_init_data"."sensor" AS "sensor"
			, "load_init_data"."sensor_value" AS "sensor_value"
			, "load_init_data"."unit_of_measurement" AS "unit_of_measurement"
		FROM "load_init_data" "load_init_data"
		UNION ALL 
		SELECT 
			  'I' ::text AS "operation"
			, "mex_ext_src"."record_type" AS "record_type"
			, "mex_ext_src"."load_cycle_id" ::int AS "load_cycle_id"
			, "mex_ext_src"."key_attribute_varchar"::text AS "vehicle_number"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "product_number"
			, "mex_ext_src"."attribute_varchar"::text AS "sensor"
			, TO_NUMBER("mex_ext_src"."attribute_numeric", '999999999999D999999999'::varchar) AS "sensor_value"
			, "mex_ext_src"."attribute_varchar"::text AS "unit_of_measurement"
		FROM "moto_sales_scn01_mtd"."mtd_exception_records" "mex_ext_src"
	)
	, "calculate_bk" AS 
	( 
		SELECT 
			  COALESCE("prep_excep"."load_cycle_id","lci_src"."load_cycle_id") AS "load_cycle_id"
			, "lci_src"."load_date" AS "load_date"
			, "prep_excep"."operation" AS "operation"
			, "prep_excep"."record_type" AS "record_type"
			, "prep_excep"."vehicle_number" AS "vehicle_number"
			, "prep_excep"."product_number" AS "product_number"
			, CASE WHEN TRIM("prep_excep"."vehicle_number")= '' OR "prep_excep"."vehicle_number" IS NULL THEN "mex_src"."key_attribute_varchar" ELSE UPPER(REPLACE(TRIM("prep_excep"."vehicle_number")
				,'#','\' || '#'))END AS "vehicle_number_bk"
			, "prep_excep"."sensor" AS "sensor"
			, "prep_excep"."sensor_value" AS "sensor_value"
			, "prep_excep"."unit_of_measurement" AS "unit_of_measurement"
		FROM "prep_excep" "prep_excep"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	SELECT 
		  "calculate_bk"."load_cycle_id" AS "load_cycle_id"
		, "calculate_bk"."load_date" AS "load_date"
		, "calculate_bk"."operation" AS "operation"
		, "calculate_bk"."record_type" AS "record_type"
		, "calculate_bk"."vehicle_number" AS "vehicle_number"
		, "calculate_bk"."product_number" AS "product_number"
		, "calculate_bk"."vehicle_number_bk" AS "vehicle_number_bk"
		, ROW_NUMBER()OVER(PARTITION BY "calculate_bk"."vehicle_number_bk","calculate_bk"."load_cycle_id" ORDER BY "calculate_bk"."vehicle_number_bk",
			"calculate_bk"."load_cycle_id") AS "subsequence_seq"
		, "calculate_bk"."sensor" AS "sensor"
		, "calculate_bk"."sensor_value" AS "sensor_value"
		, "calculate_bk"."unit_of_measurement" AS "unit_of_measurement"
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 
