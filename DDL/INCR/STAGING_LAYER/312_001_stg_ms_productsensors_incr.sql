CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_ms_productsensors_incr"() 
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

Vaultspeed version: 5.7.2.16, generation date: 2025/01/17 07:04:17
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/16 14:56:23, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08
 */


BEGIN 

BEGIN -- stg_tgt

	TRUNCATE TABLE "moto_sales_scn01_stg"."product_sensors"  CASCADE;

	INSERT INTO "moto_sales_scn01_stg"."product_sensors"(
		 "product_sensors_hkey"
		,"products_hkey"
		,"lnk_prse_prod_hkey"
		,"load_date"
		,"load_cycle_id"
		,"operation"
		,"record_type"
		,"vehicle_number"
		,"product_number"
		,"vehicle_number_bk"
		,"product_cc_fk_productnumber_bk"
		,"product_et_code_fk_productnumber_bk"
		,"product_part_code_fk_productnumber_bk"
		,"subsequence_seq"
		,"sensor"
		,"sensor_value"
		,"unit_of_measurement"
	)
	WITH "dist_fk1" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src1"."product_number" AS "product_number"
		FROM "moto_sales_scn01_ext"."product_sensors" "ext_dis_src1"
	)
	, "prep_find_bk_fk1" AS 
	( 
		SELECT 
			  "hub_src1"."product_cc_bk" AS "product_cc_bk"
			, "hub_src1"."product_et_code_bk" AS "product_et_code_bk"
			, "hub_src1"."product_part_code_bk" AS "product_part_code_bk"
			, "dist_fk1"."product_number" AS "product_number"
			, "sat_src1"."load_date" AS "load_date"
			, 1 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_scn01_fl"."sat_ms_products" "sat_src1" ON  "dist_fk1"."product_number" = "sat_src1"."product_id"
		INNER JOIN "moto_scn01_fl"."hub_products" "hub_src1" ON  "hub_src1"."products_hkey" = "sat_src1"."products_hkey"
		WHERE  "sat_src1"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
		UNION ALL 
		SELECT 
			  "ext_fkbk_src1"."product_cc_bk" AS "product_cc_bk"
			, "ext_fkbk_src1"."product_et_code_bk" AS "product_et_code_bk"
			, "ext_fkbk_src1"."product_part_code_bk" AS "product_part_code_bk"
			, "dist_fk1"."product_number" AS "product_number"
			, "ext_fkbk_src1"."load_date" AS "load_date"
			, 0 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_sales_scn01_ext"."products" "ext_fkbk_src1" ON  "dist_fk1"."product_number" = "ext_fkbk_src1"."product_id"
	)
	, "order_bk_fk1" AS 
	( 
		SELECT 
			  "prep_find_bk_fk1"."product_cc_bk" AS "product_cc_bk"
			, "prep_find_bk_fk1"."product_et_code_bk" AS "product_et_code_bk"
			, "prep_find_bk_fk1"."product_part_code_bk" AS "product_part_code_bk"
			, "prep_find_bk_fk1"."product_number" AS "product_number"
			, ROW_NUMBER()OVER(PARTITION BY "prep_find_bk_fk1"."product_number" ORDER BY "prep_find_bk_fk1"."general_order",
				"prep_find_bk_fk1"."load_date" DESC) AS "dummy"
		FROM "prep_find_bk_fk1" "prep_find_bk_fk1"
	)
	, "find_bk_fk1" AS 
	( 
		SELECT 
			  "order_bk_fk1"."product_cc_bk" AS "product_cc_bk"
			, "order_bk_fk1"."product_et_code_bk" AS "product_et_code_bk"
			, "order_bk_fk1"."product_part_code_bk" AS "product_part_code_bk"
			, "order_bk_fk1"."product_number" AS "product_number"
		FROM "order_bk_fk1" "order_bk_fk1"
		WHERE  "order_bk_fk1"."dummy" = 1
	)
	, "calc_hash_keys" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST( "ext_src"."vehicle_number_bk" || '#' ,'MD5'),'HEX')) AS "product_sensors_hkey"
			, UPPER(ENCODE(DIGEST( 'moto_sales_scn01' || '#' || COALESCE("find_bk_fk1"."product_cc_bk","mex_src"."key_attribute_numeric")
				||'#' ||  COALESCE("find_bk_fk1"."product_et_code_bk","mex_src"."key_attribute_character")|| '#' ||  COALESCE("find_bk_fk1"."product_part_code_bk","mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "products_hkey"
			, UPPER(ENCODE(DIGEST( "ext_src"."vehicle_number_bk" || '#' || 'moto_sales_scn01' || '#' || COALESCE("find_bk_fk1"."product_cc_bk",
				"mex_src"."key_attribute_numeric")|| '#' ||  COALESCE("find_bk_fk1"."product_et_code_bk","mex_src"."key_attribute_character")|| '#' ||  COALESCE("find_bk_fk1"."product_part_code_bk","mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "lnk_prse_prod_hkey"
			, "ext_src"."load_date" AS "load_date"
			, "ext_src"."load_cycle_id" AS "load_cycle_id"
			, "ext_src"."operation" AS "operation"
			, "ext_src"."record_type" AS "record_type"
			, "ext_src"."vehicle_number" AS "vehicle_number"
			, "ext_src"."product_number" AS "product_number"
			, "ext_src"."vehicle_number_bk" AS "vehicle_number_bk"
			, NULL::text AS "product_cc_fk_productnumber_bk"
			, NULL::text AS "product_et_code_fk_productnumber_bk"
			, NULL::text AS "product_part_code_fk_productnumber_bk"
			, "ext_src"."subsequence_seq" AS "subsequence_seq"
			, "ext_src"."sensor" AS "sensor"
			, "ext_src"."sensor_value" AS "sensor_value"
			, "ext_src"."unit_of_measurement" AS "unit_of_measurement"
		FROM "moto_sales_scn01_ext"."product_sensors" "ext_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
		LEFT OUTER JOIN "find_bk_fk1" "find_bk_fk1" ON  "ext_src"."product_number" = "find_bk_fk1"."product_number"
	)
	SELECT 
		  "calc_hash_keys"."product_sensors_hkey" AS "product_sensors_hkey"
		, "calc_hash_keys"."products_hkey" AS "products_hkey"
		, "calc_hash_keys"."lnk_prse_prod_hkey" AS "lnk_prse_prod_hkey"
		, "calc_hash_keys"."load_date" AS "load_date"
		, "calc_hash_keys"."load_cycle_id" AS "load_cycle_id"
		, "calc_hash_keys"."operation" AS "operation"
		, "calc_hash_keys"."record_type" AS "record_type"
		, "calc_hash_keys"."vehicle_number" AS "vehicle_number"
		, "calc_hash_keys"."product_number" AS "product_number"
		, "calc_hash_keys"."vehicle_number_bk" AS "vehicle_number_bk"
		, "calc_hash_keys"."product_cc_fk_productnumber_bk" AS "product_cc_fk_productnumber_bk"
		, "calc_hash_keys"."product_et_code_fk_productnumber_bk" AS "product_et_code_fk_productnumber_bk"
		, "calc_hash_keys"."product_part_code_fk_productnumber_bk" AS "product_part_code_fk_productnumber_bk"
		, "calc_hash_keys"."subsequence_seq" AS "subsequence_seq"
		, "calc_hash_keys"."sensor" AS "sensor"
		, "calc_hash_keys"."sensor_value" AS "sensor_value"
		, "calc_hash_keys"."unit_of_measurement" AS "unit_of_measurement"
	FROM "calc_hash_keys" "calc_hash_keys"
	;
END;



END;
$function$;
 
 
