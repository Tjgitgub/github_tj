CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_ms_products_init"() 
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

	TRUNCATE TABLE "moto_sales_scn01_ext"."products"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."products"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"product_id"
		,"replacement_product_id"
		,"product_cc_bk"
		,"product_et_code_bk"
		,"product_part_code_bk"
		,"product_intro_date"
		,"product_name"
		,"update_timestamp"
	)
	WITH "load_init_data" AS 
	( 
		SELECT 
			  'I' ::text AS "operation"
			, 'S'::text AS "record_type"
			, COALESCE("ini_src"."product_id", TO_NUMBER("mex_inr_src"."key_attribute_numeric", '999999999999D999999999'::varchar)
				) AS "product_id"
			, COALESCE("ini_src"."replacement_product_id", TO_NUMBER("mex_inr_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "replacement_product_id"
			, "ini_src"."product_cc" AS "product_cc"
			, "ini_src"."product_et_code" AS "product_et_code"
			, "ini_src"."product_part_code" AS "product_part_code"
			, "ini_src"."product_intro_date" AS "product_intro_date"
			, "ini_src"."product_name" AS "product_name"
			, "ini_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01"."moto_products" "ini_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_inr_src" ON  "mex_inr_src"."record_type" = 'N'
	)
	, "prep_excep" AS 
	( 
		SELECT 
			  "load_init_data"."operation" AS "operation"
			, "load_init_data"."record_type" AS "record_type"
			, NULL ::int AS "load_cycle_id"
			, "load_init_data"."product_id" AS "product_id"
			, "load_init_data"."replacement_product_id" AS "replacement_product_id"
			, "load_init_data"."product_cc" AS "product_cc"
			, "load_init_data"."product_et_code" AS "product_et_code"
			, "load_init_data"."product_part_code" AS "product_part_code"
			, "load_init_data"."product_intro_date" AS "product_intro_date"
			, "load_init_data"."product_name" AS "product_name"
			, "load_init_data"."update_timestamp" AS "update_timestamp"
		FROM "load_init_data" "load_init_data"
		UNION ALL 
		SELECT 
			  'I' ::text AS "operation"
			, "mex_ext_src"."record_type" AS "record_type"
			, "mex_ext_src"."load_cycle_id" ::int AS "load_cycle_id"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "product_id"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "replacement_product_id"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "product_cc"
			, "mex_ext_src"."key_attribute_character"::text AS "product_et_code"
			, "mex_ext_src"."key_attribute_varchar"::text AS "product_part_code"
			, TO_DATE("mex_ext_src"."attribute_date", 'DD/MM/YYYY'::varchar) AS "product_intro_date"
			, "mex_ext_src"."attribute_varchar"::text AS "product_name"
			, TO_TIMESTAMP("mex_ext_src"."attribute_timestamp", 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "update_timestamp"
		FROM "moto_sales_scn01_mtd"."mtd_exception_records" "mex_ext_src"
	)
	, "calculate_bk" AS 
	( 
		SELECT 
			  COALESCE("prep_excep"."load_cycle_id","lci_src"."load_cycle_id") AS "load_cycle_id"
			, "lci_src"."load_date" AS "load_date"
			, "lci_src"."load_date" AS "trans_timestamp"
			, "prep_excep"."operation" AS "operation"
			, "prep_excep"."record_type" AS "record_type"
			, "prep_excep"."product_id" AS "product_id"
			, "prep_excep"."replacement_product_id" AS "replacement_product_id"
			, COALESCE(UPPER( "prep_excep"."product_cc"::text),"mex_src"."key_attribute_numeric") AS "product_cc_bk"
			, CASE WHEN TRIM("prep_excep"."product_et_code")= '' OR "prep_excep"."product_et_code" IS NULL THEN "mex_src"."key_attribute_character" ELSE UPPER(REPLACE(TRIM("prep_excep"."product_et_code")
				,'#','\' || '#'))END AS "product_et_code_bk"
			, CASE WHEN TRIM("prep_excep"."product_part_code")= '' OR "prep_excep"."product_part_code" IS NULL THEN "mex_src"."key_attribute_varchar" ELSE UPPER(REPLACE(TRIM("prep_excep"."product_part_code")
				,'#','\' || '#'))END AS "product_part_code_bk"
			, "prep_excep"."product_intro_date" AS "product_intro_date"
			, "prep_excep"."product_name" AS "product_name"
			, "prep_excep"."update_timestamp" AS "update_timestamp"
		FROM "prep_excep" "prep_excep"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	SELECT 
		  "calculate_bk"."load_cycle_id" AS "load_cycle_id"
		, "calculate_bk"."load_date" AS "load_date"
		, "calculate_bk"."trans_timestamp" AS "trans_timestamp"
		, "calculate_bk"."operation" AS "operation"
		, "calculate_bk"."record_type" AS "record_type"
		, "calculate_bk"."product_id" AS "product_id"
		, "calculate_bk"."replacement_product_id" AS "replacement_product_id"
		, "calculate_bk"."product_cc_bk" AS "product_cc_bk"
		, "calculate_bk"."product_et_code_bk" AS "product_et_code_bk"
		, "calculate_bk"."product_part_code_bk" AS "product_part_code_bk"
		, "calculate_bk"."product_intro_date" AS "product_intro_date"
		, "calculate_bk"."product_name" AS "product_name"
		, "calculate_bk"."update_timestamp" AS "update_timestamp"
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 
