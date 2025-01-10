CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_ms_parts_init"() 
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

	TRUNCATE TABLE "moto_sales_scn01_ext"."parts"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."parts"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"part_id"
		,"ref_part_number_fk"
		,"ref_part_language_code_fk"
		,"part_number_bk"
		,"part_langua_code_bk"
		,"part_number"
		,"part_language_code"
		,"update_timestamp"
	)
	WITH "load_init_data" AS 
	( 
		SELECT 
			  'I' ::text AS "operation"
			, 'S'::text AS "record_type"
			, COALESCE("ini_src"."part_id", TO_NUMBER("mex_inr_src"."key_attribute_numeric", '999999999999D999999999'::varchar)
				) AS "part_id"
			, CASE WHEN "ini_src"."part_number" = '' THEN "mex_inr_src"."key_attribute_varchar"::text ELSE COALESCE("ini_src"."part_number",
				 "mex_inr_src"."key_attribute_varchar"::text)END AS "ref_part_number_fk"
			, CASE WHEN "ini_src"."part_language_code" = '' THEN "mex_inr_src"."key_attribute_varchar"::text ELSE COALESCE("ini_src"."part_language_code",
				 "mex_inr_src"."key_attribute_varchar"::text)END AS "ref_part_language_code_fk"
			, "ini_src"."part_number" AS "part_number"
			, "ini_src"."part_language_code" AS "part_language_code"
			, "ini_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01"."moto_parts" "ini_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_inr_src" ON  "mex_inr_src"."record_type" = 'N'
	)
	, "prep_excep" AS 
	( 
		SELECT 
			  "load_init_data"."operation" AS "operation"
			, "load_init_data"."record_type" AS "record_type"
			, NULL ::int AS "load_cycle_id"
			, "load_init_data"."part_id" AS "part_id"
			, "load_init_data"."ref_part_number_fk" AS "ref_part_number_fk"
			, "load_init_data"."ref_part_language_code_fk" AS "ref_part_language_code_fk"
			, "load_init_data"."part_number" AS "part_number"
			, "load_init_data"."part_language_code" AS "part_language_code"
			, "load_init_data"."update_timestamp" AS "update_timestamp"
		FROM "load_init_data" "load_init_data"
		UNION ALL 
		SELECT 
			  'I' ::text AS "operation"
			, "mex_ext_src"."record_type" AS "record_type"
			, "mex_ext_src"."load_cycle_id" ::int AS "load_cycle_id"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "part_id"
			, "mex_ext_src"."key_attribute_varchar"::text AS "ref_part_number_fk"
			, "mex_ext_src"."key_attribute_varchar"::text AS "ref_part_language_code_fk"
			, "mex_ext_src"."key_attribute_varchar"::text AS "part_number"
			, "mex_ext_src"."key_attribute_varchar"::text AS "part_language_code"
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
			, "prep_excep"."part_id" AS "part_id"
			, "prep_excep"."ref_part_number_fk" AS "ref_part_number_fk"
			, "prep_excep"."ref_part_language_code_fk" AS "ref_part_language_code_fk"
			, CASE WHEN TRIM("prep_excep"."part_number")= '' OR "prep_excep"."part_number" IS NULL THEN "mex_src"."key_attribute_varchar" ELSE UPPER(REPLACE(TRIM("prep_excep"."part_number")
				,'#','\' || '#'))END AS "part_number_bk"
			, CASE WHEN TRIM("prep_excep"."part_language_code")= '' OR "prep_excep"."part_language_code" IS NULL THEN "mex_src"."key_attribute_varchar" ELSE UPPER(REPLACE(TRIM("prep_excep"."part_language_code")
				,'#','\' || '#'))END AS "part_langua_code_bk"
			, "prep_excep"."part_number" AS "part_number"
			, "prep_excep"."part_language_code" AS "part_language_code"
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
		, "calculate_bk"."part_id" AS "part_id"
		, "calculate_bk"."ref_part_number_fk" AS "ref_part_number_fk"
		, "calculate_bk"."ref_part_language_code_fk" AS "ref_part_language_code_fk"
		, "calculate_bk"."part_number_bk" AS "part_number_bk"
		, "calculate_bk"."part_langua_code_bk" AS "part_langua_code_bk"
		, "calculate_bk"."part_number" AS "part_number"
		, "calculate_bk"."part_language_code" AS "part_language_code"
		, "calculate_bk"."update_timestamp" AS "update_timestamp"
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 
