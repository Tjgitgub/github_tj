CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ref_ms_codes_to_languag_init"() 
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

BEGIN -- ref_tgt

	TRUNCATE TABLE "moto_scn01_fl"."ref_ms_codes_to_languag"  CASCADE;

	INSERT INTO "moto_scn01_fl"."ref_ms_codes_to_languag"(
		 "code"
		,"language_code"
		,"load_cycle_id"
		,"load_date"
		,"description"
		,"update_timestamp"
	)
	WITH "prep_excep" AS 
	( 
		SELECT 
			  "ini_src"."code" AS "code"
			, "ini_src"."language_code" AS "language_code"
			, NULL ::int AS "load_cycle_id"
			, "ini_src"."description" AS "description"
			, "ini_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01"."codes_to_language" "ini_src"
		UNION 
		SELECT 
			  "mex_ex_src"."key_attribute_varchar"::text AS "code"
			, "mex_ex_src"."key_attribute_varchar"::text AS "language_code"
			, "mex_ex_src"."load_cycle_id" ::int AS "load_cycle_id"
			, "mex_ex_src"."attribute_varchar"::text AS "description"
			, TO_TIMESTAMP("mex_ex_src"."attribute_timestamp", 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "update_timestamp"
		FROM "moto_sales_scn01_mtd"."mtd_exception_records" "mex_ex_src"
	)
	SELECT 
		  "prep_excep"."code" AS "code"
		, "prep_excep"."language_code" AS "language_code"
		, COALESCE("prep_excep"."load_cycle_id","lci_src"."load_cycle_id") AS "load_cycle_id"
		, "lci_src"."load_date" AS "load_date"
		, "prep_excep"."description" AS "description"
		, "prep_excep"."update_timestamp" AS "update_timestamp"
	FROM "prep_excep" "prep_excep"
	INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
	;
END;



END;
$function$;
 
 
