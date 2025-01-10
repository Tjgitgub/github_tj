CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_mm_campaigns_init"() 
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

	TRUNCATE TABLE "moto_mktg_scn01_ext"."campaigns"  CASCADE;

	INSERT INTO "moto_mktg_scn01_ext"."campaigns"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"campaign_code"
		,"campaign_start_date"
		,"campaign_code_bk"
		,"campaign_start_date_bk"
		,"campaign_name"
		,"update_timestamp"
	)
	WITH "load_init_data" AS 
	( 
		SELECT 
			  'I' ::text AS "operation"
			, 'S'::text AS "record_type"
			, COALESCE("ini_src"."campaign_code", "mex_inr_src"."key_attribute_varchar"::text) AS "campaign_code"
			, COALESCE("ini_src"."campaign_start_date", TO_DATE("mex_inr_src"."key_attribute_date", 'DD/MM/YYYY'::varchar)
				) AS "campaign_start_date"
			, "ini_src"."campaign_name" AS "campaign_name"
			, "ini_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_mktg_scn01"."campaigns" "ini_src"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_inr_src" ON  "mex_inr_src"."record_type" = 'N'
	)
	, "prep_excep" AS 
	( 
		SELECT 
			  "load_init_data"."operation" AS "operation"
			, "load_init_data"."record_type" AS "record_type"
			, NULL ::int AS "load_cycle_id"
			, "load_init_data"."campaign_code" AS "campaign_code"
			, "load_init_data"."campaign_start_date" AS "campaign_start_date"
			, "load_init_data"."campaign_name" AS "campaign_name"
			, "load_init_data"."update_timestamp" AS "update_timestamp"
		FROM "load_init_data" "load_init_data"
		UNION ALL 
		SELECT 
			  'I' ::text AS "operation"
			, "mex_ext_src"."record_type" AS "record_type"
			, "mex_ext_src"."load_cycle_id" ::int AS "load_cycle_id"
			, "mex_ext_src"."key_attribute_varchar"::text AS "campaign_code"
			, TO_DATE("mex_ext_src"."key_attribute_date", 'DD/MM/YYYY'::varchar) AS "campaign_start_date"
			, "mex_ext_src"."attribute_varchar"::text AS "campaign_name"
			, TO_TIMESTAMP("mex_ext_src"."attribute_timestamp", 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "update_timestamp"
		FROM "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_ext_src"
	)
	, "calculate_bk" AS 
	( 
		SELECT 
			  COALESCE("prep_excep"."load_cycle_id","lci_src"."load_cycle_id") AS "load_cycle_id"
			, "lci_src"."load_date" AS "load_date"
			, "lci_src"."load_date" AS "trans_timestamp"
			, "prep_excep"."operation" AS "operation"
			, "prep_excep"."record_type" AS "record_type"
			, "prep_excep"."campaign_code" AS "campaign_code"
			, "prep_excep"."campaign_start_date" AS "campaign_start_date"
			, COALESCE(UPPER(REPLACE(TRIM("prep_excep"."campaign_code"),'#','\' || '#')),"mex_src"."key_attribute_varchar") AS "campaign_code_bk"
			, COALESCE(UPPER( TO_CHAR("prep_excep"."campaign_start_date", 'DD/MM/YYYY'::varchar)),"mex_src"."key_attribute_date") AS "campaign_start_date_bk"
			, "prep_excep"."campaign_name" AS "campaign_name"
			, "prep_excep"."update_timestamp" AS "update_timestamp"
		FROM "prep_excep" "prep_excep"
		INNER JOIN "moto_mktg_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	SELECT 
		  "calculate_bk"."load_cycle_id" AS "load_cycle_id"
		, "calculate_bk"."load_date" AS "load_date"
		, "calculate_bk"."trans_timestamp" AS "trans_timestamp"
		, "calculate_bk"."operation" AS "operation"
		, "calculate_bk"."record_type" AS "record_type"
		, "calculate_bk"."campaign_code" AS "campaign_code"
		, "calculate_bk"."campaign_start_date" AS "campaign_start_date"
		, "calculate_bk"."campaign_code_bk" AS "campaign_code_bk"
		, "calculate_bk"."campaign_start_date_bk" AS "campaign_start_date_bk"
		, "calculate_bk"."campaign_name" AS "campaign_name"
		, "calculate_bk"."update_timestamp" AS "update_timestamp"
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 
