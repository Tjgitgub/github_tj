CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_mm_campmotochannel_init"() 
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

	TRUNCATE TABLE "moto_mktg_scn01_ext"."camp_moto_channel"  CASCADE;

	INSERT INTO "moto_mktg_scn01_ext"."camp_moto_channel"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"channel_id"
		,"campaign_code"
		,"campaign_start_date"
		,"campaign_code_fk_campaignstartdate_bk"
		,"campaign_start_date_fk_campaignstartdate_bk"
		,"from_date_seq"
		,"from_date"
		,"motorcycle_name"
		,"to_date"
		,"valid_from_date"
		,"valid_to_date"
		,"update_timestamp"
	)
	WITH "load_init_data" AS 
	( 
		SELECT 
			  'I' ::text AS "operation"
			, 'S'::text AS "record_type"
			, COALESCE("ini_src"."channel_id", TO_NUMBER("mex_inr_src"."key_attribute_numeric", '999999999999D999999999'::varchar)
				) AS "channel_id"
			, COALESCE("ini_src"."campaign_code", "mex_inr_src"."key_attribute_varchar"::text) AS "campaign_code"
			, COALESCE("ini_src"."campaign_start_date", TO_DATE("mex_inr_src"."key_attribute_date", 'DD/MM/YYYY'::varchar)
				) AS "campaign_start_date"
			, COALESCE("ini_src"."from_date", TO_TIMESTAMP("mex_inr_src"."key_attribute_timestamp", 
				'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "from_date_seq"
			, "ini_src"."from_date" AS "from_date"
			, "ini_src"."motorcycle_name" AS "motorcycle_name"
			, "ini_src"."to_date" AS "to_date"
			, "ini_src"."valid_from_date" AS "valid_from_date"
			, "ini_src"."valid_to_date" AS "valid_to_date"
			, "ini_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_mktg_scn01"."camp_moto_channel" "ini_src"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_inr_src" ON  "mex_inr_src"."record_type" = 'N'
	)
	, "prep_excep" AS 
	( 
		SELECT 
			  "load_init_data"."operation" AS "operation"
			, "load_init_data"."record_type" AS "record_type"
			, NULL ::int AS "load_cycle_id"
			, "load_init_data"."channel_id" AS "channel_id"
			, "load_init_data"."campaign_code" AS "campaign_code"
			, "load_init_data"."campaign_start_date" AS "campaign_start_date"
			, "load_init_data"."from_date_seq" AS "from_date_seq"
			, "load_init_data"."from_date" AS "from_date"
			, "load_init_data"."motorcycle_name" AS "motorcycle_name"
			, "load_init_data"."to_date" AS "to_date"
			, "load_init_data"."valid_from_date" AS "valid_from_date"
			, "load_init_data"."valid_to_date" AS "valid_to_date"
			, "load_init_data"."update_timestamp" AS "update_timestamp"
		FROM "load_init_data" "load_init_data"
		UNION ALL 
		SELECT 
			  'I' ::text AS "operation"
			, "mex_ext_src"."record_type" AS "record_type"
			, "mex_ext_src"."load_cycle_id" ::int AS "load_cycle_id"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "channel_id"
			, "mex_ext_src"."key_attribute_varchar"::text AS "campaign_code"
			, TO_DATE("mex_ext_src"."key_attribute_date", 'DD/MM/YYYY'::varchar) AS "campaign_start_date"
			, TO_TIMESTAMP("mex_ext_src"."key_attribute_timestamp", 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "from_date_seq"
			, TO_TIMESTAMP("mex_ext_src"."key_attribute_timestamp", 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "from_date"
			, "mex_ext_src"."attribute_varchar"::text AS "motorcycle_name"
			, TO_TIMESTAMP("mex_ext_src"."attribute_timestamp", 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "to_date"
			, TO_TIMESTAMP("mex_ext_src"."attribute_timestamp", 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "valid_from_date"
			, TO_TIMESTAMP("mex_ext_src"."attribute_timestamp", 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "valid_to_date"
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
			, "prep_excep"."channel_id" AS "channel_id"
			, "prep_excep"."campaign_code" AS "campaign_code"
			, "prep_excep"."campaign_start_date" AS "campaign_start_date"
			, UPPER(REPLACE(TRIM("prep_excep"."campaign_code"),'#','\' || '#')) AS "campaign_code_fk_campaignstartdate_bk"
			, UPPER( TO_CHAR("prep_excep"."campaign_start_date", 'DD/MM/YYYY'::varchar)) AS "campaign_start_date_fk_campaignstartdate_bk"
			, "prep_excep"."from_date_seq" AS "from_date_seq"
			, "prep_excep"."from_date" AS "from_date"
			, "prep_excep"."motorcycle_name" AS "motorcycle_name"
			, "prep_excep"."to_date" AS "to_date"
			, "prep_excep"."valid_from_date" AS "valid_from_date"
			, "prep_excep"."valid_to_date" AS "valid_to_date"
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
		, "calculate_bk"."channel_id" AS "channel_id"
		, "calculate_bk"."campaign_code" AS "campaign_code"
		, "calculate_bk"."campaign_start_date" AS "campaign_start_date"
		, "calculate_bk"."campaign_code_fk_campaignstartdate_bk" AS "campaign_code_fk_campaignstartdate_bk"
		, "calculate_bk"."campaign_start_date_fk_campaignstartdate_bk" AS "campaign_start_date_fk_campaignstartdate_bk"
		, "calculate_bk"."from_date_seq" AS "from_date_seq"
		, "calculate_bk"."from_date" AS "from_date"
		, "calculate_bk"."motorcycle_name" AS "motorcycle_name"
		, "calculate_bk"."to_date" AS "to_date"
		, "calculate_bk"."valid_from_date" AS "valid_from_date"
		, "calculate_bk"."valid_to_date" AS "valid_to_date"
		, "calculate_bk"."update_timestamp" AS "update_timestamp"
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 
