CREATE OR REPLACE FUNCTION "moto_scn01_proc"."hub_mm_campaigns_init"() 
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

Vaultspeed version: 5.7.2.16, generation date: 2025/01/16 15:01:59
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/16 14:56:23, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46
 */


BEGIN 

BEGIN -- hub_tgt

	TRUNCATE TABLE "moto_scn01_fl"."hub_campaigns"  CASCADE;

	INSERT INTO "moto_scn01_fl"."hub_campaigns"(
		 "campaigns_hkey"
		,"load_date"
		,"load_cycle_id"
		,"campaign_code_bk"
		,"campaign_start_date_bk"
	)
	WITH "change_set" AS 
	( 
		SELECT 
			  "stg_src1"."campaigns_hkey" AS "campaigns_hkey"
			, "stg_src1"."load_date" AS "load_date"
			, "stg_src1"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "logposition"
			, "stg_src1"."campaign_code_bk" AS "campaign_code_bk"
			, "stg_src1"."campaign_start_date_bk" AS "campaign_start_date_bk"
			, 0 AS "general_order"
		FROM "moto_mktg_scn01_stg"."campaigns" "stg_src1"
		UNION ALL 
		SELECT 
			  "stg_fk_src1_1"."campaigns_hkey" AS "campaigns_hkey"
			, "stg_fk_src1_1"."load_date" AS "load_date"
			, "stg_fk_src1_1"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "logposition"
			, "stg_fk_src1_1"."campaign_code_fk_campaignstartdate_bk" AS "campaign_code_bk"
			, "stg_fk_src1_1"."campaign_start_date_fk_campaignstartdate_bk" AS "campaign_start_date_bk"
			, 1 AS "general_order"
		FROM "moto_mktg_scn01_stg"."campaign_motorcycles" "stg_fk_src1_1"
		UNION ALL 
		SELECT 
			  "stg_fk_src1_2"."campaigns_hkey" AS "campaigns_hkey"
			, "stg_fk_src1_2"."load_date" AS "load_date"
			, "stg_fk_src1_2"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "logposition"
			, "stg_fk_src1_2"."campaign_code_fk_campaignstartdate_bk" AS "campaign_code_bk"
			, "stg_fk_src1_2"."campaign_start_date_fk_campaignstartdate_bk" AS "campaign_start_date_bk"
			, 1 AS "general_order"
		FROM "moto_mktg_scn01_stg"."camp_moto_channel" "stg_fk_src1_2"
		UNION ALL 
		SELECT 
			  "stg_fk_src1_3"."campaigns_hkey" AS "campaigns_hkey"
			, "stg_fk_src1_3"."load_date" AS "load_date"
			, "stg_fk_src1_3"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "logposition"
			, "stg_fk_src1_3"."campaign_code_fk_campaignstartdate_bk" AS "campaign_code_bk"
			, "stg_fk_src1_3"."campaign_start_date_fk_campaignstartdate_bk" AS "campaign_start_date_bk"
			, 1 AS "general_order"
		FROM "moto_mktg_scn01_stg"."camp_part_cont" "stg_fk_src1_3"
		UNION ALL 
		SELECT 
			  "stg_fk_src1_4"."campaigns_hkey" AS "campaigns_hkey"
			, "stg_fk_src1_4"."load_date" AS "load_date"
			, "stg_fk_src1_4"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "logposition"
			, "stg_fk_src1_4"."campaign_code_fk_campaignstartdate_bk" AS "campaign_code_bk"
			, "stg_fk_src1_4"."campaign_start_date_fk_campaignstartdate_bk" AS "campaign_start_date_bk"
			, 1 AS "general_order"
		FROM "moto_mktg_scn01_stg"."camp_moto_chan_region" "stg_fk_src1_4"
	)
	, "min_load_time" AS 
	( 
		SELECT 
			  "change_set"."campaigns_hkey" AS "campaigns_hkey"
			, "change_set"."load_date" AS "load_date"
			, "change_set"."load_cycle_id" AS "load_cycle_id"
			, "change_set"."campaign_code_bk" AS "campaign_code_bk"
			, "change_set"."campaign_start_date_bk" AS "campaign_start_date_bk"
			, ROW_NUMBER()OVER(PARTITION BY "change_set"."campaigns_hkey" ORDER BY "change_set"."general_order",
				"change_set"."load_date","change_set"."logposition") AS "dummy"
		FROM "change_set" "change_set"
	)
	SELECT 
		  "min_load_time"."campaigns_hkey" AS "campaigns_hkey"
		, "min_load_time"."load_date" AS "load_date"
		, "min_load_time"."load_cycle_id" AS "load_cycle_id"
		, "min_load_time"."campaign_code_bk" AS "campaign_code_bk"
		, "min_load_time"."campaign_start_date_bk" AS "campaign_start_date_bk"
	FROM "min_load_time" "min_load_time"
	WHERE  "min_load_time"."dummy" = 1
	;
END;



END;
$function$;
 
 
