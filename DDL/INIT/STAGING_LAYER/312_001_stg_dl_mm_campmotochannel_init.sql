CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_dl_mm_campmotochannel_init"() 
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

BEGIN -- stg_dl_tgt

	TRUNCATE TABLE "moto_mktg_scn01_stg"."camp_moto_channel"  CASCADE;

	INSERT INTO "moto_mktg_scn01_stg"."camp_moto_channel"(
		 "lnd_camp_moto_channel_hkey"
		,"channels_hkey"
		,"campaigns_hkey"
		,"load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"channel_id"
		,"campaign_code"
		,"campaign_start_date"
		,"channel_code_fk_channelid_bk"
		,"campaign_code_fk_campaignstartdate_bk"
		,"campaign_start_date_fk_campaignstartdate_bk"
		,"from_date_seq"
		,"from_date"
		,"motorcycle_name"
		,"to_date"
		,"valid_from_date"
		,"valid_to_date"
		,"update_timestamp"
		,"error_code_camp_moto_channel"
	)
	WITH "dist_fk1" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src1"."channel_id" AS "channel_id"
		FROM "moto_mktg_scn01_ext"."camp_moto_channel" "ext_dis_src1"
	)
	, "prep_find_bk_fk1" AS 
	( 
		SELECT 
			  "hub_src1"."channel_code_bk" AS "channel_code_bk"
			, "dist_fk1"."channel_id" AS "channel_id"
			, "sat_src1"."load_date" AS "load_date"
			, 1 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_scn01_fl"."sat_mm_channels" "sat_src1" ON  "dist_fk1"."channel_id" = "sat_src1"."channel_id"
		INNER JOIN "moto_scn01_fl"."hub_channels" "hub_src1" ON  "hub_src1"."channels_hkey" = "sat_src1"."channels_hkey"
		WHERE  "sat_src1"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
		UNION ALL 
		SELECT 
			  "ext_fkbk_src1"."channel_code_bk" AS "channel_code_bk"
			, "dist_fk1"."channel_id" AS "channel_id"
			, "ext_fkbk_src1"."load_date" AS "load_date"
			, 0 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_mktg_scn01_ext"."channels" "ext_fkbk_src1" ON  "dist_fk1"."channel_id" = "ext_fkbk_src1"."channel_id"
	)
	, "order_bk_fk1" AS 
	( 
		SELECT 
			  "prep_find_bk_fk1"."channel_code_bk" AS "channel_code_bk"
			, "prep_find_bk_fk1"."channel_id" AS "channel_id"
			, ROW_NUMBER()OVER(PARTITION BY "prep_find_bk_fk1"."channel_id" ORDER BY "prep_find_bk_fk1"."general_order",
				"prep_find_bk_fk1"."load_date" DESC) AS "dummy"
		FROM "prep_find_bk_fk1" "prep_find_bk_fk1"
	)
	, "find_bk_fk1" AS 
	( 
		SELECT 
			  "order_bk_fk1"."channel_code_bk" AS "channel_code_bk"
			, "order_bk_fk1"."channel_id" AS "channel_id"
		FROM "order_bk_fk1" "order_bk_fk1"
		WHERE  "order_bk_fk1"."dummy" = 1
	)
	SELECT 
		  UPPER(ENCODE(DIGEST(  COALESCE("find_bk_fk1"."channel_code_bk","mex_src"."key_attribute_varchar")|| '#' || 
			"ext_src"."campaign_code_fk_campaignstartdate_bk" || '#' ||  "ext_src"."campaign_start_date_fk_campaignstartdate_bk" || '#'  ,'MD5'),'HEX')) AS "lnd_camp_moto_channel_hkey"
		, UPPER(ENCODE(DIGEST( COALESCE("find_bk_fk1"."channel_code_bk","mex_src"."key_attribute_varchar")|| '#' ,'MD5')
			,'HEX')) AS "channels_hkey"
		, UPPER(ENCODE(DIGEST( "ext_src"."campaign_code_fk_campaignstartdate_bk" || '#' ||  "ext_src"."campaign_start_date_fk_campaignstartdate_bk" || 
			'#' ,'MD5'),'HEX')) AS "campaigns_hkey"
		, "ext_src"."load_date" AS "load_date"
		, "ext_src"."load_cycle_id" AS "load_cycle_id"
		, "ext_src"."trans_timestamp" AS "trans_timestamp"
		, "ext_src"."operation" AS "operation"
		, "ext_src"."record_type" AS "record_type"
		, "ext_src"."channel_id" AS "channel_id"
		, "ext_src"."campaign_code" AS "campaign_code"
		, "ext_src"."campaign_start_date" AS "campaign_start_date"
		, NULL::text AS "channel_code_fk_channelid_bk"
		, "ext_src"."campaign_code_fk_campaignstartdate_bk" AS "campaign_code_fk_campaignstartdate_bk"
		, "ext_src"."campaign_start_date_fk_campaignstartdate_bk" AS "campaign_start_date_fk_campaignstartdate_bk"
		, "ext_src"."from_date_seq" AS "from_date_seq"
		, "ext_src"."from_date" AS "from_date"
		, "ext_src"."motorcycle_name" AS "motorcycle_name"
		, "ext_src"."to_date" AS "to_date"
		, "ext_src"."valid_from_date" AS "valid_from_date"
		, "ext_src"."valid_to_date" AS "valid_to_date"
		, "ext_src"."update_timestamp" AS "update_timestamp"
		, CASE WHEN  "find_bk_fk1"."channel_id" IS NULL  THEN 1 ELSE 0 END AS "error_code_camp_moto_channel"
	FROM "moto_mktg_scn01_ext"."camp_moto_channel" "ext_src"
	INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
	LEFT OUTER JOIN "find_bk_fk1" "find_bk_fk1" ON  "ext_src"."channel_id" = "find_bk_fk1"."channel_id"
	;
END;


BEGIN -- ext_err_tgt

	TRUNCATE TABLE "moto_mktg_scn01_ext"."camp_moto_channel_err"  CASCADE;

	INSERT INTO "moto_mktg_scn01_ext"."camp_moto_channel_err"(
		 "load_date"
		,"load_cycle_id"
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
		,"error_code_camp_moto_channel"
	)
	SELECT 
		  "stg_err_src"."load_date" AS "load_date"
		, "stg_err_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_err_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_err_src"."operation" AS "operation"
		, "stg_err_src"."record_type" AS "record_type"
		, "stg_err_src"."channel_id" AS "channel_id"
		, "stg_err_src"."campaign_code" AS "campaign_code"
		, "stg_err_src"."campaign_start_date" AS "campaign_start_date"
		, "stg_err_src"."campaign_code_fk_campaignstartdate_bk" AS "campaign_code_fk_campaignstartdate_bk"
		, "stg_err_src"."campaign_start_date_fk_campaignstartdate_bk" AS "campaign_start_date_fk_campaignstartdate_bk"
		, "stg_err_src"."from_date_seq" AS "from_date_seq"
		, "stg_err_src"."from_date" AS "from_date"
		, "stg_err_src"."motorcycle_name" AS "motorcycle_name"
		, "stg_err_src"."to_date" AS "to_date"
		, "stg_err_src"."valid_from_date" AS "valid_from_date"
		, "stg_err_src"."valid_to_date" AS "valid_to_date"
		, "stg_err_src"."update_timestamp" AS "update_timestamp"
		, "stg_err_src"."error_code_camp_moto_channel" AS "error_code_camp_moto_channel"
	FROM "moto_mktg_scn01_stg"."camp_moto_channel" "stg_err_src"
	WHERE ( "stg_err_src"."error_code_camp_moto_channel" > 0)AND "stg_err_src"."load_cycle_id" >= 0
	;
END;



END;
$function$;
 
 
