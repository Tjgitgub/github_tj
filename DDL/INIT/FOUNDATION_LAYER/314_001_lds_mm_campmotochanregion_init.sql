CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lds_mm_campmotochanregion_init"() 
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

BEGIN -- lds_tgt

	TRUNCATE TABLE "moto_scn01_fl"."lds_mm_camp_moto_chan_region"  CASCADE;

	INSERT INTO "moto_scn01_fl"."lds_mm_camp_moto_chan_region"(
		 "lnd_camp_moto_chan_region_hkey"
		,"channels_hkey"
		,"products_hkey"
		,"campaigns_hkey"
		,"campaign_code"
		,"campaign_start_date"
		,"channel_id"
		,"motorcycle_id"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"delete_flag"
		,"trans_timestamp"
		,"region_seq"
		,"update_timestamp"
	)
	WITH "stg_dl_src" AS 
	( 
		SELECT 
			  "stg_dl_inr_src"."lnd_camp_moto_chan_region_hkey" AS "lnd_camp_moto_chan_region_hkey"
			, "stg_dl_inr_src"."channels_hkey" AS "channels_hkey"
			, "stg_dl_inr_src"."products_hkey" AS "products_hkey"
			, "stg_dl_inr_src"."campaigns_hkey" AS "campaigns_hkey"
			, "stg_dl_inr_src"."campaign_code" AS "campaign_code"
			, "stg_dl_inr_src"."campaign_start_date" AS "campaign_start_date"
			, "stg_dl_inr_src"."channel_id" AS "channel_id"
			, "stg_dl_inr_src"."motorcycle_id" AS "motorcycle_id"
			, "stg_dl_inr_src"."load_date" AS "load_date"
			, "stg_dl_inr_src"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, 'N'::text AS "delete_flag"
			, "stg_dl_inr_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_dl_inr_src"."region_seq" AS "region_seq"
			, "stg_dl_inr_src"."update_timestamp" AS "update_timestamp"
			, ROW_NUMBER()OVER(PARTITION BY "stg_dl_inr_src"."channels_hkey" ,  "stg_dl_inr_src"."campaigns_hkey",
				"stg_dl_inr_src"."region_seq" ORDER BY "stg_dl_inr_src"."load_date") AS "dummy"
		FROM "moto_mktg_scn01_stg"."camp_moto_chan_region" "stg_dl_inr_src"
		WHERE  "stg_dl_inr_src"."error_code_camp_moto_chan_region" = 0
	)
	SELECT 
		  "stg_dl_src"."lnd_camp_moto_chan_region_hkey" AS "lnd_camp_moto_chan_region_hkey"
		, "stg_dl_src"."channels_hkey" AS "channels_hkey"
		, "stg_dl_src"."products_hkey" AS "products_hkey"
		, "stg_dl_src"."campaigns_hkey" AS "campaigns_hkey"
		, "stg_dl_src"."campaign_code" AS "campaign_code"
		, "stg_dl_src"."campaign_start_date" AS "campaign_start_date"
		, "stg_dl_src"."channel_id" AS "channel_id"
		, "stg_dl_src"."motorcycle_id" AS "motorcycle_id"
		, "stg_dl_src"."load_date" AS "load_date"
		, "stg_dl_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_dl_src"."load_end_date" AS "load_end_date"
		, "stg_dl_src"."delete_flag" AS "delete_flag"
		, "stg_dl_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_dl_src"."region_seq" AS "region_seq"
		, "stg_dl_src"."update_timestamp" AS "update_timestamp"
	FROM "stg_dl_src" "stg_dl_src"
	WHERE  "stg_dl_src"."dummy" = 1
	;
END;



END;
$function$;
 
 
