/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.14, generation date: 2025/01/09 12:50:01
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/09 09:40:46, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04
 */


DROP VIEW IF EXISTS "moto_scn01_bv"."lds_mm_camp_moto_channel";
CREATE  VIEW "moto_scn01_bv"."lds_mm_camp_moto_channel"  AS 
	SELECT 
		  "dvt_src"."lnd_camp_moto_channel_hkey" AS "lnd_camp_moto_channel_hkey"
		, "dvt_src"."channels_hkey" AS "channels_hkey"
		, "dvt_src"."campaigns_hkey" AS "campaigns_hkey"
		, "dvt_src"."campaign_code" AS "campaign_code"
		, "dvt_src"."campaign_start_date" AS "campaign_start_date"
		, "dvt_src"."channel_id" AS "channel_id"
		, "dvt_src"."load_date" AS "load_date"
		, "dvt_src"."load_cycle_id" AS "load_cycle_id"
		, "dvt_src"."load_end_date" AS "load_end_date"
		, "dvt_src"."hash_diff" AS "hash_diff"
		, "dvt_src"."delete_flag" AS "delete_flag"
		, "dvt_src"."trans_timestamp" AS "trans_timestamp"
		, "dvt_src"."from_date_seq" AS "from_date_seq"
		, "dvt_src"."from_date" AS "from_date"
		, "dvt_src"."motorcycle_name" AS "motorcycle_name"
		, "dvt_src"."to_date" AS "to_date"
		, "dvt_src"."valid_from_date" AS "valid_from_date"
		, "dvt_src"."valid_to_date" AS "valid_to_date"
		, "dvt_src"."update_timestamp" AS "update_timestamp"
	FROM "moto_scn01_fl"."lds_mm_camp_moto_channel" "dvt_src"
	;

 
 
