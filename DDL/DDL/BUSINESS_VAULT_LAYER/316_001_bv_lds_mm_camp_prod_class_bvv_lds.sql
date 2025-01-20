/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.16, generation date: 2025/01/18 14:08:56
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/16 14:56:23, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08
 */


DROP VIEW IF EXISTS "moto_scn01_bv"."lds_mm_camp_prod_class";
CREATE  VIEW "moto_scn01_bv"."lds_mm_camp_prod_class"  AS 
	SELECT 
		  "dvt_src"."lnd_camp_prod_hkey" AS "lnd_camp_prod_hkey"
		, "dvt_src"."products_hkey" AS "products_hkey"
		, "dvt_src"."campaigns_hkey" AS "campaigns_hkey"
		, "dvt_src"."campaign_code" AS "campaign_code"
		, "dvt_src"."campaign_start_date" AS "campaign_start_date"
		, "dvt_src"."motorcycle_id" AS "motorcycle_id"
		, "dvt_src"."load_date" AS "load_date"
		, "dvt_src"."load_cycle_id" AS "load_cycle_id"
		, "dvt_src"."load_end_date" AS "load_end_date"
		, "dvt_src"."hash_diff" AS "hash_diff"
		, "dvt_src"."delete_flag" AS "delete_flag"
		, "dvt_src"."trans_timestamp" AS "trans_timestamp"
		, "dvt_src"."motorcycle_class_desc" AS "motorcycle_class_desc"
		, "dvt_src"."motorcycle_subclass_desc" AS "motorcycle_subclass_desc"
		, "dvt_src"."update_timestamp" AS "update_timestamp"
	FROM "moto_scn01_fl"."lds_mm_camp_prod_class" "dvt_src"
	;

 
 
