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


DROP VIEW IF EXISTS "moto_scn01_bv"."sat_ms_product_features";
CREATE  VIEW "moto_scn01_bv"."sat_ms_product_features"  AS 
	SELECT 
		  "dvt_src"."product_features_hkey" AS "product_features_hkey"
		, "dvt_src"."load_date" AS "load_date"
		, "dvt_src"."load_cycle_id" AS "load_cycle_id"
		, "dvt_src"."load_end_date" AS "load_end_date"
		, "dvt_src"."hash_diff" AS "hash_diff"
		, "dvt_src"."delete_flag" AS "delete_flag"
		, "dvt_src"."trans_timestamp" AS "trans_timestamp"
		, "dvt_src"."product_feature_id" AS "product_feature_id"
		, "dvt_src"."product_feature_code" AS "product_feature_code"
		, "dvt_src"."pro_fea_lan_code_seq" AS "pro_fea_lan_code_seq"
		, "dvt_src"."product_feature_language_code" AS "product_feature_language_code"
		, "dvt_src"."product_feature_description" AS "product_feature_description"
		, "dvt_src"."update_timestamp" AS "update_timestamp"
	FROM "moto_scn01_fl"."sat_ms_product_features" "dvt_src"
	;

 
 
