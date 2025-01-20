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


DROP VIEW IF EXISTS "moto_scn01_bv"."lds_ms_pro_fea_clas_rel";
CREATE  VIEW "moto_scn01_bv"."lds_ms_pro_fea_clas_rel"  AS 
	SELECT 
		  "dvt_src"."lnd_pro_fea_clas_rel_hkey" AS "lnd_pro_fea_clas_rel_hkey"
		, "dvt_src"."products_hkey" AS "products_hkey"
		, "dvt_src"."prod_featu_class_hkey" AS "prod_featu_class_hkey"
		, "dvt_src"."product_features_hkey" AS "product_features_hkey"
		, "dvt_src"."product_id" AS "product_id"
		, "dvt_src"."product_feature_class_id" AS "product_feature_class_id"
		, "dvt_src"."product_feature_id" AS "product_feature_id"
		, "dvt_src"."load_date" AS "load_date"
		, "dvt_src"."load_cycle_id" AS "load_cycle_id"
		, "dvt_src"."load_end_date" AS "load_end_date"
		, "dvt_src"."delete_flag" AS "delete_flag"
		, "dvt_src"."trans_timestamp" AS "trans_timestamp"
		, "dvt_src"."update_timestamp" AS "update_timestamp"
	FROM "moto_scn01_fl"."lds_ms_pro_fea_clas_rel" "dvt_src"
	;

 
 
