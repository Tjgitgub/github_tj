CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lds_ms_pro_fea_clas_rel_init"() 
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

Vaultspeed version: 5.7.2.16, generation date: 2025/01/16 15:00:22
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/16 14:56:23, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08
 */


BEGIN 

BEGIN -- lds_tgt

	TRUNCATE TABLE "moto_scn01_fl"."lds_ms_pro_fea_clas_rel"  CASCADE;

	INSERT INTO "moto_scn01_fl"."lds_ms_pro_fea_clas_rel"(
		 "lnd_pro_fea_clas_rel_hkey"
		,"products_hkey"
		,"prod_featu_class_hkey"
		,"product_features_hkey"
		,"product_id"
		,"product_feature_class_id"
		,"product_feature_id"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"delete_flag"
		,"trans_timestamp"
		,"update_timestamp"
	)
	WITH "stg_dl_src" AS 
	( 
		SELECT 
			  "stg_dl_inr_src"."lnd_pro_fea_clas_rel_hkey" AS "lnd_pro_fea_clas_rel_hkey"
			, "stg_dl_inr_src"."products_hkey" AS "products_hkey"
			, "stg_dl_inr_src"."prod_featu_class_hkey" AS "prod_featu_class_hkey"
			, "stg_dl_inr_src"."product_features_hkey" AS "product_features_hkey"
			, "stg_dl_inr_src"."product_id" AS "product_id"
			, "stg_dl_inr_src"."product_feature_class_id" AS "product_feature_class_id"
			, "stg_dl_inr_src"."product_feature_id" AS "product_feature_id"
			, "stg_dl_inr_src"."load_date" AS "load_date"
			, "stg_dl_inr_src"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, 'N'::text AS "delete_flag"
			, "stg_dl_inr_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_dl_inr_src"."update_timestamp" AS "update_timestamp"
			, ROW_NUMBER()OVER(PARTITION BY "stg_dl_inr_src"."products_hkey" ,  "stg_dl_inr_src"."product_features_hkey" ORDER BY "stg_dl_inr_src"."load_date") AS "dummy"
		FROM "moto_sales_scn01_stg"."product_feat_class_rel" "stg_dl_inr_src"
	)
	SELECT 
		  "stg_dl_src"."lnd_pro_fea_clas_rel_hkey" AS "lnd_pro_fea_clas_rel_hkey"
		, "stg_dl_src"."products_hkey" AS "products_hkey"
		, "stg_dl_src"."prod_featu_class_hkey" AS "prod_featu_class_hkey"
		, "stg_dl_src"."product_features_hkey" AS "product_features_hkey"
		, "stg_dl_src"."product_id" AS "product_id"
		, "stg_dl_src"."product_feature_class_id" AS "product_feature_class_id"
		, "stg_dl_src"."product_feature_id" AS "product_feature_id"
		, "stg_dl_src"."load_date" AS "load_date"
		, "stg_dl_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_dl_src"."load_end_date" AS "load_end_date"
		, "stg_dl_src"."delete_flag" AS "delete_flag"
		, "stg_dl_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_dl_src"."update_timestamp" AS "update_timestamp"
	FROM "stg_dl_src" "stg_dl_src"
	WHERE  "stg_dl_src"."dummy" = 1
	;
END;



END;
$function$;
 
 
