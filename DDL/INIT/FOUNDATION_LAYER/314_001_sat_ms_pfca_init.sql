CREATE OR REPLACE FUNCTION "moto_scn01_proc"."sat_ms_pfca_init"() 
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

BEGIN -- sat_tgt

	TRUNCATE TABLE "moto_scn01_fl"."sat_ms_produ_featur_cat"  CASCADE;

	INSERT INTO "moto_scn01_fl"."sat_ms_produ_featur_cat"(
		 "produ_featur_cat_hkey"
		,"load_date"
		,"load_end_date"
		,"load_cycle_id"
		,"hash_diff"
		,"delete_flag"
		,"trans_timestamp"
		,"product_feature_category_id"
		,"product_feature_category_code"
		,"pr_fe_ca_lan_cod_seq"
		,"prod_feat_cat_language_code"
		,"prod_feat_cat_description"
		,"update_timestamp"
	)
	WITH "stg_src" AS 
	( 
		SELECT 
			  "stg_inr_src"."produ_featur_cat_hkey" AS "produ_featur_cat_hkey"
			, "stg_inr_src"."load_date" AS "load_date"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, "stg_inr_src"."load_cycle_id" AS "load_cycle_id"
			, UPPER(ENCODE(DIGEST(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "stg_inr_src"."prod_feat_cat_description"),
				'~'),'#','\' || '#'))|| '#','#' || '~'),'~') ,'MD5'),'HEX')) AS "hash_diff"
			, 'N'::text AS "delete_flag"
			, "stg_inr_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_inr_src"."product_feature_category_id" AS "product_feature_category_id"
			, "stg_inr_src"."product_feature_category_code" AS "product_feature_category_code"
			, "stg_inr_src"."pr_fe_ca_lan_cod_seq" AS "pr_fe_ca_lan_cod_seq"
			, "stg_inr_src"."prod_feat_cat_language_code" AS "prod_feat_cat_language_code"
			, "stg_inr_src"."prod_feat_cat_description" AS "prod_feat_cat_description"
			, "stg_inr_src"."update_timestamp" AS "update_timestamp"
			, ROW_NUMBER()OVER(PARTITION BY "stg_inr_src"."produ_featur_cat_hkey","stg_inr_src"."pr_fe_ca_lan_cod_seq" ORDER BY "stg_inr_src"."load_date") AS "dummy"
		FROM "moto_sales_scn01_stg"."product_feature_cat" "stg_inr_src"
	)
	SELECT 
		  "stg_src"."produ_featur_cat_hkey" AS "produ_featur_cat_hkey"
		, "stg_src"."load_date" AS "load_date"
		, "stg_src"."load_end_date" AS "load_end_date"
		, "stg_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_src"."hash_diff" AS "hash_diff"
		, "stg_src"."delete_flag" AS "delete_flag"
		, "stg_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_src"."product_feature_category_id" AS "product_feature_category_id"
		, "stg_src"."product_feature_category_code" AS "product_feature_category_code"
		, "stg_src"."pr_fe_ca_lan_cod_seq" AS "pr_fe_ca_lan_cod_seq"
		, "stg_src"."prod_feat_cat_language_code" AS "prod_feat_cat_language_code"
		, "stg_src"."prod_feat_cat_description" AS "prod_feat_cat_description"
		, "stg_src"."update_timestamp" AS "update_timestamp"
	FROM "stg_src" "stg_src"
	WHERE  "stg_src"."dummy" = 1
	;
END;



END;
$function$;
 
 
