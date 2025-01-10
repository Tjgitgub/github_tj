CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_ms_productfeatures_incr"() 
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

Vaultspeed version: 5.7.2.14, generation date: 2025/01/09 12:47:43
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/09 09:40:46, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04
 */


BEGIN 

BEGIN -- stg_tgt

	TRUNCATE TABLE "moto_sales_scn01_stg"."product_features"  CASCADE;

	INSERT INTO "moto_sales_scn01_stg"."product_features"(
		 "product_features_hkey"
		,"produ_featur_cat_hkey"
		,"lnk_prfe_pfca_hkey"
		,"load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"product_feature_id"
		,"product_feature_cat_id"
		,"product_feature_code"
		,"produ_featu_code_bk"
		,"pro_fea_cat_code_fk_productfeaturecatid_bk"
		,"pro_fea_lan_code_seq"
		,"product_feature_language_code"
		,"product_feature_description"
		,"update_timestamp"
		,"error_code_prfe_pfca"
	)
	WITH "dist_fk1" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src1"."product_feature_cat_id" AS "product_feature_cat_id"
		FROM "moto_sales_scn01_ext"."product_features" "ext_dis_src1"
	)
	, "prep_find_bk_fk1" AS 
	( 
		SELECT 
			  "hub_src1"."pro_fea_cat_code_bk" AS "pro_fea_cat_code_bk"
			, "dist_fk1"."product_feature_cat_id" AS "product_feature_cat_id"
			, "sat_src1"."load_date" AS "load_date"
			, 1 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_scn01_fl"."sat_ms_produ_featur_cat" "sat_src1" ON  "dist_fk1"."product_feature_cat_id" = "sat_src1"."product_feature_category_id"
		INNER JOIN "moto_scn01_fl"."hub_produ_featur_cat" "hub_src1" ON  "hub_src1"."produ_featur_cat_hkey" = "sat_src1"."produ_featur_cat_hkey"
		WHERE  "sat_src1"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
		UNION ALL 
		SELECT 
			  "ext_fkbk_src1"."pro_fea_cat_code_bk" AS "pro_fea_cat_code_bk"
			, "dist_fk1"."product_feature_cat_id" AS "product_feature_cat_id"
			, "ext_fkbk_src1"."load_date" AS "load_date"
			, 0 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_sales_scn01_ext"."product_feature_cat" "ext_fkbk_src1" ON  "dist_fk1"."product_feature_cat_id" = "ext_fkbk_src1"."product_feature_category_id"
	)
	, "order_bk_fk1" AS 
	( 
		SELECT 
			  "prep_find_bk_fk1"."pro_fea_cat_code_bk" AS "pro_fea_cat_code_bk"
			, "prep_find_bk_fk1"."product_feature_cat_id" AS "product_feature_cat_id"
			, ROW_NUMBER()OVER(PARTITION BY "prep_find_bk_fk1"."product_feature_cat_id" ORDER BY "prep_find_bk_fk1"."general_order",
				"prep_find_bk_fk1"."load_date" DESC) AS "dummy"
		FROM "prep_find_bk_fk1" "prep_find_bk_fk1"
	)
	, "find_bk_fk1" AS 
	( 
		SELECT 
			  "order_bk_fk1"."pro_fea_cat_code_bk" AS "pro_fea_cat_code_bk"
			, "order_bk_fk1"."product_feature_cat_id" AS "product_feature_cat_id"
		FROM "order_bk_fk1" "order_bk_fk1"
		WHERE  "order_bk_fk1"."dummy" = 1
	)
	, "calc_hash_keys" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST( "ext_src"."produ_featu_code_bk" || '#' ,'MD5'),'HEX')) AS "product_features_hkey"
			, UPPER(ENCODE(DIGEST( COALESCE("find_bk_fk1"."pro_fea_cat_code_bk","mex_src"."key_attribute_varchar")|| '#' ,
				'MD5'),'HEX')) AS "produ_featur_cat_hkey"
			, UPPER(ENCODE(DIGEST( "ext_src"."produ_featu_code_bk" || '#' || COALESCE("find_bk_fk1"."pro_fea_cat_code_bk",
				"mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "lnk_prfe_pfca_hkey"
			, "ext_src"."load_date" AS "load_date"
			, "ext_src"."load_cycle_id" AS "load_cycle_id"
			, "ext_src"."trans_timestamp" AS "trans_timestamp"
			, "ext_src"."operation" AS "operation"
			, "ext_src"."record_type" AS "record_type"
			, "ext_src"."product_feature_id" AS "product_feature_id"
			, "ext_src"."product_feature_cat_id" AS "product_feature_cat_id"
			, "ext_src"."product_feature_code" AS "product_feature_code"
			, "ext_src"."produ_featu_code_bk" AS "produ_featu_code_bk"
			, NULL::text AS "pro_fea_cat_code_fk_productfeaturecatid_bk"
			, "ext_src"."pro_fea_lan_code_seq" AS "pro_fea_lan_code_seq"
			, "ext_src"."product_feature_language_code" AS "product_feature_language_code"
			, "ext_src"."product_feature_description" AS "product_feature_description"
			, "ext_src"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "ext_src"."error_code_prfe_pfca" = 2 THEN 2 WHEN "ext_src"."error_code_prfe_pfca" =
				- 1 THEN 0 WHEN "find_bk_fk1"."product_feature_cat_id" IS NULL THEN "ext_src"."error_code_prfe_pfca" ELSE 0 END AS "error_code_prfe_pfca"
		FROM "moto_sales_scn01_ext"."product_features" "ext_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
		LEFT OUTER JOIN "find_bk_fk1" "find_bk_fk1" ON  "ext_src"."product_feature_cat_id" = "find_bk_fk1"."product_feature_cat_id"
	)
	SELECT 
		  "calc_hash_keys"."product_features_hkey" AS "product_features_hkey"
		, "calc_hash_keys"."produ_featur_cat_hkey" AS "produ_featur_cat_hkey"
		, "calc_hash_keys"."lnk_prfe_pfca_hkey" AS "lnk_prfe_pfca_hkey"
		, "calc_hash_keys"."load_date" AS "load_date"
		, "calc_hash_keys"."load_cycle_id" AS "load_cycle_id"
		, "calc_hash_keys"."trans_timestamp" AS "trans_timestamp"
		, "calc_hash_keys"."operation" AS "operation"
		, "calc_hash_keys"."record_type" AS "record_type"
		, "calc_hash_keys"."product_feature_id" AS "product_feature_id"
		, "calc_hash_keys"."product_feature_cat_id" AS "product_feature_cat_id"
		, "calc_hash_keys"."product_feature_code" AS "product_feature_code"
		, "calc_hash_keys"."produ_featu_code_bk" AS "produ_featu_code_bk"
		, "calc_hash_keys"."pro_fea_cat_code_fk_productfeaturecatid_bk" AS "pro_fea_cat_code_fk_productfeaturecatid_bk"
		, "calc_hash_keys"."pro_fea_lan_code_seq" AS "pro_fea_lan_code_seq"
		, "calc_hash_keys"."product_feature_language_code" AS "product_feature_language_code"
		, "calc_hash_keys"."product_feature_description" AS "product_feature_description"
		, "calc_hash_keys"."update_timestamp" AS "update_timestamp"
		, "calc_hash_keys"."error_code_prfe_pfca" AS "error_code_prfe_pfca"
	FROM "calc_hash_keys" "calc_hash_keys"
	;
END;


BEGIN -- ext_err_tgt

	TRUNCATE TABLE "moto_sales_scn01_ext"."product_features_err"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."product_features_err"(
		 "load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"product_feature_id"
		,"product_feature_cat_id"
		,"product_feature_code"
		,"produ_featu_code_bk"
		,"pro_fea_lan_code_seq"
		,"product_feature_language_code"
		,"product_feature_description"
		,"update_timestamp"
		,"error_code_prfe_pfca"
	)
	SELECT 
		  CASE WHEN "stg_err_src"."error_code_prfe_pfca" = 1 THEN "stg_err_src"."load_date" + interval'1 microsecond'   ELSE "stg_err_src"."load_date" END AS "load_date"
		, "stg_err_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_err_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_err_src"."operation" AS "operation"
		, "stg_err_src"."record_type" AS "record_type"
		, "stg_err_src"."product_feature_id" AS "product_feature_id"
		, "stg_err_src"."product_feature_cat_id" AS "product_feature_cat_id"
		, "stg_err_src"."product_feature_code" AS "product_feature_code"
		, "stg_err_src"."produ_featu_code_bk" AS "produ_featu_code_bk"
		, "stg_err_src"."pro_fea_lan_code_seq" AS "pro_fea_lan_code_seq"
		, "stg_err_src"."product_feature_language_code" AS "product_feature_language_code"
		, "stg_err_src"."product_feature_description" AS "product_feature_description"
		, "stg_err_src"."update_timestamp" AS "update_timestamp"
		, "stg_err_src"."error_code_prfe_pfca" AS "error_code_prfe_pfca"
	FROM "moto_sales_scn01_stg"."product_features" "stg_err_src"
	WHERE ( "stg_err_src"."error_code_prfe_pfca" IN(1,3))AND "stg_err_src"."load_cycle_id" >= 0
	;
END;



END;
$function$;
 
 
