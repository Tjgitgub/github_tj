CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_dl_ms_productfeatclassrel_init"() 
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

BEGIN -- stg_dl_tgt

	TRUNCATE TABLE "moto_sales_scn01_stg"."product_feat_class_rel"  CASCADE;

	INSERT INTO "moto_sales_scn01_stg"."product_feat_class_rel"(
		 "lnd_pro_fea_clas_rel_hkey"
		,"products_hkey"
		,"prod_featu_class_hkey"
		,"product_features_hkey"
		,"load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"product_feature_id"
		,"product_id"
		,"product_feature_class_id"
		,"product_cc_fk_productid_bk"
		,"product_et_code_fk_productid_bk"
		,"product_part_code_fk_productid_bk"
		,"pro_fea_cla_code_fk_productfeatureclassid_bk"
		,"produ_featu_code_fk_productfeatureid_bk"
		,"update_timestamp"
		,"error_code_pro_fea_clas_rel"
	)
	WITH "dist_fk1" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src1"."product_id" AS "product_id"
		FROM "moto_sales_scn01_ext"."product_feat_class_rel" "ext_dis_src1"
	)
	, "dist_fk2" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src2"."product_feature_class_id" AS "product_feature_class_id"
		FROM "moto_sales_scn01_ext"."product_feat_class_rel" "ext_dis_src2"
	)
	, "dist_fk3" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src3"."product_feature_id" AS "product_feature_id"
		FROM "moto_sales_scn01_ext"."product_feat_class_rel" "ext_dis_src3"
	)
	, "prep_find_bk_fk1" AS 
	( 
		SELECT 
			  "hub_src1"."product_cc_bk" AS "product_cc_bk"
			, "hub_src1"."product_et_code_bk" AS "product_et_code_bk"
			, "hub_src1"."product_part_code_bk" AS "product_part_code_bk"
			, "dist_fk1"."product_id" AS "product_id"
			, "sat_src1"."load_date" AS "load_date"
			, 1 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_scn01_fl"."sat_ms_products" "sat_src1" ON  "dist_fk1"."product_id" = "sat_src1"."product_id"
		INNER JOIN "moto_scn01_fl"."hub_products" "hub_src1" ON  "hub_src1"."products_hkey" = "sat_src1"."products_hkey"
		WHERE  "sat_src1"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
		UNION ALL 
		SELECT 
			  "ext_fkbk_src1"."product_cc_bk" AS "product_cc_bk"
			, "ext_fkbk_src1"."product_et_code_bk" AS "product_et_code_bk"
			, "ext_fkbk_src1"."product_part_code_bk" AS "product_part_code_bk"
			, "dist_fk1"."product_id" AS "product_id"
			, "ext_fkbk_src1"."load_date" AS "load_date"
			, 0 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_sales_scn01_ext"."products" "ext_fkbk_src1" ON  "dist_fk1"."product_id" = "ext_fkbk_src1"."product_id"
	)
	, "prep_find_bk_fk2" AS 
	( 
		SELECT 
			  "hub_src2"."pro_fea_cla_code_bk" AS "pro_fea_cla_code_bk"
			, "dist_fk2"."product_feature_class_id" AS "product_feature_class_id"
			, "sat_src2"."load_date" AS "load_date"
			, 1 AS "general_order"
		FROM "dist_fk2" "dist_fk2"
		INNER JOIN "moto_scn01_fl"."sat_ms_prod_featu_class" "sat_src2" ON  "dist_fk2"."product_feature_class_id" = "sat_src2"."product_feature_class_id"
		INNER JOIN "moto_scn01_fl"."hub_prod_featu_class" "hub_src2" ON  "hub_src2"."prod_featu_class_hkey" = "sat_src2"."prod_featu_class_hkey"
		WHERE  "sat_src2"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
		UNION ALL 
		SELECT 
			  "ext_fkbk_src2"."pro_fea_cla_code_bk" AS "pro_fea_cla_code_bk"
			, "dist_fk2"."product_feature_class_id" AS "product_feature_class_id"
			, "ext_fkbk_src2"."load_date" AS "load_date"
			, 0 AS "general_order"
		FROM "dist_fk2" "dist_fk2"
		INNER JOIN "moto_sales_scn01_ext"."product_feature_class" "ext_fkbk_src2" ON  "dist_fk2"."product_feature_class_id" = "ext_fkbk_src2"."product_feature_class_id"
	)
	, "prep_find_bk_fk3" AS 
	( 
		SELECT 
			  "hub_src3"."produ_featu_code_bk" AS "produ_featu_code_bk"
			, "dist_fk3"."product_feature_id" AS "product_feature_id"
			, "sat_src3"."load_date" AS "load_date"
			, 1 AS "general_order"
		FROM "dist_fk3" "dist_fk3"
		INNER JOIN "moto_scn01_fl"."sat_ms_product_features" "sat_src3" ON  "dist_fk3"."product_feature_id" = "sat_src3"."product_feature_id"
		INNER JOIN "moto_scn01_fl"."hub_product_features" "hub_src3" ON  "hub_src3"."product_features_hkey" = "sat_src3"."product_features_hkey"
		WHERE  "sat_src3"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
		UNION ALL 
		SELECT 
			  "ext_fkbk_src3"."produ_featu_code_bk" AS "produ_featu_code_bk"
			, "dist_fk3"."product_feature_id" AS "product_feature_id"
			, "ext_fkbk_src3"."load_date" AS "load_date"
			, 0 AS "general_order"
		FROM "dist_fk3" "dist_fk3"
		INNER JOIN "moto_sales_scn01_ext"."product_features" "ext_fkbk_src3" ON  "dist_fk3"."product_feature_id" = "ext_fkbk_src3"."product_feature_id"
	)
	, "order_bk_fk1" AS 
	( 
		SELECT 
			  "prep_find_bk_fk1"."product_cc_bk" AS "product_cc_bk"
			, "prep_find_bk_fk1"."product_et_code_bk" AS "product_et_code_bk"
			, "prep_find_bk_fk1"."product_part_code_bk" AS "product_part_code_bk"
			, "prep_find_bk_fk1"."product_id" AS "product_id"
			, ROW_NUMBER()OVER(PARTITION BY "prep_find_bk_fk1"."product_id" ORDER BY "prep_find_bk_fk1"."general_order",
				"prep_find_bk_fk1"."load_date" DESC) AS "dummy"
		FROM "prep_find_bk_fk1" "prep_find_bk_fk1"
	)
	, "order_bk_fk2" AS 
	( 
		SELECT 
			  "prep_find_bk_fk2"."pro_fea_cla_code_bk" AS "pro_fea_cla_code_bk"
			, "prep_find_bk_fk2"."product_feature_class_id" AS "product_feature_class_id"
			, ROW_NUMBER()OVER(PARTITION BY "prep_find_bk_fk2"."product_feature_class_id" ORDER BY "prep_find_bk_fk2"."general_order",
				"prep_find_bk_fk2"."load_date" DESC) AS "dummy"
		FROM "prep_find_bk_fk2" "prep_find_bk_fk2"
	)
	, "order_bk_fk3" AS 
	( 
		SELECT 
			  "prep_find_bk_fk3"."produ_featu_code_bk" AS "produ_featu_code_bk"
			, "prep_find_bk_fk3"."product_feature_id" AS "product_feature_id"
			, ROW_NUMBER()OVER(PARTITION BY "prep_find_bk_fk3"."product_feature_id" ORDER BY "prep_find_bk_fk3"."general_order",
				"prep_find_bk_fk3"."load_date" DESC) AS "dummy"
		FROM "prep_find_bk_fk3" "prep_find_bk_fk3"
	)
	, "find_bk_fk1" AS 
	( 
		SELECT 
			  "order_bk_fk1"."product_cc_bk" AS "product_cc_bk"
			, "order_bk_fk1"."product_et_code_bk" AS "product_et_code_bk"
			, "order_bk_fk1"."product_part_code_bk" AS "product_part_code_bk"
			, "order_bk_fk1"."product_id" AS "product_id"
		FROM "order_bk_fk1" "order_bk_fk1"
		WHERE  "order_bk_fk1"."dummy" = 1
	)
	, "find_bk_fk2" AS 
	( 
		SELECT 
			  "order_bk_fk2"."pro_fea_cla_code_bk" AS "pro_fea_cla_code_bk"
			, "order_bk_fk2"."product_feature_class_id" AS "product_feature_class_id"
		FROM "order_bk_fk2" "order_bk_fk2"
		WHERE  "order_bk_fk2"."dummy" = 1
	)
	, "find_bk_fk3" AS 
	( 
		SELECT 
			  "order_bk_fk3"."produ_featu_code_bk" AS "produ_featu_code_bk"
			, "order_bk_fk3"."product_feature_id" AS "product_feature_id"
		FROM "order_bk_fk3" "order_bk_fk3"
		WHERE  "order_bk_fk3"."dummy" = 1
	)
	SELECT 
		  UPPER(ENCODE(DIGEST(  'moto_sales_scn01' || '#' || COALESCE("find_bk_fk1"."product_cc_bk","mex_src"."key_attribute_numeric")
			||'#' ||  COALESCE("find_bk_fk1"."product_et_code_bk","mex_src"."key_attribute_character")|| '#' ||  COALESCE("find_bk_fk1"."product_part_code_bk","mex_src"."key_attribute_varchar")|| '#' || COALESCE("find_bk_fk2"."pro_fea_cla_code_bk","mex_src"."key_attribute_varchar")|| '#' || COALESCE("find_bk_fk3"."produ_featu_code_bk","mex_src"."key_attribute_varchar")|| '#'  ,'MD5'),'HEX')) AS "lnd_pro_fea_clas_rel_hkey"
		, UPPER(ENCODE(DIGEST( 'moto_sales_scn01' || '#' || COALESCE("find_bk_fk1"."product_cc_bk","mex_src"."key_attribute_numeric")
			||'#' ||  COALESCE("find_bk_fk1"."product_et_code_bk","mex_src"."key_attribute_character")|| '#' ||  COALESCE("find_bk_fk1"."product_part_code_bk","mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "products_hkey"
		, UPPER(ENCODE(DIGEST( COALESCE("find_bk_fk2"."pro_fea_cla_code_bk","mex_src"."key_attribute_varchar")|| '#' ,
			'MD5'),'HEX')) AS "prod_featu_class_hkey"
		, UPPER(ENCODE(DIGEST( COALESCE("find_bk_fk3"."produ_featu_code_bk","mex_src"."key_attribute_varchar")|| '#' ,
			'MD5'),'HEX')) AS "product_features_hkey"
		, "ext_src"."load_date" AS "load_date"
		, "ext_src"."load_cycle_id" AS "load_cycle_id"
		, "ext_src"."trans_timestamp" AS "trans_timestamp"
		, "ext_src"."operation" AS "operation"
		, "ext_src"."record_type" AS "record_type"
		, "ext_src"."product_feature_id" AS "product_feature_id"
		, "ext_src"."product_id" AS "product_id"
		, "ext_src"."product_feature_class_id" AS "product_feature_class_id"
		, NULL::text AS "product_cc_fk_productid_bk"
		, NULL::text AS "product_et_code_fk_productid_bk"
		, NULL::text AS "product_part_code_fk_productid_bk"
		, NULL::text AS "pro_fea_cla_code_fk_productfeatureclassid_bk"
		, NULL::text AS "produ_featu_code_fk_productfeatureid_bk"
		, "ext_src"."update_timestamp" AS "update_timestamp"
		, CASE WHEN  "find_bk_fk1"."product_id" IS NULL OR "find_bk_fk2"."product_feature_class_id" IS NULL OR 
			"find_bk_fk3"."product_feature_id" IS NULL  THEN 1 ELSE 0 END AS "error_code_pro_fea_clas_rel"
	FROM "moto_sales_scn01_ext"."product_feat_class_rel" "ext_src"
	INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
	LEFT OUTER JOIN "find_bk_fk1" "find_bk_fk1" ON  "ext_src"."product_id" = "find_bk_fk1"."product_id"
	LEFT OUTER JOIN "find_bk_fk2" "find_bk_fk2" ON  "ext_src"."product_feature_class_id" = "find_bk_fk2"."product_feature_class_id"
	LEFT OUTER JOIN "find_bk_fk3" "find_bk_fk3" ON  "ext_src"."product_feature_id" = "find_bk_fk3"."product_feature_id"
	;
END;


BEGIN -- ext_err_tgt

	TRUNCATE TABLE "moto_sales_scn01_ext"."product_feat_class_rel_err"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."product_feat_class_rel_err"(
		 "load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"product_feature_id"
		,"product_id"
		,"product_feature_class_id"
		,"update_timestamp"
		,"error_code_pro_fea_clas_rel"
	)
	SELECT 
		  CASE WHEN "stg_err_src"."error_code_pro_fea_clas_rel" = 1 THEN "stg_err_src"."load_date" + interval'1 microsecond'   ELSE "stg_err_src"."load_date" END AS "load_date"
		, "stg_err_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_err_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_err_src"."operation" AS "operation"
		, "stg_err_src"."record_type" AS "record_type"
		, "stg_err_src"."product_feature_id" AS "product_feature_id"
		, "stg_err_src"."product_id" AS "product_id"
		, "stg_err_src"."product_feature_class_id" AS "product_feature_class_id"
		, "stg_err_src"."update_timestamp" AS "update_timestamp"
		, "stg_err_src"."error_code_pro_fea_clas_rel" AS "error_code_pro_fea_clas_rel"
	FROM "moto_sales_scn01_stg"."product_feat_class_rel" "stg_err_src"
	WHERE ( "stg_err_src"."error_code_pro_fea_clas_rel" > 0)AND "stg_err_src"."load_cycle_id" >= 0
	;
END;



END;
$function$;
 
 
