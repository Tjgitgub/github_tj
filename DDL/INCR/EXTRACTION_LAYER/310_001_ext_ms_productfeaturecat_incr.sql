CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_ms_productfeaturecat_incr"() 
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

BEGIN -- ext_tgt

	TRUNCATE TABLE "moto_sales_scn01_ext"."product_feature_cat"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."product_feature_cat"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"product_feature_category_id"
		,"pro_fea_cat_code_bk"
		,"product_feature_category_code"
		,"pr_fe_ca_lan_cod_seq"
		,"prod_feat_cat_language_code"
		,"prod_feat_cat_description"
		,"update_timestamp"
	)
	WITH "calculate_bk" AS 
	( 
		SELECT 
			  "tdfv_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("tdfv_src"."operation","mex_src"."attribute_varchar") AS "operation"
			, "tdfv_src"."record_type" AS "record_type"
			, "tdfv_src"."product_feature_category_id" AS "product_feature_category_id"
			, CASE WHEN TRIM("tdfv_src"."product_feature_category_code")= '' OR "tdfv_src"."product_feature_category_code" IS NULL THEN "mex_src"."key_attribute_varchar" ELSE UPPER(REPLACE(TRIM( "tdfv_src"."product_feature_category_code")
				,'#','\' || '#'))END AS "pro_fea_cat_code_bk"
			, "tdfv_src"."product_feature_category_code" AS "product_feature_category_code"
			, COALESCE("tdfv_src"."prod_feat_cat_language_code", "mex_src"."key_attribute_varchar"::text) AS "pr_fe_ca_lan_cod_seq"
			, "tdfv_src"."prod_feat_cat_language_code" AS "prod_feat_cat_language_code"
			, "tdfv_src"."prod_feat_cat_description" AS "prod_feat_cat_description"
			, "tdfv_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01_dfv"."vw_product_feature_cat" "tdfv_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	, "ext_union" AS 
	( 
		SELECT 
			  "lci_src"."load_cycle_id" AS "load_cycle_id"
			, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "calculate_bk"."pro_fea_cat_code_bk" ,"calculate_bk"."pr_fe_ca_lan_cod_seq"  ORDER BY  "calculate_bk"."trans_timestamp")
				* interval'2 microsecond'   AS "load_date"
			, "calculate_bk"."trans_timestamp" AS "trans_timestamp"
			, "calculate_bk"."operation" AS "operation"
			, "calculate_bk"."record_type" AS "record_type"
			, "calculate_bk"."product_feature_category_id" AS "product_feature_category_id"
			, "calculate_bk"."pro_fea_cat_code_bk" AS "pro_fea_cat_code_bk"
			, "calculate_bk"."product_feature_category_code" AS "product_feature_category_code"
			, "calculate_bk"."pr_fe_ca_lan_cod_seq" AS "pr_fe_ca_lan_cod_seq"
			, "calculate_bk"."prod_feat_cat_language_code" AS "prod_feat_cat_language_code"
			, "calculate_bk"."prod_feat_cat_description" AS "prod_feat_cat_description"
			, "calculate_bk"."update_timestamp" AS "update_timestamp"
		FROM "calculate_bk" "calculate_bk"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
	)
	SELECT 
		  "ext_union"."load_cycle_id" AS "load_cycle_id"
		, "ext_union"."load_date" AS "load_date"
		, "ext_union"."trans_timestamp" AS "trans_timestamp"
		, "ext_union"."operation" AS "operation"
		, "ext_union"."record_type" AS "record_type"
		, "ext_union"."product_feature_category_id" AS "product_feature_category_id"
		, "ext_union"."pro_fea_cat_code_bk" AS "pro_fea_cat_code_bk"
		, "ext_union"."product_feature_category_code" AS "product_feature_category_code"
		, "ext_union"."pr_fe_ca_lan_cod_seq" AS "pr_fe_ca_lan_cod_seq"
		, "ext_union"."prod_feat_cat_language_code" AS "prod_feat_cat_language_code"
		, "ext_union"."prod_feat_cat_description" AS "prod_feat_cat_description"
		, "ext_union"."update_timestamp" AS "update_timestamp"
	FROM "ext_union" "ext_union"
	;
END;



END;
$function$;
 
 
