CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_ms_productfeaturecat_init"() 
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
	WITH "load_init_data" AS 
	( 
		SELECT 
			  'I' ::text AS "operation"
			, 'S'::text AS "record_type"
			, COALESCE("ini_src"."product_feature_category_id", CAST("mex_inr_src"."key_attribute_integer" AS INTEGER)) AS "product_feature_category_id"
			, "ini_src"."product_feature_category_code" AS "product_feature_category_code"
			, CASE WHEN TRIM("ini_src"."prod_feat_cat_language_code")= '' THEN "mex_inr_src"."key_attribute_varchar"::text ELSE COALESCE("ini_src"."prod_feat_cat_language_code",
				 "mex_inr_src"."key_attribute_varchar"::text)END AS "pr_fe_ca_lan_cod_seq"
			, "ini_src"."prod_feat_cat_language_code" AS "prod_feat_cat_language_code"
			, "ini_src"."prod_feat_cat_description" AS "prod_feat_cat_description"
			, "ini_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01"."product_feature_cat" "ini_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_inr_src" ON  "mex_inr_src"."record_type" = 'N'
	)
	, "prep_excep" AS 
	( 
		SELECT 
			  "load_init_data"."operation" AS "operation"
			, "load_init_data"."record_type" AS "record_type"
			, NULL ::int AS "load_cycle_id"
			, "load_init_data"."product_feature_category_id" AS "product_feature_category_id"
			, "load_init_data"."product_feature_category_code" AS "product_feature_category_code"
			, "load_init_data"."pr_fe_ca_lan_cod_seq" AS "pr_fe_ca_lan_cod_seq"
			, "load_init_data"."prod_feat_cat_language_code" AS "prod_feat_cat_language_code"
			, "load_init_data"."prod_feat_cat_description" AS "prod_feat_cat_description"
			, "load_init_data"."update_timestamp" AS "update_timestamp"
		FROM "load_init_data" "load_init_data"
		UNION ALL 
		SELECT 
			  'I' ::text AS "operation"
			, "mex_ext_src"."record_type" AS "record_type"
			, "mex_ext_src"."load_cycle_id" ::int AS "load_cycle_id"
			, CAST("mex_ext_src"."key_attribute_integer" AS INTEGER) AS "product_feature_category_id"
			, "mex_ext_src"."key_attribute_varchar"::text AS "product_feature_category_code"
			, "mex_ext_src"."key_attribute_varchar"::text AS "pr_fe_ca_lan_cod_seq"
			, "mex_ext_src"."key_attribute_varchar"::text AS "prod_feat_cat_language_code"
			, "mex_ext_src"."attribute_varchar"::text AS "prod_feat_cat_description"
			, TO_TIMESTAMP("mex_ext_src"."attribute_timestamp", 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "update_timestamp"
		FROM "moto_sales_scn01_mtd"."mtd_exception_records" "mex_ext_src"
	)
	, "calculate_bk" AS 
	( 
		SELECT 
			  COALESCE("prep_excep"."load_cycle_id","lci_src"."load_cycle_id") AS "load_cycle_id"
			, "lci_src"."load_date" AS "load_date"
			, "lci_src"."load_date" AS "trans_timestamp"
			, "prep_excep"."operation" AS "operation"
			, "prep_excep"."record_type" AS "record_type"
			, "prep_excep"."product_feature_category_id" AS "product_feature_category_id"
			, CASE WHEN TRIM("prep_excep"."product_feature_category_code")= '' OR "prep_excep"."product_feature_category_code" IS NULL THEN "mex_src"."key_attribute_varchar" ELSE UPPER(REPLACE(TRIM("prep_excep"."product_feature_category_code")
				,'#','\' || '#'))END AS "pro_fea_cat_code_bk"
			, "prep_excep"."product_feature_category_code" AS "product_feature_category_code"
			, "prep_excep"."pr_fe_ca_lan_cod_seq" AS "pr_fe_ca_lan_cod_seq"
			, "prep_excep"."prod_feat_cat_language_code" AS "prod_feat_cat_language_code"
			, "prep_excep"."prod_feat_cat_description" AS "prod_feat_cat_description"
			, "prep_excep"."update_timestamp" AS "update_timestamp"
		FROM "prep_excep" "prep_excep"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	SELECT 
		  "calculate_bk"."load_cycle_id" AS "load_cycle_id"
		, "calculate_bk"."load_date" AS "load_date"
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
	;
END;



END;
$function$;
 
 
