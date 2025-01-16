CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_ms_productfeatures_incr"() 
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

BEGIN -- ext_tgt

	TRUNCATE TABLE "moto_sales_scn01_ext"."product_features"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."product_features"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"product_feature_id"
		,"product_feature_cat_id"
		,"produ_featu_code_bk"
		,"product_feature_code"
		,"pro_fea_lan_code_seq"
		,"product_feature_language_code"
		,"product_feature_description"
		,"update_timestamp"
		,"error_code_prfe_pfca"
	)
	WITH "calculate_bk" AS 
	( 
		SELECT 
			  "tdfv_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("tdfv_src"."operation","mex_src"."attribute_varchar") AS "operation"
			, "tdfv_src"."record_type" AS "record_type"
			, "tdfv_src"."product_feature_id" AS "product_feature_id"
			, "tdfv_src"."product_feature_cat_id" AS "product_feature_cat_id"
			, CASE WHEN TRIM("tdfv_src"."product_feature_code")= '' OR "tdfv_src"."product_feature_code" IS NULL THEN "mex_src"."key_attribute_varchar" ELSE UPPER(REPLACE(TRIM( "tdfv_src"."product_feature_code")
				,'#','\' || '#'))END AS "produ_featu_code_bk"
			, "tdfv_src"."product_feature_code" AS "product_feature_code"
			, COALESCE("tdfv_src"."product_feature_language_code", "mex_src"."key_attribute_varchar"::text) AS "pro_fea_lan_code_seq"
			, "tdfv_src"."product_feature_language_code" AS "product_feature_language_code"
			, "tdfv_src"."product_feature_description" AS "product_feature_description"
			, "tdfv_src"."update_timestamp" AS "update_timestamp"
			, 1 AS "error_code_prfe_pfca"
		FROM "moto_sales_scn01_dfv"."vw_product_features" "tdfv_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	, "ext_union" AS 
	( 
		SELECT 
			  "lci_src"."load_cycle_id" AS "load_cycle_id"
			, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "calculate_bk"."produ_featu_code_bk" ,"calculate_bk"."pro_fea_lan_code_seq"  ORDER BY  "calculate_bk"."trans_timestamp")
				* interval'2 microsecond'   AS "load_date"
			, "calculate_bk"."trans_timestamp" AS "trans_timestamp"
			, "calculate_bk"."operation" AS "operation"
			, "calculate_bk"."record_type" AS "record_type"
			, "calculate_bk"."product_feature_id" AS "product_feature_id"
			, "calculate_bk"."product_feature_cat_id" AS "product_feature_cat_id"
			, "calculate_bk"."produ_featu_code_bk" AS "produ_featu_code_bk"
			, "calculate_bk"."product_feature_code" AS "product_feature_code"
			, "calculate_bk"."pro_fea_lan_code_seq" AS "pro_fea_lan_code_seq"
			, "calculate_bk"."product_feature_language_code" AS "product_feature_language_code"
			, "calculate_bk"."product_feature_description" AS "product_feature_description"
			, "calculate_bk"."update_timestamp" AS "update_timestamp"
			, "calculate_bk"."error_code_prfe_pfca" AS "error_code_prfe_pfca"
		FROM "calculate_bk" "calculate_bk"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		UNION 
		SELECT 
			  "err_lci_src"."load_cycle_id" AS "load_cycle_id"
			, "ext_err_src"."load_date" AS "load_date"
			, "ext_err_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("ext_err_src"."operation",'U') AS "operation"
			, 'E' AS "record_type"
			, "ext_err_src"."product_feature_id" AS "product_feature_id"
			, "ext_err_src"."product_feature_cat_id" AS "product_feature_cat_id"
			, "ext_err_src"."produ_featu_code_bk" AS "produ_featu_code_bk"
			, "ext_err_src"."product_feature_code" AS "product_feature_code"
			, "ext_err_src"."pro_fea_lan_code_seq" AS "pro_fea_lan_code_seq"
			, "ext_err_src"."product_feature_language_code" AS "product_feature_language_code"
			, "ext_err_src"."product_feature_description" AS "product_feature_description"
			, "ext_err_src"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "ext_err_src"."error_code_prfe_pfca" = 0 THEN 2 WHEN "ext_err_src"."error_code_prfe_pfca" =
				1 THEN 3 ELSE"ext_err_src"."error_code_prfe_pfca" END AS "error_code_prfe_pfca"
		FROM "moto_sales_scn01_ext"."product_features_err" "ext_err_src"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "err_lci_src" ON  1 = 1
		WHERE  "ext_err_src"."error_code_prfe_pfca" < 4
	)
	SELECT 
		  "ext_union"."load_cycle_id" AS "load_cycle_id"
		, "ext_union"."load_date" AS "load_date"
		, "ext_union"."trans_timestamp" AS "trans_timestamp"
		, "ext_union"."operation" AS "operation"
		, "ext_union"."record_type" AS "record_type"
		, "ext_union"."product_feature_id" AS "product_feature_id"
		, "ext_union"."product_feature_cat_id" AS "product_feature_cat_id"
		, "ext_union"."produ_featu_code_bk" AS "produ_featu_code_bk"
		, "ext_union"."product_feature_code" AS "product_feature_code"
		, "ext_union"."pro_fea_lan_code_seq" AS "pro_fea_lan_code_seq"
		, "ext_union"."product_feature_language_code" AS "product_feature_language_code"
		, "ext_union"."product_feature_description" AS "product_feature_description"
		, "ext_union"."update_timestamp" AS "update_timestamp"
		, "ext_union"."error_code_prfe_pfca" AS "error_code_prfe_pfca"
	FROM "ext_union" "ext_union"
	;
END;



END;
$function$;
 
 
