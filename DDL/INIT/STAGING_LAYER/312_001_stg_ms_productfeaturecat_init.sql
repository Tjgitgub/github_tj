CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_ms_productfeaturecat_init"() 
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

Vaultspeed version: 5.7.2.16, generation date: 2025/01/17 07:04:17
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/16 14:56:23, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08
 */


BEGIN 

BEGIN -- stg_tgt

	TRUNCATE TABLE "moto_sales_scn01_stg"."product_feature_cat"  CASCADE;

	INSERT INTO "moto_sales_scn01_stg"."product_feature_cat"(
		 "produ_featur_cat_hkey"
		,"load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"product_feature_category_id"
		,"product_feature_category_code"
		,"pro_fea_cat_code_bk"
		,"pr_fe_ca_lan_cod_seq"
		,"prod_feat_cat_language_code"
		,"prod_feat_cat_description"
		,"update_timestamp"
	)
	SELECT 
		  UPPER(ENCODE(DIGEST( "ext_src"."pro_fea_cat_code_bk" || '#' ,'MD5'),'HEX')) AS "produ_featur_cat_hkey"
		, "ext_src"."load_date" AS "load_date"
		, "ext_src"."load_cycle_id" AS "load_cycle_id"
		, "ext_src"."trans_timestamp" AS "trans_timestamp"
		, "ext_src"."operation" AS "operation"
		, "ext_src"."record_type" AS "record_type"
		, "ext_src"."product_feature_category_id" AS "product_feature_category_id"
		, "ext_src"."product_feature_category_code" AS "product_feature_category_code"
		, "ext_src"."pro_fea_cat_code_bk" AS "pro_fea_cat_code_bk"
		, "ext_src"."pr_fe_ca_lan_cod_seq" AS "pr_fe_ca_lan_cod_seq"
		, "ext_src"."prod_feat_cat_language_code" AS "prod_feat_cat_language_code"
		, "ext_src"."prod_feat_cat_description" AS "prod_feat_cat_description"
		, "ext_src"."update_timestamp" AS "update_timestamp"
	FROM "moto_sales_scn01_ext"."product_feature_cat" "ext_src"
	INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
	;
END;



END;
$function$;
 
 
