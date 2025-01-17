CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_ms_products_incr"() 
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

	TRUNCATE TABLE "moto_sales_scn01_stg"."products"  CASCADE;

	INSERT INTO "moto_sales_scn01_stg"."products"(
		 "products_hkey"
		,"products_rpid_hkey"
		,"lnk_prod_prod_rpid_hkey"
		,"load_date"
		,"load_cycle_id"
		,"src_bk"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"product_id"
		,"replacement_product_id"
		,"product_cc_bk"
		,"product_et_code_bk"
		,"product_part_code_bk"
		,"product_cc_fk_replacementproductid_bk"
		,"product_et_code_fk_replacementproductid_bk"
		,"product_part_code_fk_replacementproductid_bk"
		,"product_intro_date"
		,"product_name"
		,"update_timestamp"
		,"error_code_prod_prod_rpid"
	)
	WITH "dist_fk1" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src1"."replacement_product_id" AS "replacement_product_id"
		FROM "moto_sales_scn01_ext"."products" "ext_dis_src1"
	)
	, "prep_find_bk_fk1" AS 
	( 
		SELECT 
			  "hub_src1"."product_cc_bk" AS "product_cc_bk"
			, "hub_src1"."product_et_code_bk" AS "product_et_code_bk"
			, "hub_src1"."product_part_code_bk" AS "product_part_code_bk"
			, "dist_fk1"."replacement_product_id" AS "replacement_product_id"
			, "sat_src1"."load_date" AS "load_date"
			, 1 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_scn01_fl"."sat_ms_products" "sat_src1" ON  "dist_fk1"."replacement_product_id" = "sat_src1"."product_id"
		INNER JOIN "moto_scn01_fl"."hub_products" "hub_src1" ON  "hub_src1"."products_hkey" = "sat_src1"."products_hkey"
		WHERE  "sat_src1"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
		UNION ALL 
		SELECT 
			  "ext_fkbk_src1"."product_cc_bk" AS "product_cc_bk"
			, "ext_fkbk_src1"."product_et_code_bk" AS "product_et_code_bk"
			, "ext_fkbk_src1"."product_part_code_bk" AS "product_part_code_bk"
			, "dist_fk1"."replacement_product_id" AS "replacement_product_id"
			, "ext_fkbk_src1"."load_date" AS "load_date"
			, 0 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_sales_scn01_ext"."products" "ext_fkbk_src1" ON  "dist_fk1"."replacement_product_id" = "ext_fkbk_src1"."product_id"
	)
	, "order_bk_fk1" AS 
	( 
		SELECT 
			  "prep_find_bk_fk1"."product_cc_bk" AS "product_cc_bk"
			, "prep_find_bk_fk1"."product_et_code_bk" AS "product_et_code_bk"
			, "prep_find_bk_fk1"."product_part_code_bk" AS "product_part_code_bk"
			, "prep_find_bk_fk1"."replacement_product_id" AS "replacement_product_id"
			, ROW_NUMBER()OVER(PARTITION BY "prep_find_bk_fk1"."replacement_product_id" ORDER BY "prep_find_bk_fk1"."general_order",
				"prep_find_bk_fk1"."load_date" DESC) AS "dummy"
		FROM "prep_find_bk_fk1" "prep_find_bk_fk1"
	)
	, "find_bk_fk1" AS 
	( 
		SELECT 
			  "order_bk_fk1"."product_cc_bk" AS "product_cc_bk"
			, "order_bk_fk1"."product_et_code_bk" AS "product_et_code_bk"
			, "order_bk_fk1"."product_part_code_bk" AS "product_part_code_bk"
			, "order_bk_fk1"."replacement_product_id" AS "replacement_product_id"
		FROM "order_bk_fk1" "order_bk_fk1"
		WHERE  "order_bk_fk1"."dummy" = 1
	)
	, "calc_hash_keys" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST( 'moto_sales_scn01' || '#' || "ext_src"."product_cc_bk" || '#' ||  "ext_src"."product_et_code_bk" || 
				'#' ||  "ext_src"."product_part_code_bk" || '#' ,'MD5'),'HEX')) AS "products_hkey"
			, UPPER(ENCODE(DIGEST( 'moto_sales_scn01' || '#' || COALESCE("find_bk_fk1"."product_cc_bk","mex_src"."key_attribute_numeric")
				||'#' ||  COALESCE("find_bk_fk1"."product_et_code_bk","mex_src"."key_attribute_character")|| '#' ||  COALESCE("find_bk_fk1"."product_part_code_bk","mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "products_rpid_hkey"
			, UPPER(ENCODE(DIGEST( 'moto_sales_scn01' || '#' || "ext_src"."product_cc_bk" || '#' ||  "ext_src"."product_et_code_bk" || 
				'#' ||  "ext_src"."product_part_code_bk" || '#' || 'moto_sales_scn01' || '#' || COALESCE("find_bk_fk1"."product_cc_bk","mex_src"."key_attribute_numeric")|| '#' ||  COALESCE("find_bk_fk1"."product_et_code_bk","mex_src"."key_attribute_character")|| '#' ||  COALESCE("find_bk_fk1"."product_part_code_bk","mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "lnk_prod_prod_rpid_hkey"
			, "ext_src"."load_date" AS "load_date"
			, "ext_src"."load_cycle_id" AS "load_cycle_id"
			, 'moto_sales_scn01' AS "src_bk"
			, "ext_src"."trans_timestamp" AS "trans_timestamp"
			, "ext_src"."operation" AS "operation"
			, "ext_src"."record_type" AS "record_type"
			, "ext_src"."product_id" AS "product_id"
			, "ext_src"."replacement_product_id" AS "replacement_product_id"
			, "ext_src"."product_cc_bk" AS "product_cc_bk"
			, "ext_src"."product_et_code_bk" AS "product_et_code_bk"
			, "ext_src"."product_part_code_bk" AS "product_part_code_bk"
			, NULL::text AS "product_cc_fk_replacementproductid_bk"
			, NULL::text AS "product_et_code_fk_replacementproductid_bk"
			, NULL::text AS "product_part_code_fk_replacementproductid_bk"
			, "ext_src"."product_intro_date" AS "product_intro_date"
			, "ext_src"."product_name" AS "product_name"
			, "ext_src"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "ext_src"."error_code_prod_prod_rpid" = 2 THEN 2 WHEN "ext_src"."error_code_prod_prod_rpid" =
				- 1 THEN 0 WHEN"find_bk_fk1"."replacement_product_id" IS NULL THEN "ext_src"."error_code_prod_prod_rpid" ELSE 0 END AS "error_code_prod_prod_rpid"
		FROM "moto_sales_scn01_ext"."products" "ext_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
		LEFT OUTER JOIN "find_bk_fk1" "find_bk_fk1" ON  "ext_src"."replacement_product_id" = "find_bk_fk1"."replacement_product_id"
	)
	SELECT 
		  "calc_hash_keys"."products_hkey" AS "products_hkey"
		, "calc_hash_keys"."products_rpid_hkey" AS "products_rpid_hkey"
		, "calc_hash_keys"."lnk_prod_prod_rpid_hkey" AS "lnk_prod_prod_rpid_hkey"
		, "calc_hash_keys"."load_date" AS "load_date"
		, "calc_hash_keys"."load_cycle_id" AS "load_cycle_id"
		, "calc_hash_keys"."src_bk" AS "src_bk"
		, "calc_hash_keys"."trans_timestamp" AS "trans_timestamp"
		, "calc_hash_keys"."operation" AS "operation"
		, "calc_hash_keys"."record_type" AS "record_type"
		, "calc_hash_keys"."product_id" AS "product_id"
		, "calc_hash_keys"."replacement_product_id" AS "replacement_product_id"
		, "calc_hash_keys"."product_cc_bk" AS "product_cc_bk"
		, "calc_hash_keys"."product_et_code_bk" AS "product_et_code_bk"
		, "calc_hash_keys"."product_part_code_bk" AS "product_part_code_bk"
		, "calc_hash_keys"."product_cc_fk_replacementproductid_bk" AS "product_cc_fk_replacementproductid_bk"
		, "calc_hash_keys"."product_et_code_fk_replacementproductid_bk" AS "product_et_code_fk_replacementproductid_bk"
		, "calc_hash_keys"."product_part_code_fk_replacementproductid_bk" AS "product_part_code_fk_replacementproductid_bk"
		, "calc_hash_keys"."product_intro_date" AS "product_intro_date"
		, "calc_hash_keys"."product_name" AS "product_name"
		, "calc_hash_keys"."update_timestamp" AS "update_timestamp"
		, "calc_hash_keys"."error_code_prod_prod_rpid" AS "error_code_prod_prod_rpid"
	FROM "calc_hash_keys" "calc_hash_keys"
	;
END;


BEGIN -- ext_err_tgt

	TRUNCATE TABLE "moto_sales_scn01_ext"."products_err"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."products_err"(
		 "load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"product_id"
		,"replacement_product_id"
		,"product_cc_bk"
		,"product_et_code_bk"
		,"product_part_code_bk"
		,"product_intro_date"
		,"product_name"
		,"update_timestamp"
		,"error_code_prod_prod_rpid"
	)
	SELECT 
		  CASE WHEN "stg_err_src"."error_code_prod_prod_rpid" = 1 THEN "stg_err_src"."load_date" + interval'1 microsecond'   ELSE "stg_err_src"."load_date" END AS "load_date"
		, "stg_err_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_err_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_err_src"."operation" AS "operation"
		, "stg_err_src"."record_type" AS "record_type"
		, "stg_err_src"."product_id" AS "product_id"
		, "stg_err_src"."replacement_product_id" AS "replacement_product_id"
		, "stg_err_src"."product_cc_bk" AS "product_cc_bk"
		, "stg_err_src"."product_et_code_bk" AS "product_et_code_bk"
		, "stg_err_src"."product_part_code_bk" AS "product_part_code_bk"
		, "stg_err_src"."product_intro_date" AS "product_intro_date"
		, "stg_err_src"."product_name" AS "product_name"
		, "stg_err_src"."update_timestamp" AS "update_timestamp"
		, "stg_err_src"."error_code_prod_prod_rpid" AS "error_code_prod_prod_rpid"
	FROM "moto_sales_scn01_stg"."products" "stg_err_src"
	WHERE ( "stg_err_src"."error_code_prod_prod_rpid" IN(1,3))AND "stg_err_src"."load_cycle_id" >= 0
	;
END;



END;
$function$;
 
 
