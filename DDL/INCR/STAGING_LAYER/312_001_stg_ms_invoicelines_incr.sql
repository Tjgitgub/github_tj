CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_ms_invoicelines_incr"() 
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

	TRUNCATE TABLE "moto_sales_scn01_stg"."invoice_lines"  CASCADE;

	INSERT INTO "moto_sales_scn01_stg"."invoice_lines"(
		 "invoice_lines_hkey"
		,"products_hkey"
		,"invoices_hkey"
		,"parts_hkey"
		,"lnk_inli_prod_hkey"
		,"lnk_inli_invo_hkey"
		,"lnk_inli_part_hkey"
		,"load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"invoice_line_number"
		,"invoice_number"
		,"product_id"
		,"part_id"
		,"invoice_number_bk"
		,"invoice_line_number_bk"
		,"product_cc_fk_productid_bk"
		,"product_et_code_fk_productid_bk"
		,"product_part_code_fk_productid_bk"
		,"invoice_number_fk_invoicenumber_bk"
		,"part_number_fk_partid_bk"
		,"part_langua_code_fk_partid_bk"
		,"amount"
		,"quantity"
		,"unit_price"
		,"update_timestamp"
		,"error_code_inli_prod"
		,"error_code_inli_part"
	)
	WITH "dist_fk1" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src1"."product_id" AS "product_id"
		FROM "moto_sales_scn01_ext"."invoice_lines" "ext_dis_src1"
	)
	, "dist_fk3" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src3"."part_id" AS "part_id"
		FROM "moto_sales_scn01_ext"."invoice_lines" "ext_dis_src3"
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
	, "prep_find_bk_fk3" AS 
	( 
		SELECT 
			  "hub_src3"."part_number_bk" AS "part_number_bk"
			, "hub_src3"."part_langua_code_bk" AS "part_langua_code_bk"
			, "dist_fk3"."part_id" AS "part_id"
			, "sat_src3"."load_date" AS "load_date"
			, 1 AS "general_order"
		FROM "dist_fk3" "dist_fk3"
		INNER JOIN "moto_scn01_fl"."sat_ms_parts" "sat_src3" ON  "dist_fk3"."part_id" = "sat_src3"."part_id"
		INNER JOIN "moto_scn01_fl"."hub_parts" "hub_src3" ON  "hub_src3"."parts_hkey" = "sat_src3"."parts_hkey"
		WHERE  "sat_src3"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
		UNION ALL 
		SELECT 
			  "ext_fkbk_src3"."part_number_bk" AS "part_number_bk"
			, "ext_fkbk_src3"."part_langua_code_bk" AS "part_langua_code_bk"
			, "dist_fk3"."part_id" AS "part_id"
			, "ext_fkbk_src3"."load_date" AS "load_date"
			, 0 AS "general_order"
		FROM "dist_fk3" "dist_fk3"
		INNER JOIN "moto_sales_scn01_ext"."parts" "ext_fkbk_src3" ON  "dist_fk3"."part_id" = "ext_fkbk_src3"."part_id"
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
	, "order_bk_fk3" AS 
	( 
		SELECT 
			  "prep_find_bk_fk3"."part_number_bk" AS "part_number_bk"
			, "prep_find_bk_fk3"."part_langua_code_bk" AS "part_langua_code_bk"
			, "prep_find_bk_fk3"."part_id" AS "part_id"
			, ROW_NUMBER()OVER(PARTITION BY "prep_find_bk_fk3"."part_id" ORDER BY "prep_find_bk_fk3"."general_order",
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
	, "find_bk_fk3" AS 
	( 
		SELECT 
			  "order_bk_fk3"."part_number_bk" AS "part_number_bk"
			, "order_bk_fk3"."part_langua_code_bk" AS "part_langua_code_bk"
			, "order_bk_fk3"."part_id" AS "part_id"
		FROM "order_bk_fk3" "order_bk_fk3"
		WHERE  "order_bk_fk3"."dummy" = 1
	)
	, "calc_hash_keys" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST( "ext_src"."invoice_number_bk" || '#' ||  "ext_src"."invoice_line_number_bk" || '#' ,
				'MD5'),'HEX')) AS "invoice_lines_hkey"
			, UPPER(ENCODE(DIGEST( 'moto_sales_scn01' || '#' || COALESCE("find_bk_fk1"."product_cc_bk","mex_src"."key_attribute_numeric")
				||'#' ||  COALESCE("find_bk_fk1"."product_et_code_bk","mex_src"."key_attribute_character")|| '#' ||  COALESCE("find_bk_fk1"."product_part_code_bk","mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "products_hkey"
			, UPPER(ENCODE(DIGEST( "ext_src"."invoice_number_fk_invoicenumber_bk" || '#' ,'MD5'),'HEX')) AS "invoices_hkey"
			, UPPER(ENCODE(DIGEST( COALESCE("find_bk_fk3"."part_number_bk","mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk3"."part_langua_code_bk",
				"mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "parts_hkey"
			, UPPER(ENCODE(DIGEST( "ext_src"."invoice_number_bk" || '#' ||  "ext_src"."invoice_line_number_bk" || '#' || 
				'moto_sales_scn01' || '#' || COALESCE("find_bk_fk1"."product_cc_bk","mex_src"."key_attribute_numeric")|| '#' ||  COALESCE("find_bk_fk1"."product_et_code_bk","mex_src"."key_attribute_character")|| '#' ||  COALESCE("find_bk_fk1"."product_part_code_bk","mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "lnk_inli_prod_hkey"
			, UPPER(ENCODE(DIGEST( "ext_src"."invoice_number_bk" || '#' ||  "ext_src"."invoice_line_number_bk" || '#' || 
				"ext_src"."invoice_number_fk_invoicenumber_bk" || '#' ,'MD5'),'HEX')) AS "lnk_inli_invo_hkey"
			, UPPER(ENCODE(DIGEST( "ext_src"."invoice_number_bk" || '#' ||  "ext_src"."invoice_line_number_bk" || '#' || COALESCE("find_bk_fk3"."part_number_bk",
				"mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk3"."part_langua_code_bk","mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "lnk_inli_part_hkey"
			, "ext_src"."load_date" AS "load_date"
			, "ext_src"."load_cycle_id" AS "load_cycle_id"
			, "ext_src"."trans_timestamp" AS "trans_timestamp"
			, "ext_src"."operation" AS "operation"
			, "ext_src"."record_type" AS "record_type"
			, "ext_src"."invoice_line_number" AS "invoice_line_number"
			, "ext_src"."invoice_number" AS "invoice_number"
			, "ext_src"."product_id" AS "product_id"
			, "ext_src"."part_id" AS "part_id"
			, "ext_src"."invoice_number_bk" AS "invoice_number_bk"
			, "ext_src"."invoice_line_number_bk" AS "invoice_line_number_bk"
			, NULL::text AS "product_cc_fk_productid_bk"
			, NULL::text AS "product_et_code_fk_productid_bk"
			, NULL::text AS "product_part_code_fk_productid_bk"
			, "ext_src"."invoice_number_fk_invoicenumber_bk" AS "invoice_number_fk_invoicenumber_bk"
			, NULL::text AS "part_number_fk_partid_bk"
			, NULL::text AS "part_langua_code_fk_partid_bk"
			, "ext_src"."amount" AS "amount"
			, "ext_src"."quantity" AS "quantity"
			, "ext_src"."unit_price" AS "unit_price"
			, "ext_src"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "ext_src"."error_code_inli_prod" = 2 THEN 2 WHEN "ext_src"."error_code_inli_prod" =
				- 1 THEN 0 WHEN"find_bk_fk1"."product_id" IS NULL THEN "ext_src"."error_code_inli_prod" ELSE 0 END AS "error_code_inli_prod"
			, CASE WHEN "ext_src"."error_code_inli_part" = 2 THEN 2 WHEN "ext_src"."error_code_inli_part" =
				- 1 THEN 0 WHEN"find_bk_fk3"."part_id" IS NULL THEN "ext_src"."error_code_inli_part" ELSE 0 END AS "error_code_inli_part"
		FROM "moto_sales_scn01_ext"."invoice_lines" "ext_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
		LEFT OUTER JOIN "find_bk_fk1" "find_bk_fk1" ON  "ext_src"."product_id" = "find_bk_fk1"."product_id"
		LEFT OUTER JOIN "find_bk_fk3" "find_bk_fk3" ON  "ext_src"."part_id" = "find_bk_fk3"."part_id"
	)
	SELECT 
		  "calc_hash_keys"."invoice_lines_hkey" AS "invoice_lines_hkey"
		, "calc_hash_keys"."products_hkey" AS "products_hkey"
		, "calc_hash_keys"."invoices_hkey" AS "invoices_hkey"
		, "calc_hash_keys"."parts_hkey" AS "parts_hkey"
		, "calc_hash_keys"."lnk_inli_prod_hkey" AS "lnk_inli_prod_hkey"
		, "calc_hash_keys"."lnk_inli_invo_hkey" AS "lnk_inli_invo_hkey"
		, "calc_hash_keys"."lnk_inli_part_hkey" AS "lnk_inli_part_hkey"
		, "calc_hash_keys"."load_date" AS "load_date"
		, "calc_hash_keys"."load_cycle_id" AS "load_cycle_id"
		, "calc_hash_keys"."trans_timestamp" AS "trans_timestamp"
		, "calc_hash_keys"."operation" AS "operation"
		, "calc_hash_keys"."record_type" AS "record_type"
		, "calc_hash_keys"."invoice_line_number" AS "invoice_line_number"
		, "calc_hash_keys"."invoice_number" AS "invoice_number"
		, "calc_hash_keys"."product_id" AS "product_id"
		, "calc_hash_keys"."part_id" AS "part_id"
		, "calc_hash_keys"."invoice_number_bk" AS "invoice_number_bk"
		, "calc_hash_keys"."invoice_line_number_bk" AS "invoice_line_number_bk"
		, "calc_hash_keys"."product_cc_fk_productid_bk" AS "product_cc_fk_productid_bk"
		, "calc_hash_keys"."product_et_code_fk_productid_bk" AS "product_et_code_fk_productid_bk"
		, "calc_hash_keys"."product_part_code_fk_productid_bk" AS "product_part_code_fk_productid_bk"
		, "calc_hash_keys"."invoice_number_fk_invoicenumber_bk" AS "invoice_number_fk_invoicenumber_bk"
		, "calc_hash_keys"."part_number_fk_partid_bk" AS "part_number_fk_partid_bk"
		, "calc_hash_keys"."part_langua_code_fk_partid_bk" AS "part_langua_code_fk_partid_bk"
		, "calc_hash_keys"."amount" AS "amount"
		, "calc_hash_keys"."quantity" AS "quantity"
		, "calc_hash_keys"."unit_price" AS "unit_price"
		, "calc_hash_keys"."update_timestamp" AS "update_timestamp"
		, "calc_hash_keys"."error_code_inli_prod" AS "error_code_inli_prod"
		, "calc_hash_keys"."error_code_inli_part" AS "error_code_inli_part"
	FROM "calc_hash_keys" "calc_hash_keys"
	;
END;


BEGIN -- ext_err_tgt

	TRUNCATE TABLE "moto_sales_scn01_ext"."invoice_lines_err"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."invoice_lines_err"(
		 "load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"invoice_line_number"
		,"invoice_number"
		,"product_id"
		,"part_id"
		,"invoice_number_bk"
		,"invoice_line_number_bk"
		,"invoice_number_fk_invoicenumber_bk"
		,"amount"
		,"quantity"
		,"unit_price"
		,"update_timestamp"
		,"error_code_inli_prod"
		,"error_code_inli_part"
	)
	SELECT 
		  CASE WHEN "stg_err_src"."error_code_inli_prod" = 1 OR  "stg_err_src"."error_code_inli_part" = 1 THEN "stg_err_src"."load_date" +
			interval'1 microsecond'   ELSE "stg_err_src"."load_date" END AS "load_date"
		, "stg_err_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_err_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_err_src"."operation" AS "operation"
		, "stg_err_src"."record_type" AS "record_type"
		, "stg_err_src"."invoice_line_number" AS "invoice_line_number"
		, "stg_err_src"."invoice_number" AS "invoice_number"
		, "stg_err_src"."product_id" AS "product_id"
		, "stg_err_src"."part_id" AS "part_id"
		, "stg_err_src"."invoice_number_bk" AS "invoice_number_bk"
		, "stg_err_src"."invoice_line_number_bk" AS "invoice_line_number_bk"
		, "stg_err_src"."invoice_number_fk_invoicenumber_bk" AS "invoice_number_fk_invoicenumber_bk"
		, "stg_err_src"."amount" AS "amount"
		, "stg_err_src"."quantity" AS "quantity"
		, "stg_err_src"."unit_price" AS "unit_price"
		, "stg_err_src"."update_timestamp" AS "update_timestamp"
		, "stg_err_src"."error_code_inli_prod" AS "error_code_inli_prod"
		, "stg_err_src"."error_code_inli_part" AS "error_code_inli_part"
	FROM "moto_sales_scn01_stg"."invoice_lines" "stg_err_src"
	WHERE ( "stg_err_src"."error_code_inli_prod" IN(1,3) OR  "stg_err_src"."error_code_inli_part" IN(1,3))AND "stg_err_src"."load_cycle_id" >= 0
	;
END;



END;
$function$;
 
 
