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


DROP VIEW IF EXISTS "moto_sales_scn01_dfv"."vw_products";
CREATE  VIEW "moto_sales_scn01_dfv"."vw_products"  AS 
	WITH "delta_window" AS 
	( 
		SELECT 
			  "lwt_src"."fmc_begin_lw_timestamp" AS "fmc_begin_lw_timestamp"
			, "lwt_src"."fmc_end_lw_timestamp" AS "fmc_end_lw_timestamp"
		FROM "moto_sales_scn01_mtd"."fmc_loading_window_table" "lwt_src"
	)
	, "delta_view_filter" AS 
	( 
		SELECT 
			  "cdc_src"."trans_timestamp" AS "trans_timestamp"
			, "cdc_src"."operation" AS "operation"
			, 'S' ::text AS "record_type"
			, "cdc_src"."product_id" AS "product_id"
			, "cdc_src"."replacement_product_id" AS "replacement_product_id"
			, "cdc_src"."product_cc" AS "product_cc"
			, "cdc_src"."product_et_code" AS "product_et_code"
			, "cdc_src"."product_part_code" AS "product_part_code"
			, "cdc_src"."product_intro_date" AS "product_intro_date"
			, "cdc_src"."product_name" AS "product_name"
			, "cdc_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01"."jrn_moto_products" "cdc_src"
		INNER JOIN "delta_window" "delta_window" ON  1 = 1
		WHERE  "cdc_src"."trans_timestamp" > "delta_window"."fmc_begin_lw_timestamp" AND "cdc_src"."trans_timestamp" <= "delta_window"."fmc_end_lw_timestamp"
	)
	, "delta_view" AS 
	( 
		SELECT 
			  "delta_view_filter"."trans_timestamp" AS "trans_timestamp"
			, "delta_view_filter"."operation" AS "operation"
			, "delta_view_filter"."record_type" AS "record_type"
			, "delta_view_filter"."product_id" AS "product_id"
			, "delta_view_filter"."replacement_product_id" AS "replacement_product_id"
			, "delta_view_filter"."product_cc" AS "product_cc"
			, "delta_view_filter"."product_et_code" AS "product_et_code"
			, "delta_view_filter"."product_part_code" AS "product_part_code"
			, "delta_view_filter"."product_intro_date" AS "product_intro_date"
			, "delta_view_filter"."product_name" AS "product_name"
			, "delta_view_filter"."update_timestamp" AS "update_timestamp"
		FROM "delta_view_filter" "delta_view_filter"
	)
	, "prepjoinbk" AS 
	( 
		SELECT 
			  "delta_view"."trans_timestamp" AS "trans_timestamp"
			, "delta_view"."operation" AS "operation"
			, "delta_view"."record_type" AS "record_type"
			, COALESCE("delta_view"."product_id", TO_NUMBER("mex_bk_src"."key_attribute_numeric", '999999999999D999999999'::varchar)
				) AS "product_id"
			, COALESCE("delta_view"."replacement_product_id", TO_NUMBER("mex_bk_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "replacement_product_id"
			, "delta_view"."product_cc" AS "product_cc"
			, "delta_view"."product_et_code" AS "product_et_code"
			, "delta_view"."product_part_code" AS "product_part_code"
			, "delta_view"."product_intro_date" AS "product_intro_date"
			, "delta_view"."product_name" AS "product_name"
			, "delta_view"."update_timestamp" AS "update_timestamp"
		FROM "delta_view" "delta_view"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_bk_src" ON  1 = 1
		WHERE  "mex_bk_src"."record_type" = 'N'
	)
	SELECT 
		  "prepjoinbk"."trans_timestamp" AS "trans_timestamp"
		, "prepjoinbk"."operation" AS "operation"
		, "prepjoinbk"."record_type" AS "record_type"
		, "prepjoinbk"."product_id" AS "product_id"
		, "prepjoinbk"."replacement_product_id" AS "replacement_product_id"
		, "prepjoinbk"."product_cc" AS "product_cc"
		, "prepjoinbk"."product_et_code" AS "product_et_code"
		, "prepjoinbk"."product_part_code" AS "product_part_code"
		, "prepjoinbk"."product_intro_date" AS "product_intro_date"
		, "prepjoinbk"."product_name" AS "product_name"
		, "prepjoinbk"."update_timestamp" AS "update_timestamp"
	FROM "prepjoinbk" "prepjoinbk"
	;

 
 
