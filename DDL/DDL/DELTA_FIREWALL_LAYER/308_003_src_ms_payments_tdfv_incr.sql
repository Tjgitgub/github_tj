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


DROP VIEW IF EXISTS "moto_sales_scn01_dfv"."vw_payments";
CREATE  VIEW "moto_sales_scn01_dfv"."vw_payments"  AS 
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
			, "cdc_src"."invoice_number" AS "invoice_number"
			, "cdc_src"."customer_number" AS "customer_number"
			, "cdc_src"."transaction_id" AS "transaction_id"
			, "cdc_src"."date_time" AS "date_time"
			, "cdc_src"."amount" AS "amount"
			, "cdc_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01"."jrn_payments" "cdc_src"
		INNER JOIN "delta_window" "delta_window" ON  1 = 1
		WHERE  "cdc_src"."trans_timestamp" > "delta_window"."fmc_begin_lw_timestamp" AND "cdc_src"."trans_timestamp" <= "delta_window"."fmc_end_lw_timestamp"
	)
	, "delta_view" AS 
	( 
		SELECT 
			  "delta_view_filter"."trans_timestamp" AS "trans_timestamp"
			, "delta_view_filter"."operation" AS "operation"
			, "delta_view_filter"."record_type" AS "record_type"
			, "delta_view_filter"."invoice_number" AS "invoice_number"
			, "delta_view_filter"."customer_number" AS "customer_number"
			, "delta_view_filter"."transaction_id" AS "transaction_id"
			, "delta_view_filter"."date_time" AS "date_time"
			, "delta_view_filter"."amount" AS "amount"
			, "delta_view_filter"."update_timestamp" AS "update_timestamp"
		FROM "delta_view_filter" "delta_view_filter"
	)
	, "prepjoinbk" AS 
	( 
		SELECT 
			  "delta_view"."trans_timestamp" AS "trans_timestamp"
			, "delta_view"."operation" AS "operation"
			, "delta_view"."record_type" AS "record_type"
			, COALESCE("delta_view"."customer_number", TO_NUMBER("mex_bk_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "customer_number"
			, COALESCE("delta_view"."invoice_number", TO_NUMBER("mex_bk_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "invoice_number"
			, "delta_view"."transaction_id" AS "transaction_id"
			, "delta_view"."date_time" AS "date_time"
			, "delta_view"."amount" AS "amount"
			, "delta_view"."update_timestamp" AS "update_timestamp"
		FROM "delta_view" "delta_view"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_bk_src" ON  1 = 1
		WHERE  "mex_bk_src"."record_type" = 'N'
	)
	SELECT 
		  "prepjoinbk"."trans_timestamp" AS "trans_timestamp"
		, "prepjoinbk"."operation" AS "operation"
		, "prepjoinbk"."record_type" AS "record_type"
		, "prepjoinbk"."invoice_number" AS "invoice_number"
		, "prepjoinbk"."customer_number" AS "customer_number"
		, "prepjoinbk"."transaction_id" AS "transaction_id"
		, "prepjoinbk"."date_time" AS "date_time"
		, "prepjoinbk"."amount" AS "amount"
		, "prepjoinbk"."update_timestamp" AS "update_timestamp"
	FROM "prepjoinbk" "prepjoinbk"
	;

 
 
