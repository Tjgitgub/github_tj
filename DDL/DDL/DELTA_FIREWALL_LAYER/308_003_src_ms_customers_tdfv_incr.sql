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


DROP VIEW IF EXISTS "moto_sales_scn01_dfv"."vw_customers";
CREATE  VIEW "moto_sales_scn01_dfv"."vw_customers"  AS 
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
			, "cdc_src"."customer_number" AS "customer_number"
			, "cdc_src"."customer_invoice_address_id" AS "customer_invoice_address_id"
			, "cdc_src"."customer_ship_to_address_id" AS "customer_ship_to_address_id"
			, "cdc_src"."national_person_id" AS "national_person_id"
			, "cdc_src"."first_name" AS "first_name"
			, "cdc_src"."last_name" AS "last_name"
			, "cdc_src"."birthdate" AS "birthdate"
			, "cdc_src"."gender" AS "gender"
			, "cdc_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01"."jrn_customers" "cdc_src"
		INNER JOIN "delta_window" "delta_window" ON  1 = 1
		WHERE  "cdc_src"."trans_timestamp" > "delta_window"."fmc_begin_lw_timestamp" AND "cdc_src"."trans_timestamp" <= "delta_window"."fmc_end_lw_timestamp"
	)
	, "delta_view" AS 
	( 
		SELECT 
			  "delta_view_filter"."trans_timestamp" AS "trans_timestamp"
			, "delta_view_filter"."operation" AS "operation"
			, "delta_view_filter"."record_type" AS "record_type"
			, "delta_view_filter"."customer_number" AS "customer_number"
			, "delta_view_filter"."customer_invoice_address_id" AS "customer_invoice_address_id"
			, "delta_view_filter"."customer_ship_to_address_id" AS "customer_ship_to_address_id"
			, "delta_view_filter"."national_person_id" AS "national_person_id"
			, "delta_view_filter"."first_name" AS "first_name"
			, "delta_view_filter"."last_name" AS "last_name"
			, "delta_view_filter"."birthdate" AS "birthdate"
			, "delta_view_filter"."gender" AS "gender"
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
			, COALESCE("delta_view"."customer_ship_to_address_id", TO_NUMBER("mex_bk_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "customer_ship_to_address_id"
			, COALESCE("delta_view"."customer_invoice_address_id", TO_NUMBER("mex_bk_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "customer_invoice_address_id"
			, "delta_view"."national_person_id" AS "national_person_id"
			, "delta_view"."first_name" AS "first_name"
			, "delta_view"."last_name" AS "last_name"
			, "delta_view"."birthdate" AS "birthdate"
			, "delta_view"."gender" AS "gender"
			, "delta_view"."update_timestamp" AS "update_timestamp"
		FROM "delta_view" "delta_view"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_bk_src" ON  1 = 1
		WHERE  "mex_bk_src"."record_type" = 'N'
	)
	SELECT 
		  "prepjoinbk"."trans_timestamp" AS "trans_timestamp"
		, "prepjoinbk"."operation" AS "operation"
		, "prepjoinbk"."record_type" AS "record_type"
		, "prepjoinbk"."customer_number" AS "customer_number"
		, "prepjoinbk"."customer_invoice_address_id" AS "customer_invoice_address_id"
		, "prepjoinbk"."customer_ship_to_address_id" AS "customer_ship_to_address_id"
		, "prepjoinbk"."national_person_id" AS "national_person_id"
		, "prepjoinbk"."first_name" AS "first_name"
		, "prepjoinbk"."last_name" AS "last_name"
		, "prepjoinbk"."birthdate" AS "birthdate"
		, "prepjoinbk"."gender" AS "gender"
		, "prepjoinbk"."update_timestamp" AS "update_timestamp"
	FROM "prepjoinbk" "prepjoinbk"
	;

 
 
