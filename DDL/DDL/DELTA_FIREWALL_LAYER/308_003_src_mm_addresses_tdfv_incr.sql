/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.16, generation date: 2025/01/16 15:01:59
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/16 14:56:23, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46
 */


DROP VIEW IF EXISTS "moto_mktg_scn01_dfv"."vw_addresses";
CREATE  VIEW "moto_mktg_scn01_dfv"."vw_addresses"  AS 
	WITH "delta_window" AS 
	( 
		SELECT 
			  "lwt_src"."fmc_begin_lw_timestamp" AS "fmc_begin_lw_timestamp"
			, "lwt_src"."fmc_end_lw_timestamp" AS "fmc_end_lw_timestamp"
		FROM "moto_mktg_scn01_mtd"."fmc_loading_window_table" "lwt_src"
	)
	, "delta_view_filter" AS 
	( 
		SELECT 
			  "cdc_src"."trans_timestamp" AS "trans_timestamp"
			, "cdc_src"."operation" AS "operation"
			, 'S' ::text AS "record_type"
			, "cdc_src"."address_number" AS "address_number"
			, "cdc_src"."street_name" AS "street_name"
			, "cdc_src"."street_number" AS "street_number"
			, "cdc_src"."postal_code" AS "postal_code"
			, "cdc_src"."city" AS "city"
			, "cdc_src"."province" AS "province"
			, "cdc_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_mktg_scn01"."jrn_addresses" "cdc_src"
		INNER JOIN "delta_window" "delta_window" ON  1 = 1
		WHERE  "cdc_src"."trans_timestamp" > "delta_window"."fmc_begin_lw_timestamp" AND "cdc_src"."trans_timestamp" <= "delta_window"."fmc_end_lw_timestamp" AND("cdc_src"."operation" = 'I' or "cdc_src"."operation" = 'D')
	)
	, "delta_view" AS 
	( 
		SELECT 
			  "delta_view_filter"."trans_timestamp" AS "trans_timestamp"
			, "delta_view_filter"."operation" AS "operation"
			, "delta_view_filter"."record_type" AS "record_type"
			, "delta_view_filter"."address_number" AS "address_number"
			, "delta_view_filter"."street_name" AS "street_name"
			, "delta_view_filter"."street_number" AS "street_number"
			, "delta_view_filter"."postal_code" AS "postal_code"
			, "delta_view_filter"."city" AS "city"
			, "delta_view_filter"."province" AS "province"
			, "delta_view_filter"."update_timestamp" AS "update_timestamp"
		FROM "delta_view_filter" "delta_view_filter"
		UNION 
		SELECT 
			  "cdc_aft"."trans_timestamp" AS "trans_timestamp"
			, "cdc_aft"."operation" AS "operation"
			, 'S' ::text AS "record_type"
			, "cdc_aft"."address_number" AS "address_number"
			, CASE WHEN("cdc_aft"."street_name" = "cdc_bef"."street_name")or("cdc_aft"."street_name" IS NULL AND "cdc_bef"."street_name" IS NULL)
				THEN'unchanged'::text else "cdc_aft"."street_name" end AS "street_name"
			, CASE WHEN("cdc_aft"."street_number" = "cdc_bef"."street_number")or("cdc_aft"."street_number" IS NULL AND 
				"cdc_bef"."street_number" IS NULL)THEN TO_NUMBER('-1', '999999999999D999999999'::varchar) else "cdc_aft"."street_number" end AS "street_number"
			, CASE WHEN("cdc_aft"."postal_code" = "cdc_bef"."postal_code")or("cdc_aft"."postal_code" IS NULL AND "cdc_bef"."postal_code" IS NULL)
				THEN'unchanged'::text else "cdc_aft"."postal_code" end AS "postal_code"
			, CASE WHEN("cdc_aft"."city" = "cdc_bef"."city")or("cdc_aft"."city" IS NULL AND "cdc_bef"."city" IS NULL)THEN 'unchanged'::text else "cdc_aft"."city" end AS "city"
			, CASE WHEN("cdc_aft"."province" = "cdc_bef"."province")or("cdc_aft"."province" IS NULL AND "cdc_bef"."province" IS NULL)
				THEN'unchanged'::text else "cdc_aft"."province" end AS "province"
			, CASE WHEN("cdc_aft"."update_timestamp" = "cdc_bef"."update_timestamp")or("cdc_aft"."update_timestamp" IS NULL AND 
				"cdc_bef"."update_timestamp" IS NULL)THEN TO_TIMESTAMP('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS.US'::varchar) else "cdc_aft"."update_timestamp" end AS "update_timestamp"
		FROM "moto_mktg_scn01"."jrn_addresses" "cdc_aft"
		INNER JOIN "moto_mktg_scn01"."jrn_addresses" "cdc_bef" ON  "cdc_bef"."trans_id" = "cdc_aft"."trans_id"
		INNER JOIN "delta_window" "delta_window" ON  1 = 1
		WHERE  "cdc_bef"."image_type" = 'BEFORE' and "cdc_aft"."image_type" = 'AFTER' AND "cdc_aft"."operation" = 'U' AND "cdc_aft"."trans_timestamp" > "delta_window"."fmc_begin_lw_timestamp" AND "cdc_aft"."trans_timestamp" <= "delta_window"."fmc_end_lw_timestamp"
	)
	, "prepjoinbk" AS 
	( 
		SELECT 
			  "delta_view"."trans_timestamp" AS "trans_timestamp"
			, "delta_view"."operation" AS "operation"
			, "delta_view"."record_type" AS "record_type"
			, COALESCE("delta_view"."address_number", TO_NUMBER("mex_bk_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "address_number"
			, "delta_view"."street_name" AS "street_name"
			, "delta_view"."street_number" AS "street_number"
			, "delta_view"."postal_code" AS "postal_code"
			, "delta_view"."city" AS "city"
			, "delta_view"."province" AS "province"
			, "delta_view"."update_timestamp" AS "update_timestamp"
		FROM "delta_view" "delta_view"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_bk_src" ON  1 = 1
		WHERE  "mex_bk_src"."record_type" = 'N'
	)
	SELECT 
		  "prepjoinbk"."trans_timestamp" AS "trans_timestamp"
		, "prepjoinbk"."operation" AS "operation"
		, "prepjoinbk"."record_type" AS "record_type"
		, "prepjoinbk"."address_number" AS "address_number"
		, "prepjoinbk"."street_name" AS "street_name"
		, "prepjoinbk"."street_number" AS "street_number"
		, "prepjoinbk"."postal_code" AS "postal_code"
		, "prepjoinbk"."city" AS "city"
		, "prepjoinbk"."province" AS "province"
		, "prepjoinbk"."update_timestamp" AS "update_timestamp"
	FROM "prepjoinbk" "prepjoinbk"
	;

 
 
