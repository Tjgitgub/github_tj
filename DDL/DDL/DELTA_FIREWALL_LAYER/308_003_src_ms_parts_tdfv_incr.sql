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


DROP VIEW IF EXISTS "moto_sales_scn01_dfv"."vw_parts";
CREATE  VIEW "moto_sales_scn01_dfv"."vw_parts"  AS 
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
			, "cdc_src"."part_id" AS "part_id"
			, "cdc_src"."part_number" AS "ref_part_number_fk"
			, "cdc_src"."part_language_code" AS "ref_part_language_code_fk"
			, "cdc_src"."part_number" AS "part_number"
			, "cdc_src"."part_language_code" AS "part_language_code"
			, "cdc_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01"."jrn_moto_parts" "cdc_src"
		INNER JOIN "delta_window" "delta_window" ON  1 = 1
		WHERE  "cdc_src"."trans_timestamp" > "delta_window"."fmc_begin_lw_timestamp" AND "cdc_src"."trans_timestamp" <= "delta_window"."fmc_end_lw_timestamp"
	)
	, "delta_view" AS 
	( 
		SELECT 
			  "delta_view_filter"."trans_timestamp" AS "trans_timestamp"
			, "delta_view_filter"."operation" AS "operation"
			, "delta_view_filter"."record_type" AS "record_type"
			, "delta_view_filter"."part_id" AS "part_id"
			, "delta_view_filter"."ref_part_number_fk" AS "ref_part_number_fk"
			, "delta_view_filter"."ref_part_language_code_fk" AS "ref_part_language_code_fk"
			, "delta_view_filter"."part_number" AS "part_number"
			, "delta_view_filter"."part_language_code" AS "part_language_code"
			, "delta_view_filter"."update_timestamp" AS "update_timestamp"
		FROM "delta_view_filter" "delta_view_filter"
	)
	, "prepjoinbk" AS 
	( 
		SELECT 
			  "delta_view"."trans_timestamp" AS "trans_timestamp"
			, "delta_view"."operation" AS "operation"
			, "delta_view"."record_type" AS "record_type"
			, COALESCE("delta_view"."part_id", TO_NUMBER("mex_bk_src"."key_attribute_numeric", '999999999999D999999999'::varchar)
				) AS "part_id"
			, CASE WHEN "delta_view"."ref_part_number_fk" = '' THEN "mex_bk_src"."key_attribute_varchar" ELSE COALESCE("delta_view"."ref_part_number_fk",
				"mex_bk_src"."key_attribute_varchar")END AS "ref_part_number_fk"
			, CASE WHEN "delta_view"."ref_part_language_code_fk" = '' THEN "mex_bk_src"."key_attribute_varchar" ELSE COALESCE("delta_view"."ref_part_language_code_fk",
				"mex_bk_src"."key_attribute_varchar")END AS "ref_part_language_code_fk"
			, "delta_view"."part_number" AS "part_number"
			, "delta_view"."part_language_code" AS "part_language_code"
			, "delta_view"."update_timestamp" AS "update_timestamp"
		FROM "delta_view" "delta_view"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_bk_src" ON  1 = 1
		WHERE  "mex_bk_src"."record_type" = 'N'
	)
	SELECT 
		  "prepjoinbk"."trans_timestamp" AS "trans_timestamp"
		, "prepjoinbk"."operation" AS "operation"
		, "prepjoinbk"."record_type" AS "record_type"
		, "prepjoinbk"."part_id" AS "part_id"
		, "prepjoinbk"."ref_part_number_fk" AS "ref_part_number_fk"
		, "prepjoinbk"."ref_part_language_code_fk" AS "ref_part_language_code_fk"
		, "prepjoinbk"."part_number" AS "part_number"
		, "prepjoinbk"."part_language_code" AS "part_language_code"
		, "prepjoinbk"."update_timestamp" AS "update_timestamp"
	FROM "prepjoinbk" "prepjoinbk"
	;

 
 
