/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.14, generation date: 2025/01/09 12:48:54
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/09 09:40:46, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51
 */


DROP VIEW IF EXISTS "moto_mktg_scn01_dfv"."vw_channels";
CREATE  VIEW "moto_mktg_scn01_dfv"."vw_channels"  AS 
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
			, "cdc_src"."channel_id" AS "channel_id"
			, "cdc_src"."channel_code" AS "channel_code"
			, "cdc_src"."channel_description" AS "channel_description"
			, "cdc_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_mktg_scn01"."jrn_channels" "cdc_src"
		INNER JOIN "delta_window" "delta_window" ON  1 = 1
		WHERE  "cdc_src"."trans_timestamp" > "delta_window"."fmc_begin_lw_timestamp" AND "cdc_src"."trans_timestamp" <= "delta_window"."fmc_end_lw_timestamp" AND("cdc_src"."operation" = 'I' or "cdc_src"."operation" = 'D')
	)
	, "delta_view" AS 
	( 
		SELECT 
			  "delta_view_filter"."trans_timestamp" AS "trans_timestamp"
			, "delta_view_filter"."operation" AS "operation"
			, "delta_view_filter"."record_type" AS "record_type"
			, "delta_view_filter"."channel_id" AS "channel_id"
			, "delta_view_filter"."channel_code" AS "channel_code"
			, "delta_view_filter"."channel_description" AS "channel_description"
			, "delta_view_filter"."update_timestamp" AS "update_timestamp"
		FROM "delta_view_filter" "delta_view_filter"
		UNION 
		SELECT 
			  "cdc_aft"."trans_timestamp" AS "trans_timestamp"
			, "cdc_aft"."operation" AS "operation"
			, 'S' ::text AS "record_type"
			, "cdc_aft"."channel_id" AS "channel_id"
			, CASE WHEN("cdc_aft"."channel_code" = "cdc_bef"."channel_code")or("cdc_aft"."channel_code" IS NULL AND 
				"cdc_bef"."channel_code" IS NULL)THEN 'unchanged'::text else "cdc_aft"."channel_code" end AS "channel_code"
			, CASE WHEN("cdc_aft"."channel_description" = "cdc_bef"."channel_description")or("cdc_aft"."channel_description" IS NULL AND 
				"cdc_bef"."channel_description" IS NULL)THEN 'unchanged'::text else "cdc_aft"."channel_description" end AS "channel_description"
			, CASE WHEN("cdc_aft"."update_timestamp" = "cdc_bef"."update_timestamp")or("cdc_aft"."update_timestamp" IS NULL AND 
				"cdc_bef"."update_timestamp" IS NULL)THEN TO_TIMESTAMP('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS.US'::varchar) else "cdc_aft"."update_timestamp" end AS "update_timestamp"
		FROM "moto_mktg_scn01"."jrn_channels" "cdc_aft"
		INNER JOIN "moto_mktg_scn01"."jrn_channels" "cdc_bef" ON  "cdc_bef"."trans_id" = "cdc_aft"."trans_id"
		INNER JOIN "delta_window" "delta_window" ON  1 = 1
		WHERE  "cdc_bef"."image_type" = 'BEFORE' and "cdc_aft"."image_type" = 'AFTER' AND "cdc_aft"."operation" = 'U' AND "cdc_aft"."trans_timestamp" > "delta_window"."fmc_begin_lw_timestamp" AND "cdc_aft"."trans_timestamp" <= "delta_window"."fmc_end_lw_timestamp"
	)
	, "prepjoinbk" AS 
	( 
		SELECT 
			  "delta_view"."trans_timestamp" AS "trans_timestamp"
			, "delta_view"."operation" AS "operation"
			, "delta_view"."record_type" AS "record_type"
			, COALESCE("delta_view"."channel_id", TO_NUMBER("mex_bk_src"."key_attribute_numeric", '999999999999D999999999'::varchar)
				) AS "channel_id"
			, "delta_view"."channel_code" AS "channel_code"
			, "delta_view"."channel_description" AS "channel_description"
			, "delta_view"."update_timestamp" AS "update_timestamp"
		FROM "delta_view" "delta_view"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_bk_src" ON  1 = 1
		WHERE  "mex_bk_src"."record_type" = 'N'
	)
	SELECT 
		  "prepjoinbk"."trans_timestamp" AS "trans_timestamp"
		, "prepjoinbk"."operation" AS "operation"
		, "prepjoinbk"."record_type" AS "record_type"
		, "prepjoinbk"."channel_id" AS "channel_id"
		, "prepjoinbk"."channel_code" AS "channel_code"
		, "prepjoinbk"."channel_description" AS "channel_description"
		, "prepjoinbk"."update_timestamp" AS "update_timestamp"
	FROM "prepjoinbk" "prepjoinbk"
	;

 
 
