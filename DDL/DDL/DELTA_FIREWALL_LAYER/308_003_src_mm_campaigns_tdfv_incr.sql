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


DROP VIEW IF EXISTS "moto_mktg_scn01_dfv"."vw_campaigns";
CREATE  VIEW "moto_mktg_scn01_dfv"."vw_campaigns"  AS 
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
			, "cdc_src"."campaign_code" AS "campaign_code"
			, "cdc_src"."campaign_start_date" AS "campaign_start_date"
			, "cdc_src"."campaign_name" AS "campaign_name"
			, "cdc_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_mktg_scn01"."jrn_campaigns" "cdc_src"
		INNER JOIN "delta_window" "delta_window" ON  1 = 1
		WHERE  "cdc_src"."trans_timestamp" > "delta_window"."fmc_begin_lw_timestamp" AND "cdc_src"."trans_timestamp" <= "delta_window"."fmc_end_lw_timestamp" AND("cdc_src"."operation" = 'I' or "cdc_src"."operation" = 'D')
	)
	, "delta_view" AS 
	( 
		SELECT 
			  "delta_view_filter"."trans_timestamp" AS "trans_timestamp"
			, "delta_view_filter"."operation" AS "operation"
			, "delta_view_filter"."record_type" AS "record_type"
			, "delta_view_filter"."campaign_code" AS "campaign_code"
			, "delta_view_filter"."campaign_start_date" AS "campaign_start_date"
			, "delta_view_filter"."campaign_name" AS "campaign_name"
			, "delta_view_filter"."update_timestamp" AS "update_timestamp"
		FROM "delta_view_filter" "delta_view_filter"
		UNION 
		SELECT 
			  "cdc_aft"."trans_timestamp" AS "trans_timestamp"
			, "cdc_aft"."operation" AS "operation"
			, 'S' ::text AS "record_type"
			, "cdc_aft"."campaign_code" AS "campaign_code"
			, "cdc_aft"."campaign_start_date" AS "campaign_start_date"
			, CASE WHEN("cdc_aft"."campaign_name" = "cdc_bef"."campaign_name")or("cdc_aft"."campaign_name" IS NULL AND 
				"cdc_bef"."campaign_name" IS NULL)THEN 'unchanged'::text else "cdc_aft"."campaign_name" end AS "campaign_name"
			, CASE WHEN("cdc_aft"."update_timestamp" = "cdc_bef"."update_timestamp")or("cdc_aft"."update_timestamp" IS NULL AND 
				"cdc_bef"."update_timestamp" IS NULL)THEN TO_TIMESTAMP('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS.US'::varchar) else "cdc_aft"."update_timestamp" end AS "update_timestamp"
		FROM "moto_mktg_scn01"."jrn_campaigns" "cdc_aft"
		INNER JOIN "moto_mktg_scn01"."jrn_campaigns" "cdc_bef" ON  "cdc_bef"."trans_id" = "cdc_aft"."trans_id"
		INNER JOIN "delta_window" "delta_window" ON  1 = 1
		WHERE  "cdc_bef"."image_type" = 'BEFORE' and "cdc_aft"."image_type" = 'AFTER' AND "cdc_aft"."operation" = 'U' AND "cdc_aft"."trans_timestamp" > "delta_window"."fmc_begin_lw_timestamp" AND "cdc_aft"."trans_timestamp" <= "delta_window"."fmc_end_lw_timestamp"
	)
	, "prepjoinbk" AS 
	( 
		SELECT 
			  "delta_view"."trans_timestamp" AS "trans_timestamp"
			, "delta_view"."operation" AS "operation"
			, "delta_view"."record_type" AS "record_type"
			, COALESCE("delta_view"."campaign_code","mex_bk_src"."key_attribute_varchar") AS "campaign_code"
			, COALESCE("delta_view"."campaign_start_date", TO_DATE("mex_bk_src"."key_attribute_date", 'DD/MM/YYYY'::varchar)
				) AS "campaign_start_date"
			, "delta_view"."campaign_name" AS "campaign_name"
			, "delta_view"."update_timestamp" AS "update_timestamp"
		FROM "delta_view" "delta_view"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_bk_src" ON  1 = 1
		WHERE  "mex_bk_src"."record_type" = 'N'
	)
	SELECT 
		  "prepjoinbk"."trans_timestamp" AS "trans_timestamp"
		, "prepjoinbk"."operation" AS "operation"
		, "prepjoinbk"."record_type" AS "record_type"
		, "prepjoinbk"."campaign_code" AS "campaign_code"
		, "prepjoinbk"."campaign_start_date" AS "campaign_start_date"
		, "prepjoinbk"."campaign_name" AS "campaign_name"
		, "prepjoinbk"."update_timestamp" AS "update_timestamp"
	FROM "prepjoinbk" "prepjoinbk"
	;

 
 
