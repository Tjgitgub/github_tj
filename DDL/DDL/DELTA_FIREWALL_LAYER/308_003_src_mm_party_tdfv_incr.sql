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


DROP VIEW IF EXISTS "moto_mktg_scn01_dfv"."vw_party";
CREATE  VIEW "moto_mktg_scn01_dfv"."vw_party"  AS 
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
			, "cdc_src"."party_number" AS "party_number"
			, "cdc_src"."parent_party_number" AS "parent_party_number"
			, "cdc_src"."address_number" AS "address_number"
			, "cdc_src"."name" AS "name"
			, "cdc_src"."birthdate" AS "birthdate"
			, "cdc_src"."gender" AS "gender"
			, "cdc_src"."party_type_code" AS "party_type_code"
			, "cdc_src"."comments" AS "comments"
			, "cdc_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_mktg_scn01"."jrn_party" "cdc_src"
		INNER JOIN "delta_window" "delta_window" ON  1 = 1
		WHERE  "cdc_src"."trans_timestamp" > "delta_window"."fmc_begin_lw_timestamp" AND "cdc_src"."trans_timestamp" <= "delta_window"."fmc_end_lw_timestamp" AND("cdc_src"."operation" = 'I' or "cdc_src"."operation" = 'D')
	)
	, "delta_view" AS 
	( 
		SELECT 
			  "delta_view_filter"."trans_timestamp" AS "trans_timestamp"
			, "delta_view_filter"."operation" AS "operation"
			, "delta_view_filter"."record_type" AS "record_type"
			, "delta_view_filter"."party_number" AS "party_number"
			, "delta_view_filter"."parent_party_number" AS "parent_party_number"
			, "delta_view_filter"."address_number" AS "address_number"
			, "delta_view_filter"."name" AS "name"
			, "delta_view_filter"."birthdate" AS "birthdate"
			, "delta_view_filter"."gender" AS "gender"
			, "delta_view_filter"."party_type_code" AS "party_type_code"
			, "delta_view_filter"."comments" AS "comments"
			, "delta_view_filter"."update_timestamp" AS "update_timestamp"
		FROM "delta_view_filter" "delta_view_filter"
		UNION 
		SELECT 
			  "cdc_aft"."trans_timestamp" AS "trans_timestamp"
			, "cdc_aft"."operation" AS "operation"
			, 'S' ::text AS "record_type"
			, "cdc_aft"."party_number" AS "party_number"
			, CASE WHEN("cdc_aft"."parent_party_number" = "cdc_bef"."parent_party_number")or("cdc_aft"."parent_party_number" IS NULL AND 
				"cdc_bef"."parent_party_number" IS NULL)THEN TO_NUMBER('-1', '999999999999D999999999'::varchar) else "cdc_aft"."parent_party_number" end AS "parent_party_number"
			, CASE WHEN("cdc_aft"."address_number" = "cdc_bef"."address_number")or("cdc_aft"."address_number" IS NULL AND 
				"cdc_bef"."address_number" IS NULL)THEN TO_NUMBER('-1', '999999999999D999999999'::varchar) else "cdc_aft"."address_number" end AS "address_number"
			, CASE WHEN("cdc_aft"."name" = "cdc_bef"."name")or("cdc_aft"."name" IS NULL AND "cdc_bef"."name" IS NULL)THEN 'unchanged'::text else "cdc_aft"."name" end AS "name"
			, CASE WHEN("cdc_aft"."birthdate" = "cdc_bef"."birthdate")or("cdc_aft"."birthdate" IS NULL AND "cdc_bef"."birthdate" IS NULL)
				THEN TO_DATE('01/01/1970', 'DD/MM/YYYY'::varchar) else "cdc_aft"."birthdate" end AS "birthdate"
			, CASE WHEN("cdc_aft"."gender" = "cdc_bef"."gender")or("cdc_aft"."gender" IS NULL AND "cdc_bef"."gender" IS NULL)
				THEN'unchanged'::text else "cdc_aft"."gender" end AS "gender"
			, CASE WHEN("cdc_aft"."party_type_code" = "cdc_bef"."party_type_code")or("cdc_aft"."party_type_code" IS NULL AND 
				"cdc_bef"."party_type_code" IS NULL)THEN 'unchanged'::text else "cdc_aft"."party_type_code" end AS "party_type_code"
			, CASE WHEN("cdc_aft"."comments" = "cdc_bef"."comments")or("cdc_aft"."comments" IS NULL AND "cdc_bef"."comments" IS NULL)
				THEN'unchanged'::text else "cdc_aft"."comments" end AS "comments"
			, CASE WHEN("cdc_aft"."update_timestamp" = "cdc_bef"."update_timestamp")or("cdc_aft"."update_timestamp" IS NULL AND 
				"cdc_bef"."update_timestamp" IS NULL)THEN TO_TIMESTAMP('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS.US'::varchar) else "cdc_aft"."update_timestamp" end AS "update_timestamp"
		FROM "moto_mktg_scn01"."jrn_party" "cdc_aft"
		INNER JOIN "moto_mktg_scn01"."jrn_party" "cdc_bef" ON  "cdc_bef"."trans_id" = "cdc_aft"."trans_id"
		INNER JOIN "delta_window" "delta_window" ON  1 = 1
		WHERE  "cdc_bef"."image_type" = 'BEFORE' and "cdc_aft"."image_type" = 'AFTER' AND "cdc_aft"."operation" = 'U' AND "cdc_aft"."trans_timestamp" > "delta_window"."fmc_begin_lw_timestamp" AND "cdc_aft"."trans_timestamp" <= "delta_window"."fmc_end_lw_timestamp"
	)
	, "prepjoinbk" AS 
	( 
		SELECT 
			  "delta_view"."trans_timestamp" AS "trans_timestamp"
			, "delta_view"."operation" AS "operation"
			, "delta_view"."record_type" AS "record_type"
			, COALESCE("delta_view"."party_number", TO_NUMBER("mex_bk_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "party_number"
			, COALESCE("delta_view"."parent_party_number", TO_NUMBER("mex_bk_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "parent_party_number"
			, COALESCE("delta_view"."address_number", TO_NUMBER("mex_bk_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "address_number"
			, "delta_view"."name" AS "name"
			, "delta_view"."birthdate" AS "birthdate"
			, "delta_view"."gender" AS "gender"
			, "delta_view"."party_type_code" AS "party_type_code"
			, "delta_view"."comments" AS "comments"
			, "delta_view"."update_timestamp" AS "update_timestamp"
		FROM "delta_view" "delta_view"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_bk_src" ON  1 = 1
		WHERE  "mex_bk_src"."record_type" = 'N'
	)
	SELECT 
		  "prepjoinbk"."trans_timestamp" AS "trans_timestamp"
		, "prepjoinbk"."operation" AS "operation"
		, "prepjoinbk"."record_type" AS "record_type"
		, "prepjoinbk"."party_number" AS "party_number"
		, "prepjoinbk"."parent_party_number" AS "parent_party_number"
		, "prepjoinbk"."address_number" AS "address_number"
		, "prepjoinbk"."name" AS "name"
		, "prepjoinbk"."birthdate" AS "birthdate"
		, "prepjoinbk"."gender" AS "gender"
		, "prepjoinbk"."party_type_code" AS "party_type_code"
		, "prepjoinbk"."comments" AS "comments"
		, "prepjoinbk"."update_timestamp" AS "update_timestamp"
	FROM "prepjoinbk" "prepjoinbk"
	;

 
 
