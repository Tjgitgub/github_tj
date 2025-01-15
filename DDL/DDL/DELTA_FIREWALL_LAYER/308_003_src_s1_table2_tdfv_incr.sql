/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.13, generation date: 2025/01/14 14:14:32
DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/14 14:14:05, 
SRC_NAME: source1 - Release: source1(2) - Comment: 2 - Release date: 2025/01/14 14:13:16
 */


DROP VIEW IF EXISTS "source1_dfv"."vw_table2";
CREATE  VIEW "source1_dfv"."vw_table2"  AS 
	WITH "delta_view_filter" AS 
	( 
		SELECT 
			  'S' ::text AS "record_type"
			, "cdc_src"."table2_id" AS "table2_id"
			, "cdc_src"."some_attr" AS "some_attr"
			, "cdc_src"."other_attr" AS "other_attr"
		FROM "source1_cdc"."cdc_table2" "cdc_src"
	)
	, "delta_view" AS 
	( 
		SELECT 
			  "delta_view_filter"."record_type" AS "record_type"
			, "delta_view_filter"."table2_id" AS "table2_id"
			, "delta_view_filter"."some_attr" AS "some_attr"
			, "delta_view_filter"."other_attr" AS "other_attr"
		FROM "delta_view_filter" "delta_view_filter"
	)
	, "prepjoinbk" AS 
	( 
		SELECT 
			  "delta_view"."record_type" AS "record_type"
			, COALESCE("delta_view"."table2_id", CAST("mex_bk_src"."key_attribute_integer" AS INTEGER)) AS "table2_id"
			, "delta_view"."some_attr" AS "some_attr"
			, "delta_view"."other_attr" AS "other_attr"
		FROM "delta_view" "delta_view"
		INNER JOIN "source1_mtd"."mtd_exception_records" "mex_bk_src" ON  1 = 1
		WHERE  "mex_bk_src"."record_type" = 'N'
	)
	SELECT 
		  "prepjoinbk"."record_type" AS "record_type"
		, "prepjoinbk"."table2_id" AS "table2_id"
		, "prepjoinbk"."some_attr" AS "some_attr"
		, "prepjoinbk"."other_attr" AS "other_attr"
	FROM "prepjoinbk" "prepjoinbk"
	;

 
 
