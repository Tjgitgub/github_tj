CREATE OR REPLACE FUNCTION "project1_proc"."stg_s1_table1_prep_delete"() 
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

Vaultspeed version: 5.7.2.13, generation date: 2025/01/14 14:14:32
DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/14 14:14:05, 
SRC_NAME: source1 - Release: source1(2) - Comment: 2 - Release date: 2025/01/14 14:13:16
 */


BEGIN 

BEGIN -- stg_prep_del_tgt

	TRUNCATE TABLE "source1_stg"."table1"  CASCADE;

	INSERT INTO "source1_stg"."table1"(
		 "table1_hkey"
		,"load_date"
		,"load_cycle_id"
		,"src_bk"
		,"jrn_flag"
		,"record_type"
		,"other_attr"
		,"other_attr_bk"
		,"table1_id"
		,"some_attr"
	)
	WITH "stg_curr_prev" AS 
	( 
		SELECT 
			  "stg_curr_ins"."table1_hkey" AS "table1_hkey"
			, "stg_curr_ins"."load_date" AS "load_date"
			, "stg_curr_ins"."load_cycle_id" AS "load_cycle_id"
			, "stg_curr_ins"."src_bk" AS "src_bk"
			, 'I' ::text AS "jrn_flag"
			, "stg_curr_ins"."record_type" AS "record_type"
			, "stg_curr_ins"."other_attr" AS "other_attr"
			, "stg_curr_ins"."other_attr_bk" AS "other_attr_bk"
			, "stg_curr_ins"."table1_id" AS "table1_id"
			, "stg_curr_ins"."some_attr" AS "some_attr"
		FROM "source1_stg"."table1_curr" "stg_curr_ins"
		LEFT OUTER JOIN "source1_stg"."table1_prev" "stg_prev_ins" ON  "stg_prev_ins"."table1_hkey" = "stg_curr_ins"."table1_hkey"
		WHERE  "stg_prev_ins"."table1_hkey" IS NULL
		UNION ALL 
		SELECT 
			  "stg_curr_upd"."table1_hkey" AS "table1_hkey"
			, "stg_curr_upd"."load_date" AS "load_date"
			, "stg_curr_upd"."load_cycle_id" AS "load_cycle_id"
			, "stg_curr_upd"."src_bk" AS "src_bk"
			, 'U' ::text AS "jrn_flag"
			, "stg_curr_upd"."record_type" AS "record_type"
			, "stg_curr_upd"."other_attr" AS "other_attr"
			, "stg_curr_upd"."other_attr_bk" AS "other_attr_bk"
			, "stg_curr_upd"."table1_id" AS "table1_id"
			, "stg_curr_upd"."some_attr" AS "some_attr"
		FROM "source1_stg"."table1_curr" "stg_curr_upd"
		INNER JOIN "source1_stg"."table1_prev" "stg_prev_upd" ON  "stg_prev_upd"."table1_hkey" = "stg_curr_upd"."table1_hkey"
		INNER JOIN "source1_mtd"."mtd_exception_records" "mex_null" ON  "mex_null"."record_type" = 'N'
		WHERE  COALESCE("stg_prev_upd"."table1_id", CAST("mex_null"."attribute_integer" AS INTEGER))!= COALESCE("stg_curr_upd"."table1_id", CAST("mex_null"."attribute_integer" AS INTEGER)) OR  COALESCE("stg_prev_upd"."some_attr", CAST("mex_null"."attribute_integer" AS INTEGER))!= COALESCE("stg_curr_upd"."some_attr", CAST("mex_null"."attribute_integer" AS INTEGER)) OR  COALESCE("stg_prev_upd"."other_attr", "mex_null"."attribute_varchar"::text)!= COALESCE("stg_curr_upd"."other_attr", "mex_null"."attribute_varchar"::text)
		UNION ALL 
		SELECT 
			  "stg_prev_del"."table1_hkey" AS "table1_hkey"
			, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "stg_prev_del"."other_attr_bk"  ORDER BY  "lci_src"."load_date")
				* interval'2 microsecond'   AS "load_date"
			, "lci_src"."load_cycle_id" AS "load_cycle_id"
			, "stg_prev_del"."src_bk" AS "src_bk"
			, 'D' ::text AS "jrn_flag"
			, "stg_prev_del"."record_type" AS "record_type"
			, "stg_prev_del"."other_attr" AS "other_attr"
			, "stg_prev_del"."other_attr_bk" AS "other_attr_bk"
			, "stg_prev_del"."table1_id" AS "table1_id"
			, "stg_prev_del"."some_attr" AS "some_attr"
		FROM "source1_stg"."table1_prev" "stg_prev_del"
		LEFT OUTER JOIN "source1_stg"."table1_curr" "stg_curr_del" ON  "stg_prev_del"."table1_hkey" = "stg_curr_del"."table1_hkey"
		INNER JOIN "source1_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		WHERE  "stg_curr_del"."table1_hkey" IS NULL
	)
	SELECT 
		  "stg_curr_prev"."table1_hkey" AS "table1_hkey"
		, "stg_curr_prev"."load_date" AS "load_date"
		, "stg_curr_prev"."load_cycle_id" AS "load_cycle_id"
		, "stg_curr_prev"."src_bk" AS "src_bk"
		, "stg_curr_prev"."jrn_flag" AS "jrn_flag"
		, "stg_curr_prev"."record_type" AS "record_type"
		, "stg_curr_prev"."other_attr" AS "other_attr"
		, "stg_curr_prev"."other_attr_bk" AS "other_attr_bk"
		, "stg_curr_prev"."table1_id" AS "table1_id"
		, "stg_curr_prev"."some_attr" AS "some_attr"
	FROM "stg_curr_prev" "stg_curr_prev"
	;
END;



END;
$function$;
 
 
