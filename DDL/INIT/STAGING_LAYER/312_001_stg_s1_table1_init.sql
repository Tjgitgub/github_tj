CREATE OR REPLACE FUNCTION "project1_proc"."stg_s1_table1_init"() 
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

BEGIN -- stg_tgt

	TRUNCATE TABLE "source1_stg"."table1_curr"  CASCADE;

	INSERT INTO "source1_stg"."table1_curr"(
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
	SELECT 
		  UPPER(ENCODE(DIGEST( 's1' || '#' || "ext_src"."other_attr_bk" || '#' ,'MD5'),'HEX')) AS "table1_hkey"
		, "ext_src"."load_date" AS "load_date"
		, "ext_src"."load_cycle_id" AS "load_cycle_id"
		, 's1' AS "src_bk"
		, "ext_src"."jrn_flag" AS "jrn_flag"
		, "ext_src"."record_type" AS "record_type"
		, "ext_src"."other_attr" AS "other_attr"
		, "ext_src"."other_attr_bk" AS "other_attr_bk"
		, "ext_src"."table1_id" AS "table1_id"
		, "ext_src"."some_attr" AS "some_attr"
	FROM "source1_ext"."table1" "ext_src"
	INNER JOIN "source1_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
	;
END;


BEGIN -- stg_tgt_del

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
	SELECT 
		  "stg_curr_src"."table1_hkey" AS "table1_hkey"
		, "stg_curr_src"."load_date" AS "load_date"
		, "stg_curr_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_curr_src"."src_bk" AS "src_bk"
		, "stg_curr_src"."jrn_flag" AS "jrn_flag"
		, "stg_curr_src"."record_type" AS "record_type"
		, "stg_curr_src"."other_attr" AS "other_attr"
		, "stg_curr_src"."other_attr_bk" AS "other_attr_bk"
		, "stg_curr_src"."table1_id" AS "table1_id"
		, "stg_curr_src"."some_attr" AS "some_attr"
	FROM "source1_stg"."table1_curr" "stg_curr_src"
	;
END;



END;
$function$;
 
 
