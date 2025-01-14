CREATE OR REPLACE FUNCTION "project1_proc"."hub_s1_table1_init"() 
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

BEGIN -- hub_tgt

	TRUNCATE TABLE "dv_0114_fl"."hub_table1"  CASCADE;

	INSERT INTO "dv_0114_fl"."hub_table1"(
		 "table1_hkey"
		,"load_date"
		,"load_cycle_id"
		,"other_attr_bk"
		,"src_bk"
	)
	WITH "change_set" AS 
	( 
		SELECT 
			  "stg_src1"."table1_hkey" AS "table1_hkey"
			, "stg_src1"."load_date" AS "load_date"
			, "stg_src1"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "logposition"
			, "stg_src1"."other_attr_bk" AS "other_attr_bk"
			, "stg_src1"."src_bk" AS "src_bk"
			, 0 AS "general_order"
		FROM "source1_stg"."table1" "stg_src1"
	)
	, "min_load_time" AS 
	( 
		SELECT 
			  "change_set"."table1_hkey" AS "table1_hkey"
			, "change_set"."load_date" AS "load_date"
			, "change_set"."load_cycle_id" AS "load_cycle_id"
			, "change_set"."other_attr_bk" AS "other_attr_bk"
			, "change_set"."src_bk" AS "src_bk"
			, ROW_NUMBER()OVER(PARTITION BY "change_set"."table1_hkey" ORDER BY "change_set"."general_order","change_set"."load_date",
				"change_set"."logposition") AS "dummy"
		FROM "change_set" "change_set"
	)
	SELECT 
		  "min_load_time"."table1_hkey" AS "table1_hkey"
		, "min_load_time"."load_date" AS "load_date"
		, "min_load_time"."load_cycle_id" AS "load_cycle_id"
		, "min_load_time"."other_attr_bk" AS "other_attr_bk"
		, "min_load_time"."src_bk" AS "src_bk"
	FROM "min_load_time" "min_load_time"
	WHERE  "min_load_time"."dummy" = 1
	;
END;



END;
$function$;
 
 
