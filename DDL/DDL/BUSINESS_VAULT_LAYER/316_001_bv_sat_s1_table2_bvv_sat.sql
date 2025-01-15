/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.13, generation date: 2025/01/14 14:14:54
DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/14 14:14:05, 
SRC_NAME: source1 - Release: source1(2) - Comment: 2 - Release date: 2025/01/14 14:13:16
 */


DROP VIEW IF EXISTS "dv_0114_bv"."sat_s1_table2";
CREATE  VIEW "dv_0114_bv"."sat_s1_table2"  AS 
	SELECT 
		  "dvt_src"."table2_hkey" AS "table2_hkey"
		, "dvt_src"."load_date" AS "load_date"
		, "dvt_src"."load_cycle_id" AS "load_cycle_id"
		, "dvt_src"."load_end_date" AS "load_end_date"
		, "dvt_src"."hash_diff" AS "hash_diff"
		, "dvt_src"."delete_flag" AS "delete_flag"
		, "dvt_src"."table2_id" AS "table2_id"
		, "dvt_src"."some_attr" AS "some_attr"
		, "dvt_src"."other_attr" AS "other_attr"
	FROM "dv_0114_fl"."sat_s1_table2" "dvt_src"
	;

 
 
