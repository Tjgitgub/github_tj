/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.13, generation date: 2025/01/14 14:14:18
DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57
 */

/* DROP TABLES */

-- START
DROP TABLE IF EXISTS "dv_0114_fl"."sat_s1_table1_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "dv_0114_fl"."sat_s1_table2_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "dv_0114_fl"."sat_s1_table3_tmp" 
CASCADE
;

-- END


/* CREATE TABLES */

-- START

CREATE   TABLE "dv_0114_fl"."sat_s1_table1_tmp"
(
    "table1_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(3) NOT NULL
   ,"other_attr" VARCHAR
   ,"some_attr" INTEGER
   ,"table1_id" INTEGER
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "dv_0114_fl"."sat_s1_table1_tmp" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


CREATE   TABLE "dv_0114_fl"."sat_s1_table2_tmp"
(
    "table2_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(3) NOT NULL
   ,"other_attr" VARCHAR
   ,"some_attr" INTEGER
   ,"table2_id" INTEGER
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "dv_0114_fl"."sat_s1_table2_tmp" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


CREATE   TABLE "dv_0114_fl"."sat_s1_table3_tmp"
(
    "table3_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(3) NOT NULL
   ,"other_attr" VARCHAR
   ,"some_attr" INTEGER
   ,"table3_id" INTEGER
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "dv_0114_fl"."sat_s1_table3_tmp" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


-- END


