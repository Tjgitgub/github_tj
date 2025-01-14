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

DROP TABLE IF EXISTS "source1_stg"."table1" 
CASCADE
;
DROP TABLE IF EXISTS "source1_stg"."table1_curr" 
CASCADE
;
DROP TABLE IF EXISTS "source1_stg"."table1_prev" 
CASCADE
;
DROP TABLE IF EXISTS "source1_stg"."table2" 
CASCADE
;
DROP TABLE IF EXISTS "source1_stg"."table2_curr" 
CASCADE
;
DROP TABLE IF EXISTS "source1_stg"."table2_prev" 
CASCADE
;
DROP TABLE IF EXISTS "source1_stg"."table3" 
CASCADE
;
DROP TABLE IF EXISTS "source1_stg"."table3_curr" 
CASCADE
;
DROP TABLE IF EXISTS "source1_stg"."table3_prev" 
CASCADE
;
-- END


/* CREATE TABLES */

-- START


CREATE   TABLE "source1_stg"."table1"
(
    "table1_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"src_bk" VARCHAR
   ,"load_cycle_id" INTEGER
   ,"jrn_flag" VARCHAR
   ,"record_type" VARCHAR
   ,"other_attr_bk" VARCHAR(1500)
   ,"other_attr" VARCHAR
   ,"table1_id" INTEGER
   ,"some_attr" INTEGER
)
;

COMMENT ON TABLE "source1_stg"."table1" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


CREATE   TABLE "source1_stg"."table1_curr"
(
    "table1_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"src_bk" VARCHAR
   ,"load_cycle_id" INTEGER
   ,"jrn_flag" VARCHAR
   ,"record_type" VARCHAR
   ,"other_attr_bk" VARCHAR(1500)
   ,"other_attr" VARCHAR
   ,"table1_id" INTEGER
   ,"some_attr" INTEGER
)
;

COMMENT ON TABLE "source1_stg"."table1_curr" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


CREATE   TABLE "source1_stg"."table1_prev"
(
    "table1_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"src_bk" VARCHAR
   ,"load_cycle_id" INTEGER
   ,"jrn_flag" VARCHAR
   ,"record_type" VARCHAR
   ,"other_attr_bk" VARCHAR(1500)
   ,"other_attr" VARCHAR
   ,"table1_id" INTEGER
   ,"some_attr" INTEGER
)
;

COMMENT ON TABLE "source1_stg"."table1_prev" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


CREATE   TABLE "source1_stg"."table2"
(
    "table2_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"src_bk" VARCHAR
   ,"load_cycle_id" INTEGER
   ,"jrn_flag" VARCHAR
   ,"record_type" VARCHAR
   ,"table2_id_bk" VARCHAR(1500)
   ,"table2_id" INTEGER
   ,"some_attr" INTEGER
   ,"other_attr" VARCHAR
)
;

COMMENT ON TABLE "source1_stg"."table2" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


CREATE   TABLE "source1_stg"."table2_curr"
(
    "table2_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"src_bk" VARCHAR
   ,"load_cycle_id" INTEGER
   ,"jrn_flag" VARCHAR
   ,"record_type" VARCHAR
   ,"table2_id_bk" VARCHAR(1500)
   ,"table2_id" INTEGER
   ,"some_attr" INTEGER
   ,"other_attr" VARCHAR
)
;

COMMENT ON TABLE "source1_stg"."table2_curr" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


CREATE   TABLE "source1_stg"."table2_prev"
(
    "table2_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"src_bk" VARCHAR
   ,"load_cycle_id" INTEGER
   ,"jrn_flag" VARCHAR
   ,"record_type" VARCHAR
   ,"table2_id_bk" VARCHAR(1500)
   ,"table2_id" INTEGER
   ,"some_attr" INTEGER
   ,"other_attr" VARCHAR
)
;

COMMENT ON TABLE "source1_stg"."table2_prev" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


CREATE   TABLE "source1_stg"."table3"
(
    "table3_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"src_bk" VARCHAR
   ,"load_cycle_id" INTEGER
   ,"jrn_flag" VARCHAR
   ,"record_type" VARCHAR
   ,"table3_id_bk" VARCHAR(1500)
   ,"table3_id" INTEGER
   ,"some_attr" INTEGER
   ,"other_attr" VARCHAR
)
;

COMMENT ON TABLE "source1_stg"."table3" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


CREATE   TABLE "source1_stg"."table3_curr"
(
    "table3_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"src_bk" VARCHAR
   ,"load_cycle_id" INTEGER
   ,"jrn_flag" VARCHAR
   ,"record_type" VARCHAR
   ,"table3_id_bk" VARCHAR(1500)
   ,"table3_id" INTEGER
   ,"some_attr" INTEGER
   ,"other_attr" VARCHAR
)
;

COMMENT ON TABLE "source1_stg"."table3_curr" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


CREATE   TABLE "source1_stg"."table3_prev"
(
    "table3_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"src_bk" VARCHAR
   ,"load_cycle_id" INTEGER
   ,"jrn_flag" VARCHAR
   ,"record_type" VARCHAR
   ,"table3_id_bk" VARCHAR(1500)
   ,"table3_id" INTEGER
   ,"some_attr" INTEGER
   ,"other_attr" VARCHAR
)
;

COMMENT ON TABLE "source1_stg"."table3_prev" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


-- END


