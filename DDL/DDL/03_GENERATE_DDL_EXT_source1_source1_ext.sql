/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.13, generation date: 2025/01/14 14:14:18
DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
SRC_NAME: source1 - Release: source1(2) - Comment: 2 - Release date: 2025/01/14 14:13:16
 */

/* DROP TABLES */

-- START

DROP TABLE IF EXISTS "source1_ext"."table1" 
CASCADE
;
DROP TABLE IF EXISTS "source1_ext"."table2" 
CASCADE
;
DROP TABLE IF EXISTS "source1_ext"."table3" 
CASCADE
;

-- END


/* CREATE TABLES */

-- START


CREATE   TABLE "source1_ext"."table1"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"jrn_flag" VARCHAR
   ,"record_type" VARCHAR
   ,"other_attr" VARCHAR
   ,"other_attr_bk" VARCHAR(1500)
   ,"table1_id" INTEGER
   ,"some_attr" INTEGER
)
;

COMMENT ON TABLE "source1_ext"."table1" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
SRC_NAME: source1 - Release: source1(2) - Comment: 2 - Release date: 2025/01/14 14:13:16';


CREATE   TABLE "source1_ext"."table2"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"jrn_flag" VARCHAR
   ,"record_type" VARCHAR
   ,"table2_id" INTEGER
   ,"table2_id_bk" VARCHAR(1500)
   ,"some_attr" INTEGER
   ,"other_attr" VARCHAR
)
;

COMMENT ON TABLE "source1_ext"."table2" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
SRC_NAME: source1 - Release: source1(2) - Comment: 2 - Release date: 2025/01/14 14:13:16';


CREATE   TABLE "source1_ext"."table3"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"jrn_flag" VARCHAR
   ,"record_type" VARCHAR
   ,"table3_id" INTEGER
   ,"table3_id_bk" VARCHAR(1500)
   ,"some_attr" INTEGER
   ,"other_attr" VARCHAR
)
;

COMMENT ON TABLE "source1_ext"."table3" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
SRC_NAME: source1 - Release: source1(2) - Comment: 2 - Release date: 2025/01/14 14:13:16';


-- END


