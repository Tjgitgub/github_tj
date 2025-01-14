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

DROP TABLE IF EXISTS "source1_cdc"."cdc_table1" 
CASCADE
;
DROP TABLE IF EXISTS "source1_cdc"."cdc_table2" 
CASCADE
;
DROP TABLE IF EXISTS "source1_cdc"."cdc_table3" 
CASCADE
;

-- END


/* CREATE TABLES */

-- START


CREATE   TABLE "source1_cdc"."cdc_table1"
(
    "table1_id" INTEGER
   ,"some_attr" INTEGER
   ,"other_attr" VARCHAR
)
;

COMMENT ON TABLE "source1_cdc"."cdc_table1" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
SRC_NAME: source1 - Release: source1(2) - Comment: 2 - Release date: 2025/01/14 14:13:16';


CREATE   TABLE "source1_cdc"."cdc_table2"
(
    "table2_id" INTEGER
   ,"some_attr" INTEGER
   ,"other_attr" VARCHAR
)
;

COMMENT ON TABLE "source1_cdc"."cdc_table2" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
SRC_NAME: source1 - Release: source1(2) - Comment: 2 - Release date: 2025/01/14 14:13:16';


CREATE   TABLE "source1_cdc"."cdc_table3"
(
    "table3_id" INTEGER
   ,"some_attr" INTEGER
   ,"other_attr" VARCHAR
)
;

COMMENT ON TABLE "source1_cdc"."cdc_table3" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
SRC_NAME: source1 - Release: source1(2) - Comment: 2 - Release date: 2025/01/14 14:13:16';


-- END


