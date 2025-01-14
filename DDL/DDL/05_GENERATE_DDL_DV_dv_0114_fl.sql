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
DROP TABLE IF EXISTS "dv_0114_fl"."sat_s1_table1" 
CASCADE
;
DROP TABLE IF EXISTS "dv_0114_fl"."sat_s1_table2" 
CASCADE
;
DROP TABLE IF EXISTS "dv_0114_fl"."sat_s1_table3" 
CASCADE
;
DROP TABLE IF EXISTS "dv_0114_fl"."hub_table1" 
CASCADE
;
DROP TABLE IF EXISTS "dv_0114_fl"."hub_table2" 
CASCADE
;
DROP TABLE IF EXISTS "dv_0114_fl"."hub_table3" 
CASCADE
;

-- END


/* CREATE TABLES */

-- START

CREATE   TABLE "dv_0114_fl"."hub_table1"
(
    "table1_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"src_bk" VARCHAR NOT NULL
   ,"other_attr_bk" VARCHAR(1500)
   ,CONSTRAINT "hub_table1_pk" PRIMARY KEY ("table1_hkey")   
   ,CONSTRAINT "hub_table1_uk" UNIQUE ("src_bk", "other_attr_bk")   
)
;

COMMENT ON TABLE "dv_0114_fl"."hub_table1" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


CREATE   TABLE "dv_0114_fl"."hub_table2"
(
    "table2_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"src_bk" VARCHAR NOT NULL
   ,"table2_id_bk" VARCHAR(1500)
   ,CONSTRAINT "hub_table2_pk" PRIMARY KEY ("table2_hkey")   
   ,CONSTRAINT "hub_table2_uk" UNIQUE ("src_bk", "table2_id_bk")   
)
;

COMMENT ON TABLE "dv_0114_fl"."hub_table2" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


CREATE   TABLE "dv_0114_fl"."hub_table3"
(
    "table3_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"src_bk" VARCHAR NOT NULL
   ,"table3_id_bk" VARCHAR(1500)
   ,CONSTRAINT "hub_table3_pk" PRIMARY KEY ("table3_hkey")   
   ,CONSTRAINT "hub_table3_uk" UNIQUE ("table3_id_bk", "src_bk")   
)
;

COMMENT ON TABLE "dv_0114_fl"."hub_table3" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


CREATE   TABLE "dv_0114_fl"."sat_s1_table1"
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
   ,CONSTRAINT "sat_s1_table1_uk" UNIQUE ("table1_hkey", "load_date")   
)
;

ALTER TABLE "dv_0114_fl"."sat_s1_table1" ADD CONSTRAINT "sat_s1_table1_fk" FOREIGN KEY ("table1_hkey") REFERENCES "dv_0114_fl"."hub_table1"("table1_hkey");
 ALTER TABLE "dv_0114_fl"."sat_s1_table1" DISABLE TRIGGER ALL;
COMMENT ON TABLE "dv_0114_fl"."sat_s1_table1" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


CREATE   TABLE "dv_0114_fl"."sat_s1_table2"
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
   ,CONSTRAINT "sat_s1_table2_uk" UNIQUE ("table2_hkey", "load_date")   
)
;

ALTER TABLE "dv_0114_fl"."sat_s1_table2" ADD CONSTRAINT "sat_s1_table2_fk" FOREIGN KEY ("table2_hkey") REFERENCES "dv_0114_fl"."hub_table2"("table2_hkey");
 ALTER TABLE "dv_0114_fl"."sat_s1_table2" DISABLE TRIGGER ALL;
COMMENT ON TABLE "dv_0114_fl"."sat_s1_table2" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


CREATE   TABLE "dv_0114_fl"."sat_s1_table3"
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
   ,CONSTRAINT "sat_s1_table3_uk" UNIQUE ("table3_hkey", "load_date")   
)
;

ALTER TABLE "dv_0114_fl"."sat_s1_table3" ADD CONSTRAINT "sat_s1_table3_fk" FOREIGN KEY ("table3_hkey") REFERENCES "dv_0114_fl"."hub_table3"("table3_hkey");
 ALTER TABLE "dv_0114_fl"."sat_s1_table3" DISABLE TRIGGER ALL;
COMMENT ON TABLE "dv_0114_fl"."sat_s1_table3" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57';


-- END


