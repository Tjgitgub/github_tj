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
DROP TABLE IF EXISTS "source1_mtd"."fmc_loading_window_table" 
CASCADE
;
DROP TABLE IF EXISTS "source1_mtd"."load_cycle_info" 
CASCADE
;
DROP TABLE IF EXISTS "source1_mtd"."mtd_exception_records" 
CASCADE
;

-- END


/* CREATE TABLES */
-- START

CREATE   TABLE "source1_mtd"."fmc_loading_window_table"
(
	"fmc_begin_lw_timestamp" TIMESTAMP,
	"fmc_end_lw_timestamp" TIMESTAMP
)
;

COMMENT ON TABLE "source1_mtd"."fmc_loading_window_table" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
SRC_NAME: source1 - Release: source1(2) - Comment: 2 - Release date: 2025/01/14 14:13:16';


CREATE   TABLE "source1_mtd"."load_cycle_info"
(
	"load_cycle_id" INTEGER,
	"load_date" TIMESTAMP
)
;

COMMENT ON TABLE "source1_mtd"."load_cycle_info" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
SRC_NAME: source1 - Release: source1(2) - Comment: 2 - Release date: 2025/01/14 14:13:16';


CREATE   TABLE "source1_mtd"."mtd_exception_records"
(
	"load_cycle_id" VARCHAR(3),
	"record_type" VARCHAR(3),
	"key_attribute_integer" VARCHAR(11),
	"key_attribute_timestamp" VARCHAR(19),
	"key_attribute_timestamp_with_time_zone" VARCHAR,
	"key_attribute_varchar" VARCHAR(3),
	"attribute_integer" VARCHAR(11),
	"attribute_timestamp" VARCHAR(19),
	"attribute_timestamp_with_time_zone" VARCHAR,
	"attribute_varchar" VARCHAR(3)
)
;

COMMENT ON TABLE "source1_mtd"."mtd_exception_records" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
SRC_NAME: source1 - Release: source1(2) - Comment: 2 - Release date: 2025/01/14 14:13:16';


INSERT INTO "source1_mtd"."mtd_exception_records"
("load_cycle_id", "record_type", "key_attribute_integer", "key_attribute_timestamp", "key_attribute_timestamp_with_time_zone", "key_attribute_varchar", "attribute_integer", "attribute_timestamp", "attribute_timestamp_with_time_zone", "attribute_varchar") VALUES ('-2','U','-2147483647','01/01/2899 00:00:00','01/01/2899 00:00:00','~?~','-2147483647','01/01/2899 00:00:00','01/01/2899 00:00:00','~?~');

INSERT INTO "source1_mtd"."mtd_exception_records"
("load_cycle_id", "record_type", "key_attribute_integer", "key_attribute_timestamp", "key_attribute_timestamp_with_time_zone", "key_attribute_varchar", "attribute_integer", "attribute_timestamp", "attribute_timestamp_with_time_zone", "attribute_varchar") VALUES ('-1','N','-2147483648','01/01/2999 00:00:00','01/01/2999 00:00:00','0','-2147483648','01/01/2999 00:00:00','01/01/2999 00:00:00','0');

-- END


