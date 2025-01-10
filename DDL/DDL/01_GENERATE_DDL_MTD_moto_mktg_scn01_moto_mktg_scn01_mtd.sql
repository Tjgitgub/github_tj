/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.14, generation date: 2025/01/09 12:47:27
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51
 */

/* DROP TABLES */
-- START
DROP TABLE IF EXISTS "moto_mktg_scn01_mtd"."fmc_loading_window_table" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_mtd"."load_cycle_info" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_mtd"."mtd_exception_records" 
CASCADE
;

-- END


/* CREATE TABLES */
-- START

CREATE   TABLE "moto_mktg_scn01_mtd"."fmc_loading_window_table"
(
	"fmc_begin_lw_timestamp" TIMESTAMP,
	"fmc_end_lw_timestamp" TIMESTAMP
)
;

COMMENT ON TABLE "moto_mktg_scn01_mtd"."fmc_loading_window_table" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_mtd"."load_cycle_info"
(
	"load_cycle_id" INTEGER,
	"load_date" TIMESTAMP
)
;

COMMENT ON TABLE "moto_mktg_scn01_mtd"."load_cycle_info" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_mtd"."mtd_exception_records"
(
	"load_cycle_id" VARCHAR(4),
	"record_type" VARCHAR(4),
	"key_attribute_character" VARCHAR(4),
	"key_attribute_date" VARCHAR(10),
	"key_attribute_gps" VARCHAR(12),
	"key_attribute_integer" VARCHAR(11),
	"key_attribute_numeric" VARCHAR(13),
	"key_attribute_timestamp" VARCHAR(19),
	"key_attribute_timestamp_with_time_zone" VARCHAR,
	"key_attribute_varchar" VARCHAR(4),
	"attribute_character" VARCHAR(4),
	"attribute_date" VARCHAR(10),
	"attribute_gps" VARCHAR(12),
	"attribute_integer" VARCHAR(11),
	"attribute_numeric" VARCHAR(13),
	"attribute_timestamp" VARCHAR(19),
	"attribute_timestamp_with_time_zone" VARCHAR,
	"attribute_varchar" VARCHAR(4)
)
;

COMMENT ON TABLE "moto_mktg_scn01_mtd"."mtd_exception_records" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


INSERT INTO "moto_mktg_scn01_mtd"."mtd_exception_records"
("load_cycle_id", "record_type", "key_attribute_character", "key_attribute_date", "key_attribute_gps", "key_attribute_integer", "key_attribute_numeric", "key_attribute_timestamp", "key_attribute_timestamp_with_time_zone", "key_attribute_varchar", "attribute_character", "attribute_date", "attribute_gps", "attribute_integer", "attribute_numeric", "attribute_timestamp", "attribute_timestamp_with_time_zone", "attribute_varchar") VALUES ('-2','U','~UN~','31/12/2599','(-2, -2, -2)','-2147483647','-999999999998','01/01/2899 00:00:00','01/01/2899 00:00:00','~UN~','~UN~','31/12/2599','(-2, -2, -2)','-2147483647','-999999999998','01/01/2899 00:00:00','01/01/2899 00:00:00','~UN~');

INSERT INTO "moto_mktg_scn01_mtd"."mtd_exception_records"
("load_cycle_id", "record_type", "key_attribute_character", "key_attribute_date", "key_attribute_gps", "key_attribute_integer", "key_attribute_numeric", "key_attribute_timestamp", "key_attribute_timestamp_with_time_zone", "key_attribute_varchar", "attribute_character", "attribute_date", "attribute_gps", "attribute_integer", "attribute_numeric", "attribute_timestamp", "attribute_timestamp_with_time_zone", "attribute_varchar") VALUES ('-1','N','~NL~','31/12/2499','(-1, -1, -1)','-2147483648','-999999999999','01/01/2999 00:00:00','01/01/2999 00:00:00','~NL~','~NL~','31/12/2499','(-1, -1, -1)','-2147483648','-999999999999','01/01/2999 00:00:00','01/01/2999 00:00:00','~NL~');

-- END


