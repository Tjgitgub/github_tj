/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.13, generation date: 2025/01/14 14:14:18
DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/14 14:14:05
 */

/* DROP TABLES */

-- START
DROP TABLE IF EXISTS "project1_fmc"."fmc_bv_loading_window_table" 
CASCADE
;
DROP TABLE IF EXISTS "project1_fmc"."load_cycle_info" 
CASCADE
;
DROP TABLE IF EXISTS "project1_fmc"."dv_load_cycle_info" 
CASCADE
;

-- END


/* CREATE TABLES */

-- START

CREATE   TABLE "project1_fmc"."fmc_bv_loading_window_table"
(
	"fmc_begin_lw_timestamp" TIMESTAMP,
	"fmc_end_lw_timestamp" TIMESTAMP
)
;

COMMENT ON TABLE "project1_fmc"."fmc_bv_loading_window_table" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/14 14:14:05';


CREATE   TABLE "project1_fmc"."load_cycle_info"
(
	"load_cycle_id" INTEGER,
	"load_date" TIMESTAMP
)
;

COMMENT ON TABLE "project1_fmc"."load_cycle_info" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/14 14:14:05';


CREATE   TABLE "project1_fmc"."dv_load_cycle_info"
(
	"dv_load_cycle_id" INTEGER
)
;

COMMENT ON TABLE "project1_fmc"."dv_load_cycle_info" IS 'DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/14 14:14:05';


-- END


