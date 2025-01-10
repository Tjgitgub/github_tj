/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.14, generation date: 2025/01/09 12:50:01
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/09 09:40:46, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04
 */


DROP VIEW IF EXISTS "moto_scn01_bv"."ref_ms_codes_to_languag";
CREATE  VIEW "moto_scn01_bv"."ref_ms_codes_to_languag"  AS 
	SELECT 
		  "dvt_src"."code" AS "code"
		, "dvt_src"."language_code" AS "language_code"
		, "dvt_src"."load_cycle_id" AS "load_cycle_id"
		, "dvt_src"."load_date" AS "load_date"
		, "dvt_src"."description" AS "description"
		, "dvt_src"."update_timestamp" AS "update_timestamp"
	FROM "moto_scn01_fl"."ref_ms_codes_to_languag" "dvt_src"
	;

 
 
