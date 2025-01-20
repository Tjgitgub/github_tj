/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.16, generation date: 2025/01/18 14:08:56
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/16 14:56:23, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08
 */


DROP VIEW IF EXISTS "moto_scn01_bv"."view_pit_camp_prod_camp_prod_snapshotdates";
CREATE  VIEW "moto_scn01_bv"."view_pit_camp_prod_camp_prod_snapshotdates"  AS 
	SELECT 
		  "sat_src1"."lnd_camp_prod_hkey" AS "lnd_camp_prod_hkey"
		, "sat_src1"."trans_timestamp" AS "snapshot_timestamp"
	FROM "moto_scn01_fl"."lds_mm_camp_prod_class" "sat_src1"
	UNION 
	SELECT 
		  "sat_src2"."lnd_camp_prod_hkey" AS "lnd_camp_prod_hkey"
		, "sat_src2"."trans_timestamp" AS "snapshot_timestamp"
	FROM "moto_scn01_fl"."lds_mm_camp_prod_emo" "sat_src2"
	;

 
 
