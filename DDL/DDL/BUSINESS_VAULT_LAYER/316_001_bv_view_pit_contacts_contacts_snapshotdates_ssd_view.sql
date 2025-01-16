/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.16, generation date: 2025/01/16 15:03:29
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/16 14:56:23, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08
 */


DROP VIEW IF EXISTS "moto_scn01_bv"."view_pit_contacts_contacts_snapshotdates";
CREATE  VIEW "moto_scn01_bv"."view_pit_contacts_contacts_snapshotdates"  AS 
	WITH RECURSIVE "snapshotdates"( "snapshot_timestamp" ) AS 
	( 
		SELECT 
			  date_trunc('MINUTE', "bvlwt_src"."fmc_begin_lw_timestamp")   AS "snapshot_timestamp"
		FROM "moto_scn01_fmc"."fmc_bv_loading_window_table" "bvlwt_src"
		UNION ALL 
		SELECT 
			  "snapshotdates"."snapshot_timestamp"  +  1 * interval'1 MINUTE'   AS "snapshot_timestamp"
		FROM "snapshotdates" "snapshotdates"
		INNER JOIN "moto_scn01_fmc"."fmc_bv_loading_window_table" "bvlwt_src" ON  "snapshotdates"."snapshot_timestamp"  +  1 * interval'1 MINUTE'   <= "bvlwt_src"."fmc_end_lw_timestamp"
	)
	SELECT 
		  "snapshotdates"."snapshot_timestamp" AS "snapshot_timestamp"
	FROM "snapshotdates" "snapshotdates"
	;

 
 
