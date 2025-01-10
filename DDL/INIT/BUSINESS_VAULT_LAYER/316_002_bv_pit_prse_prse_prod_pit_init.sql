CREATE OR REPLACE FUNCTION "moto_scn01_proc"."bv_pit_prse_prse_prod_pit_init"() 
RETURNS void 
LANGUAGE 'plpgsql' 

AS $function$ 
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


BEGIN 

BEGIN -- pit_tgt

	TRUNCATE TABLE "moto_scn01_bv"."pit_prse_prse_prod"  CASCADE;

	INSERT INTO "moto_scn01_bv"."pit_prse_prse_prod"(
		 "pit_prse_prse_prod_hkey"
		,"lnk_prse_prod_hkey"
		,"snapshot_timestamp"
		,"load_cycle_id"
		,"lks_ms_prse_prod_hkey"
		,"lks_ms_prse_prod_load_date"
	)
	SELECT 
		  UPPER(ENCODE(DIGEST( "hub_src"."product_sensors_hkey"::text || '#' || TO_CHAR("snapshotdates"."snapshot_timestamp",
			 'DD/MM/YYYY HH24:MI:SS.US'::varchar) ,'MD5'),'HEX')) AS "pit_prse_prse_prod_hkey"
		, "hub_src"."lnk_prse_prod_hkey" AS "lnk_prse_prod_hkey"
		, "snapshotdates"."snapshot_timestamp" AS "snapshot_timestamp"
		, "bvlci_src"."load_cycle_id" AS "load_cycle_id"
		, COALESCE("sat_src1"."lnk_prse_prod_hkey","unsat_src1"."lnk_prse_prod_hkey") AS "lks_ms_prse_prod_hkey"
		, COALESCE("sat_src1"."load_date","unsat_src1"."load_date") AS "lks_ms_prse_prod_load_date"
	FROM "moto_scn01_fl"."lnk_prse_prod" "hub_src"
	INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
	INNER JOIN "moto_scn01_fmc"."load_cycle_info" "bvlci_src" ON  1 = 1
	INNER JOIN "moto_scn01_bv"."view_pit_prse_prse_prod_snapshotdates" "snapshotdates" ON  "snapshotdates"."lnk_prse_prod_hkey" = "hub_src"."lnk_prse_prod_hkey"
	LEFT OUTER JOIN "moto_scn01_fl"."lks_ms_prse_prod" "sat_src1" ON  "hub_src"."product_sensors_hkey" = "sat_src1"."product_sensors_hkey" AND "snapshotdates"."snapshot_timestamp" >=
		 "sat_src1"."load_date" AND "snapshotdates"."snapshot_timestamp" < "sat_src1"."load_end_date" AND "sat_src1"."delete_flag" != 'Y'::text
	INNER JOIN "moto_scn01_fl"."lks_ms_prse_prod" "unsat_src1" ON  "mex_src"."load_cycle_id"::int = "unsat_src1"."load_cycle_id"
	WHERE ("hub_src"."lnk_prse_prod_hkey" = "sat_src1"."lnk_prse_prod_hkey" OR "sat_src1"."lnk_prse_prod_hkey" IS NULL)
	;
END;



END;
$function$;
 
 