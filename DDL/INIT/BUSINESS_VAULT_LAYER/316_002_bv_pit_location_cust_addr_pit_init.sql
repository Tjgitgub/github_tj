CREATE OR REPLACE FUNCTION "moto_scn01_proc"."bv_pit_location_cust_addr_pit_init"() 
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

Vaultspeed version: 5.7.2.16, generation date: 2025/01/18 14:08:56
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/16 14:56:23, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08
 */


BEGIN 

BEGIN -- pit_tgt

	TRUNCATE TABLE "moto_scn01_bv"."pit_location_cust_addr"  CASCADE;

	INSERT INTO "moto_scn01_bv"."pit_location_cust_addr"(
		 "pit_location_cust_addr_hkey"
		,"lnk_cust_addr_hkey"
		,"snapshot_timestamp"
		,"load_cycle_id"
		,"lks_mm_cust_addr_hkey"
		,"lks_mm_cust_addr_load_date"
	)
	SELECT 
		  UPPER(ENCODE(DIGEST( "hub_src"."customers_hkey"::text || '#' || TO_CHAR("snapshotdates"."snapshot_timestamp", 
			'DD/MM/YYYY HH24:MI:SS.US'::varchar) ,'MD5'),'HEX')) AS "pit_location_cust_addr_hkey"
		, "hub_src"."lnk_cust_addr_hkey" AS "lnk_cust_addr_hkey"
		, "snapshotdates"."snapshot_timestamp" AS "snapshot_timestamp"
		, "bvlci_src"."load_cycle_id" AS "load_cycle_id"
		, COALESCE("sat_src1"."lnk_cust_addr_hkey","unsat_src1"."lnk_cust_addr_hkey") AS "lks_mm_cust_addr_hkey"
		, COALESCE("sat_src1"."load_date","unsat_src1"."load_date") AS "lks_mm_cust_addr_load_date"
	FROM "moto_scn01_fl"."lnk_cust_addr" "hub_src"
	INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
	INNER JOIN "moto_scn01_fmc"."load_cycle_info" "bvlci_src" ON  1 = 1
	INNER JOIN "moto_scn01_bv"."view_pit_location_cust_addr_snapshotdates" "snapshotdates" ON  "snapshotdates"."lnk_cust_addr_hkey" = "hub_src"."lnk_cust_addr_hkey"
	LEFT OUTER JOIN "moto_scn01_fl"."lks_mm_cust_addr" "sat_src1" ON  "hub_src"."customers_hkey" = "sat_src1"."customers_hkey" AND "snapshotdates"."snapshot_timestamp" >= "sat_src1"."load_date" AND 
		"snapshotdates"."snapshot_timestamp" < "sat_src1"."load_end_date" AND "sat_src1"."delete_flag" != 'Y'::text
	INNER JOIN "moto_scn01_fl"."lks_mm_cust_addr" "unsat_src1" ON  "mex_src"."load_cycle_id"::int = "unsat_src1"."load_cycle_id"
	WHERE ("hub_src"."lnk_cust_addr_hkey" = "sat_src1"."lnk_cust_addr_hkey" OR "sat_src1"."lnk_cust_addr_hkey" IS NULL)
	;
END;



END;
$function$;
 
 
