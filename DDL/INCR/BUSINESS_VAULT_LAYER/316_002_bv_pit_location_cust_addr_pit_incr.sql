CREATE OR REPLACE FUNCTION "moto_scn01_proc"."bv_pit_location_cust_addr_pit_incr"() 
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

Vaultspeed version: 5.7.2.16, generation date: 2025/01/16 15:03:29
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/16 14:56:23, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08
 */


BEGIN 

BEGIN -- pit_upd

	WITH "snapshotdates_upd" AS 
	( 
		SELECT 
			  "ssdv_src_upd"."lnk_cust_addr_hkey" AS "lnk_cust_addr_hkey"
			, "ssdv_src_upd"."snapshot_timestamp" AS "snapshot_timestamp"
		FROM "moto_scn01_bv"."view_pit_location_cust_addr_snapshotdates" "ssdv_src_upd"
		INNER JOIN "moto_scn01_fmc"."fmc_bv_loading_window_table" "bvlwt_src_upd" ON  1 = 1
		WHERE  "ssdv_src_upd"."snapshot_timestamp" >= "bvlwt_src_upd"."fmc_begin_lw_timestamp"
	)
	, "miv_upd" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST( "hub_src_upd"."customers_hkey"::text || '#' || TO_CHAR("snapshotdates_upd"."snapshot_timestamp",
				'DD/MM/YYYY HH24:MI:SS.US'::varchar) ,'MD5'),'HEX')) AS "pit_location_cust_addr_hkey"
			, "hub_src_upd"."lnk_cust_addr_hkey" AS "lnk_cust_addr_hkey"
			, "snapshotdates_upd"."snapshot_timestamp" AS "snapshot_timestamp"
			, "bvlci_upd"."load_cycle_id" AS "load_cycle_id"
			, COALESCE("sat_src_upd1"."lnk_cust_addr_hkey","unsat_src_upd1"."lnk_cust_addr_hkey") AS "lks_mm_cust_addr_hkey"
			, COALESCE("sat_src_upd1"."load_date","unsat_src_upd1"."load_date") AS "lks_mm_cust_addr_load_date"
		FROM "moto_scn01_fl"."lnk_cust_addr" "hub_src_upd"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src_upd" ON  "mex_src_upd"."record_type" = 'U'
		INNER JOIN "moto_scn01_fmc"."load_cycle_info" "bvlci_upd" ON  1 = 1
		INNER JOIN "snapshotdates_upd" "snapshotdates_upd" ON  "snapshotdates_upd"."lnk_cust_addr_hkey" = "hub_src_upd"."lnk_cust_addr_hkey"
		LEFT OUTER JOIN "moto_scn01_fl"."lks_mm_cust_addr" "sat_src_upd1" ON  "hub_src_upd"."customers_hkey" = "sat_src_upd1"."customers_hkey" AND "snapshotdates_upd"."snapshot_timestamp" >=
			"sat_src_upd1"."load_date" AND "snapshotdates_upd"."snapshot_timestamp" < "sat_src_upd1"."load_end_date" AND "sat_src_upd1"."delete_flag" != 'Y'::text
		INNER JOIN "moto_scn01_fl"."lks_mm_cust_addr" "unsat_src_upd1" ON  "mex_src_upd"."load_cycle_id"::int = "unsat_src_upd1"."load_cycle_id"
		WHERE ("hub_src_upd"."lnk_cust_addr_hkey" = "sat_src_upd1"."lnk_cust_addr_hkey" OR "sat_src_upd1"."lnk_cust_addr_hkey" IS NULL)
	)
	, "upd_data_src" AS 
	( 
		SELECT 
			  "miv_upd"."pit_location_cust_addr_hkey" AS "pit_location_cust_addr_hkey"
			, "miv_upd"."lnk_cust_addr_hkey" AS "lnk_cust_addr_hkey"
			, "miv_upd"."snapshot_timestamp" AS "snapshot_timestamp"
			, "miv_upd"."load_cycle_id" AS "load_cycle_id"
			, "miv_upd"."lks_mm_cust_addr_hkey" AS "lks_mm_cust_addr_hkey"
			, "miv_upd"."lks_mm_cust_addr_load_date" AS "lks_mm_cust_addr_load_date"
		FROM "miv_upd" "miv_upd"
		LEFT OUTER JOIN "moto_scn01_bv"."pit_location_cust_addr" "pit_src_upd" ON  "miv_upd"."pit_location_cust_addr_hkey" = "pit_src_upd"."pit_location_cust_addr_hkey" AND "miv_upd"."lks_mm_cust_addr_hkey" =
			"pit_src_upd"."lks_mm_cust_addr_hkey" AND "miv_upd"."lks_mm_cust_addr_load_date" = "pit_src_upd"."lks_mm_cust_addr_load_date"
		WHERE  "pit_src_upd"."pit_location_cust_addr_hkey" IS NULL
	)
	UPDATE "moto_scn01_bv"."pit_location_cust_addr" "pit_upd_tgt"
	SET 
		 "lks_mm_cust_addr_hkey" =  "upd_data_src"."lks_mm_cust_addr_hkey"
		,"lks_mm_cust_addr_load_date" =  "upd_data_src"."lks_mm_cust_addr_load_date"
		,"load_cycle_id" =  "upd_data_src"."load_cycle_id"
	FROM  "upd_data_src"
	WHERE "pit_upd_tgt"."pit_location_cust_addr_hkey" =  "upd_data_src"."pit_location_cust_addr_hkey"
	;
END;


BEGIN -- pit_tgt

	INSERT INTO "moto_scn01_bv"."pit_location_cust_addr"(
		 "pit_location_cust_addr_hkey"
		,"lnk_cust_addr_hkey"
		,"snapshot_timestamp"
		,"load_cycle_id"
		,"lks_mm_cust_addr_hkey"
		,"lks_mm_cust_addr_load_date"
	)
	WITH "snapshotdates" AS 
	( 
		SELECT 
			  "ssdv_src"."lnk_cust_addr_hkey" AS "lnk_cust_addr_hkey"
			, "ssdv_src"."snapshot_timestamp" AS "snapshot_timestamp"
		FROM "moto_scn01_bv"."view_pit_location_cust_addr_snapshotdates" "ssdv_src"
		INNER JOIN "moto_scn01_fmc"."fmc_bv_loading_window_table" "bvlwt_src" ON  1 = 1
		WHERE  "ssdv_src"."snapshot_timestamp" >= "bvlwt_src"."fmc_begin_lw_timestamp"
	)
	, "miv" AS 
	( 
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
		INNER JOIN "snapshotdates" "snapshotdates" ON  "snapshotdates"."lnk_cust_addr_hkey" = "hub_src"."lnk_cust_addr_hkey"
		LEFT OUTER JOIN "moto_scn01_fl"."lks_mm_cust_addr" "sat_src1" ON  "hub_src"."customers_hkey" = "sat_src1"."customers_hkey" AND "snapshotdates"."snapshot_timestamp" >= 
			"sat_src1"."load_date" AND "snapshotdates"."snapshot_timestamp" < "sat_src1"."load_end_date" AND "sat_src1"."delete_flag" != 'Y'::text
		INNER JOIN "moto_scn01_fl"."lks_mm_cust_addr" "unsat_src1" ON  "mex_src"."load_cycle_id"::int = "unsat_src1"."load_cycle_id"
		WHERE ("hub_src"."lnk_cust_addr_hkey" = "sat_src1"."lnk_cust_addr_hkey" OR "sat_src1"."lnk_cust_addr_hkey" IS NULL)
	)
	, "data_src" AS 
	( 
		SELECT 
			  "miv"."pit_location_cust_addr_hkey" AS "pit_location_cust_addr_hkey"
			, "miv"."lnk_cust_addr_hkey" AS "lnk_cust_addr_hkey"
			, "miv"."snapshot_timestamp" AS "snapshot_timestamp"
			, "miv"."load_cycle_id" AS "load_cycle_id"
			, "miv"."lks_mm_cust_addr_hkey" AS "lks_mm_cust_addr_hkey"
			, "miv"."lks_mm_cust_addr_load_date" AS "lks_mm_cust_addr_load_date"
		FROM "miv" "miv"
		LEFT OUTER JOIN "moto_scn01_bv"."pit_location_cust_addr" "pit_src" ON  "miv"."pit_location_cust_addr_hkey" = "pit_src"."pit_location_cust_addr_hkey" AND "miv"."lks_mm_cust_addr_hkey" =
			"pit_src"."lks_mm_cust_addr_hkey" AND "miv"."lks_mm_cust_addr_load_date" = "pit_src"."lks_mm_cust_addr_load_date"
		WHERE  "pit_src"."pit_location_cust_addr_hkey" IS NULL
	)
	SELECT 
		  "data_src"."pit_location_cust_addr_hkey" AS "pit_location_cust_addr_hkey"
		, "data_src"."lnk_cust_addr_hkey" AS "lnk_cust_addr_hkey"
		, "data_src"."snapshot_timestamp" AS "snapshot_timestamp"
		, "data_src"."load_cycle_id" AS "load_cycle_id"
		, "data_src"."lks_mm_cust_addr_hkey" AS "lks_mm_cust_addr_hkey"
		, "data_src"."lks_mm_cust_addr_load_date" AS "lks_mm_cust_addr_load_date"
	FROM "data_src" "data_src"
	;
END;



END;
$function$;
 
 
