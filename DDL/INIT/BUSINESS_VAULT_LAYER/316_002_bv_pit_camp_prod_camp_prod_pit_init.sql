CREATE OR REPLACE FUNCTION "moto_scn01_proc"."bv_pit_camp_prod_camp_prod_pit_init"() 
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

BEGIN -- pit_tgt

	TRUNCATE TABLE "moto_scn01_bv"."pit_camp_prod_camp_prod"  CASCADE;

	INSERT INTO "moto_scn01_bv"."pit_camp_prod_camp_prod"(
		 "pit_camp_prod_camp_prod_hkey"
		,"lnd_camp_prod_hkey"
		,"snapshot_timestamp"
		,"load_cycle_id"
		,"lds_mm_camp_prod_class_hkey"
		,"lds_mm_camp_prod_emo_hkey"
		,"lds_mm_camp_prod_class_trans_timestamp"
		,"lds_mm_camp_prod_emo_trans_timestamp"
	)
	WITH "sat_src2" AS 
	( 
		SELECT 
			  "sat_ed_src2"."lnd_camp_prod_hkey" AS "lnd_camp_prod_hkey"
			, "sat_ed_src2"."trans_timestamp" AS "load_date"
			, COALESCE(LEAD("sat_ed_src2"."trans_timestamp")OVER(PARTITION BY "sat_ed_src2"."lnd_camp_prod_hkey" ORDER BY "sat_ed_src2"."trans_timestamp")
				, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
			, "sat_ed_src2"."delete_flag" AS "delete_flag"
		FROM "moto_scn01_fl"."lds_mm_camp_prod_emo" "sat_ed_src2"
	)
	, "sat_src1" AS 
	( 
		SELECT 
			  "sat_ed_src1"."lnd_camp_prod_hkey" AS "lnd_camp_prod_hkey"
			, "sat_ed_src1"."trans_timestamp" AS "load_date"
			, COALESCE(LEAD("sat_ed_src1"."trans_timestamp")OVER(PARTITION BY "sat_ed_src1"."lnd_camp_prod_hkey" ORDER BY "sat_ed_src1"."trans_timestamp")
				, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
			, "sat_ed_src1"."delete_flag" AS "delete_flag"
		FROM "moto_scn01_fl"."lds_mm_camp_prod_class" "sat_ed_src1"
	)
	SELECT 
		  UPPER(ENCODE(DIGEST( "hub_src"."lnd_camp_prod_hkey"::text || '#' || TO_CHAR("snapshotdates"."snapshot_timestamp",
			'DD/MM/YYYY HH24:MI:SS.US'::varchar) ,'MD5'),'HEX')) AS "pit_camp_prod_camp_prod_hkey"
		, "hub_src"."lnd_camp_prod_hkey" AS "lnd_camp_prod_hkey"
		, "snapshotdates"."snapshot_timestamp" AS "snapshot_timestamp"
		, "bvlci_src"."load_cycle_id" AS "load_cycle_id"
		, COALESCE("sat_src1"."lnd_camp_prod_hkey","unsat_src1"."lnd_camp_prod_hkey") AS "lds_mm_camp_prod_class_hkey"
		, COALESCE("sat_src2"."lnd_camp_prod_hkey","unsat_src2"."lnd_camp_prod_hkey") AS "lds_mm_camp_prod_emo_hkey"
		, COALESCE("sat_src1"."load_date","unsat_src1"."load_date") AS "lds_mm_camp_prod_class_trans_timestamp"
		, COALESCE("sat_src2"."load_date","unsat_src2"."load_date") AS "lds_mm_camp_prod_emo_trans_timestamp"
	FROM "moto_scn01_fl"."lnd_camp_prod" "hub_src"
	INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
	INNER JOIN "moto_scn01_fmc"."load_cycle_info" "bvlci_src" ON  1 = 1
	INNER JOIN "moto_scn01_bv"."view_pit_camp_prod_camp_prod_snapshotdates" "snapshotdates" ON  "snapshotdates"."lnd_camp_prod_hkey" = "hub_src"."lnd_camp_prod_hkey"
	LEFT OUTER JOIN "sat_src1" "sat_src1" ON  "hub_src"."lnd_camp_prod_hkey" = "sat_src1"."lnd_camp_prod_hkey" AND "snapshotdates"."snapshot_timestamp" >= 
		"sat_src1"."load_date" AND "snapshotdates"."snapshot_timestamp" < "sat_src1"."load_end_date" AND "sat_src1"."delete_flag" != 'Y'::text
	LEFT OUTER JOIN "sat_src2" "sat_src2" ON  "hub_src"."lnd_camp_prod_hkey" = "sat_src2"."lnd_camp_prod_hkey" AND "snapshotdates"."snapshot_timestamp" >= 
		"sat_src2"."load_date" AND "snapshotdates"."snapshot_timestamp" < "sat_src2"."load_end_date" AND "sat_src2"."delete_flag" != 'Y'::text
	INNER JOIN "moto_scn01_fl"."lds_mm_camp_prod_class" "unsat_src1" ON  "mex_src"."load_cycle_id"::int = "unsat_src1"."load_cycle_id"
	INNER JOIN "moto_scn01_fl"."lds_mm_camp_prod_emo" "unsat_src2" ON  "mex_src"."load_cycle_id"::int = "unsat_src2"."load_cycle_id"
	WHERE  COALESCE("sat_src1"."load_date","unsat_src1"."load_date")<= "snapshotdates"."snapshot_timestamp" OR COALESCE("sat_src2"."load_date","unsat_src2"."load_date")<= "snapshotdates"."snapshot_timestamp"
	;
END;



END;
$function$;
 
 
