CREATE OR REPLACE FUNCTION "moto_scn01_proc"."bv_pit_camp_prod_camp_prod_pit_incr"() 
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
			  "ssdv_src_upd"."lnd_camp_prod_hkey" AS "lnd_camp_prod_hkey"
			, "ssdv_src_upd"."snapshot_timestamp" AS "snapshot_timestamp"
		FROM "moto_scn01_bv"."view_pit_camp_prod_camp_prod_snapshotdates" "ssdv_src_upd"
		INNER JOIN "moto_scn01_fmc"."fmc_bv_loading_window_table" "bvlwt_src_upd" ON  1 = 1
		WHERE  "ssdv_src_upd"."snapshot_timestamp" >= "bvlwt_src_upd"."fmc_begin_lw_timestamp"
	)
	, "sat_src_upd1" AS 
	( 
		SELECT 
			  "sat_ed_src_upd1"."lnd_camp_prod_hkey" AS "lnd_camp_prod_hkey"
			, "sat_ed_src_upd1"."trans_timestamp" AS "load_date"
			, COALESCE(LEAD("sat_ed_src_upd1"."trans_timestamp")OVER(PARTITION BY "sat_ed_src_upd1"."lnd_camp_prod_hkey" ORDER BY "sat_ed_src_upd1"."trans_timestamp")
				, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
			, "sat_ed_src_upd1"."delete_flag" AS "delete_flag"
		FROM "moto_scn01_fl"."lds_mm_camp_prod_class" "sat_ed_src_upd1"
	)
	, "sat_src_upd2" AS 
	( 
		SELECT 
			  "sat_ed_src_upd2"."lnd_camp_prod_hkey" AS "lnd_camp_prod_hkey"
			, "sat_ed_src_upd2"."trans_timestamp" AS "load_date"
			, COALESCE(LEAD("sat_ed_src_upd2"."trans_timestamp")OVER(PARTITION BY "sat_ed_src_upd2"."lnd_camp_prod_hkey" ORDER BY "sat_ed_src_upd2"."trans_timestamp")
				, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
			, "sat_ed_src_upd2"."delete_flag" AS "delete_flag"
		FROM "moto_scn01_fl"."lds_mm_camp_prod_emo" "sat_ed_src_upd2"
	)
	, "miv_upd" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST( "hub_src_upd"."lnd_camp_prod_hkey"::text || '#' || TO_CHAR("snapshotdates_upd"."snapshot_timestamp",
				'DD/MM/YYYY HH24:MI:SS.US'::varchar) ,'MD5'),'HEX')) AS "pit_camp_prod_camp_prod_hkey"
			, "hub_src_upd"."lnd_camp_prod_hkey" AS "lnd_camp_prod_hkey"
			, "snapshotdates_upd"."snapshot_timestamp" AS "snapshot_timestamp"
			, "bvlci_upd"."load_cycle_id" AS "load_cycle_id"
			, COALESCE("sat_src_upd1"."lnd_camp_prod_hkey","unsat_src_upd1"."lnd_camp_prod_hkey") AS "lds_mm_camp_prod_class_hkey"
			, COALESCE("sat_src_upd2"."lnd_camp_prod_hkey","unsat_src_upd2"."lnd_camp_prod_hkey") AS "lds_mm_camp_prod_emo_hkey"
			, COALESCE("sat_src_upd1"."load_date","unsat_src_upd1"."load_date") AS "lds_mm_camp_prod_class_trans_timestamp"
			, COALESCE("sat_src_upd2"."load_date","unsat_src_upd2"."load_date") AS "lds_mm_camp_prod_emo_trans_timestamp"
		FROM "moto_scn01_fl"."lnd_camp_prod" "hub_src_upd"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src_upd" ON  "mex_src_upd"."record_type" = 'U'
		INNER JOIN "moto_scn01_fmc"."load_cycle_info" "bvlci_upd" ON  1 = 1
		INNER JOIN "snapshotdates_upd" "snapshotdates_upd" ON  "snapshotdates_upd"."lnd_camp_prod_hkey" = "hub_src_upd"."lnd_camp_prod_hkey"
		LEFT OUTER JOIN "sat_src_upd1" "sat_src_upd1" ON  "hub_src_upd"."lnd_camp_prod_hkey" = "sat_src_upd1"."lnd_camp_prod_hkey" AND "snapshotdates_upd"."snapshot_timestamp" >=
			"sat_src_upd1"."load_date" AND "snapshotdates_upd"."snapshot_timestamp" < "sat_src_upd1"."load_end_date" AND "sat_src_upd1"."delete_flag" != 'Y'::text
		LEFT OUTER JOIN "sat_src_upd2" "sat_src_upd2" ON  "hub_src_upd"."lnd_camp_prod_hkey" = "sat_src_upd2"."lnd_camp_prod_hkey" AND "snapshotdates_upd"."snapshot_timestamp" >=
			"sat_src_upd2"."load_date" AND "snapshotdates_upd"."snapshot_timestamp" < "sat_src_upd2"."load_end_date" AND "sat_src_upd2"."delete_flag" != 'Y'::text
		INNER JOIN "moto_scn01_fl"."lds_mm_camp_prod_class" "unsat_src_upd1" ON  "mex_src_upd"."load_cycle_id"::int = "unsat_src_upd1"."load_cycle_id"
		INNER JOIN "moto_scn01_fl"."lds_mm_camp_prod_emo" "unsat_src_upd2" ON  "mex_src_upd"."load_cycle_id"::int = "unsat_src_upd2"."load_cycle_id"
	)
	, "upd_data_src" AS 
	( 
		SELECT 
			  "miv_upd"."pit_camp_prod_camp_prod_hkey" AS "pit_camp_prod_camp_prod_hkey"
			, "miv_upd"."lnd_camp_prod_hkey" AS "lnd_camp_prod_hkey"
			, "miv_upd"."snapshot_timestamp" AS "snapshot_timestamp"
			, "miv_upd"."load_cycle_id" AS "load_cycle_id"
			, "miv_upd"."lds_mm_camp_prod_class_hkey" AS "lds_mm_camp_prod_class_hkey"
			, "miv_upd"."lds_mm_camp_prod_emo_hkey" AS "lds_mm_camp_prod_emo_hkey"
			, "miv_upd"."lds_mm_camp_prod_class_trans_timestamp" AS "lds_mm_camp_prod_class_trans_timestamp"
			, "miv_upd"."lds_mm_camp_prod_emo_trans_timestamp" AS "lds_mm_camp_prod_emo_trans_timestamp"
		FROM "miv_upd" "miv_upd"
		LEFT OUTER JOIN "moto_scn01_bv"."pit_camp_prod_camp_prod" "pit_src_upd" ON  "miv_upd"."pit_camp_prod_camp_prod_hkey" = "pit_src_upd"."pit_camp_prod_camp_prod_hkey" AND "miv_upd"."lds_mm_camp_prod_class_hkey" =
			"pit_src_upd"."lds_mm_camp_prod_class_hkey" AND "miv_upd"."lds_mm_camp_prod_emo_hkey" = "pit_src_upd"."lds_mm_camp_prod_emo_hkey" AND "miv_upd"."lds_mm_camp_prod_class_trans_timestamp" = "pit_src_upd"."lds_mm_camp_prod_class_trans_timestamp" AND "miv_upd"."lds_mm_camp_prod_emo_trans_timestamp" = "pit_src_upd"."lds_mm_camp_prod_emo_trans_timestamp"
		WHERE  "pit_src_upd"."pit_camp_prod_camp_prod_hkey" IS NULL
	)
	UPDATE "moto_scn01_bv"."pit_camp_prod_camp_prod" "pit_upd_tgt"
	SET 
		 "lds_mm_camp_prod_class_hkey" =  "upd_data_src"."lds_mm_camp_prod_class_hkey"
		,"lds_mm_camp_prod_emo_hkey" =  "upd_data_src"."lds_mm_camp_prod_emo_hkey"
		,"lds_mm_camp_prod_class_trans_timestamp" =  "upd_data_src"."lds_mm_camp_prod_class_trans_timestamp"
		,"lds_mm_camp_prod_emo_trans_timestamp" =  "upd_data_src"."lds_mm_camp_prod_emo_trans_timestamp"
		,"load_cycle_id" =  "upd_data_src"."load_cycle_id"
	FROM  "upd_data_src"
	WHERE "pit_upd_tgt"."pit_camp_prod_camp_prod_hkey" =  "upd_data_src"."pit_camp_prod_camp_prod_hkey"
	;
END;


BEGIN -- pit_tgt

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
	WITH "snapshotdates" AS 
	( 
		SELECT 
			  "ssdv_src"."lnd_camp_prod_hkey" AS "lnd_camp_prod_hkey"
			, "ssdv_src"."snapshot_timestamp" AS "snapshot_timestamp"
		FROM "moto_scn01_bv"."view_pit_camp_prod_camp_prod_snapshotdates" "ssdv_src"
		INNER JOIN "moto_scn01_fmc"."fmc_bv_loading_window_table" "bvlwt_src" ON  1 = 1
		WHERE  "ssdv_src"."snapshot_timestamp" >= "bvlwt_src"."fmc_begin_lw_timestamp"
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
	, "sat_src2" AS 
	( 
		SELECT 
			  "sat_ed_src2"."lnd_camp_prod_hkey" AS "lnd_camp_prod_hkey"
			, "sat_ed_src2"."trans_timestamp" AS "load_date"
			, COALESCE(LEAD("sat_ed_src2"."trans_timestamp")OVER(PARTITION BY "sat_ed_src2"."lnd_camp_prod_hkey" ORDER BY "sat_ed_src2"."trans_timestamp")
				, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
			, "sat_ed_src2"."delete_flag" AS "delete_flag"
		FROM "moto_scn01_fl"."lds_mm_camp_prod_emo" "sat_ed_src2"
	)
	, "miv" AS 
	( 
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
		INNER JOIN "snapshotdates" "snapshotdates" ON  "snapshotdates"."lnd_camp_prod_hkey" = "hub_src"."lnd_camp_prod_hkey"
		LEFT OUTER JOIN "sat_src1" "sat_src1" ON  "hub_src"."lnd_camp_prod_hkey" = "sat_src1"."lnd_camp_prod_hkey" AND "snapshotdates"."snapshot_timestamp" >= 
			"sat_src1"."load_date" AND "snapshotdates"."snapshot_timestamp" < "sat_src1"."load_end_date" AND "sat_src1"."delete_flag" != 'Y'::text
		LEFT OUTER JOIN "sat_src2" "sat_src2" ON  "hub_src"."lnd_camp_prod_hkey" = "sat_src2"."lnd_camp_prod_hkey" AND "snapshotdates"."snapshot_timestamp" >= 
			"sat_src2"."load_date" AND "snapshotdates"."snapshot_timestamp" < "sat_src2"."load_end_date" AND "sat_src2"."delete_flag" != 'Y'::text
		INNER JOIN "moto_scn01_fl"."lds_mm_camp_prod_class" "unsat_src1" ON  "mex_src"."load_cycle_id"::int = "unsat_src1"."load_cycle_id"
		INNER JOIN "moto_scn01_fl"."lds_mm_camp_prod_emo" "unsat_src2" ON  "mex_src"."load_cycle_id"::int = "unsat_src2"."load_cycle_id"
		WHERE  COALESCE("sat_src1"."load_date","unsat_src1"."load_date")<= "snapshotdates"."snapshot_timestamp" OR COALESCE("sat_src2"."load_date","unsat_src2"."load_date")<= "snapshotdates"."snapshot_timestamp"
	)
	, "data_src" AS 
	( 
		SELECT 
			  "miv"."pit_camp_prod_camp_prod_hkey" AS "pit_camp_prod_camp_prod_hkey"
			, "miv"."lnd_camp_prod_hkey" AS "lnd_camp_prod_hkey"
			, "miv"."snapshot_timestamp" AS "snapshot_timestamp"
			, "miv"."load_cycle_id" AS "load_cycle_id"
			, "miv"."lds_mm_camp_prod_class_hkey" AS "lds_mm_camp_prod_class_hkey"
			, "miv"."lds_mm_camp_prod_emo_hkey" AS "lds_mm_camp_prod_emo_hkey"
			, "miv"."lds_mm_camp_prod_class_trans_timestamp" AS "lds_mm_camp_prod_class_trans_timestamp"
			, "miv"."lds_mm_camp_prod_emo_trans_timestamp" AS "lds_mm_camp_prod_emo_trans_timestamp"
		FROM "miv" "miv"
		LEFT OUTER JOIN "moto_scn01_bv"."pit_camp_prod_camp_prod" "pit_src" ON  "miv"."pit_camp_prod_camp_prod_hkey" = "pit_src"."pit_camp_prod_camp_prod_hkey" AND "miv"."lds_mm_camp_prod_class_hkey" =
			"pit_src"."lds_mm_camp_prod_class_hkey" AND "miv"."lds_mm_camp_prod_emo_hkey" = "pit_src"."lds_mm_camp_prod_emo_hkey" AND "miv"."lds_mm_camp_prod_class_trans_timestamp" = "pit_src"."lds_mm_camp_prod_class_trans_timestamp" AND "miv"."lds_mm_camp_prod_emo_trans_timestamp" = "pit_src"."lds_mm_camp_prod_emo_trans_timestamp"
		WHERE  "pit_src"."pit_camp_prod_camp_prod_hkey" IS NULL
	)
	SELECT 
		  "data_src"."pit_camp_prod_camp_prod_hkey" AS "pit_camp_prod_camp_prod_hkey"
		, "data_src"."lnd_camp_prod_hkey" AS "lnd_camp_prod_hkey"
		, "data_src"."snapshot_timestamp" AS "snapshot_timestamp"
		, "data_src"."load_cycle_id" AS "load_cycle_id"
		, "data_src"."lds_mm_camp_prod_class_hkey" AS "lds_mm_camp_prod_class_hkey"
		, "data_src"."lds_mm_camp_prod_emo_hkey" AS "lds_mm_camp_prod_emo_hkey"
		, "data_src"."lds_mm_camp_prod_class_trans_timestamp" AS "lds_mm_camp_prod_class_trans_timestamp"
		, "data_src"."lds_mm_camp_prod_emo_trans_timestamp" AS "lds_mm_camp_prod_emo_trans_timestamp"
	FROM "data_src" "data_src"
	;
END;



END;
$function$;
 
 
