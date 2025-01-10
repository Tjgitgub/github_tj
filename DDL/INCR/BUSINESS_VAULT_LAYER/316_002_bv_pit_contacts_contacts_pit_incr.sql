CREATE OR REPLACE FUNCTION "moto_scn01_proc"."bv_pit_contacts_contacts_pit_incr"() 
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

BEGIN -- pit_upd

	WITH "snapshotdates_upd" AS 
	( 
		SELECT 
			  "ssdv_src_upd"."snapshot_timestamp" AS "snapshot_timestamp"
		FROM "moto_scn01_bv"."view_pit_contacts_contacts_snapshotdates" "ssdv_src_upd"
		INNER JOIN "moto_scn01_fmc"."fmc_bv_loading_window_table" "bvlwt_src_upd" ON  1 = 1
		WHERE  "ssdv_src_upd"."snapshot_timestamp" >= "bvlwt_src_upd"."fmc_begin_lw_timestamp"
	)
	, "sat_src_upd2" AS 
	( 
		SELECT 
			  "sat_ed_src_upd2"."contacts_hkey" AS "contacts_hkey"
			, "sat_ed_src_upd2"."trans_timestamp" AS "load_date"
			, COALESCE(LEAD("sat_ed_src_upd2"."trans_timestamp")OVER(PARTITION BY "sat_ed_src_upd2"."contacts_hkey" ORDER BY "sat_ed_src_upd2"."trans_timestamp")
				, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
			, "sat_ed_src_upd2"."delete_flag" AS "delete_flag"
		FROM "moto_scn01_fl"."sat_mm_e_mails" "sat_ed_src_upd2"
	)
	, "sat_src_upd3" AS 
	( 
		SELECT 
			  "sat_ed_src_upd3"."contacts_hkey" AS "contacts_hkey"
			, "sat_ed_src_upd3"."trans_timestamp" AS "load_date"
			, COALESCE(LEAD("sat_ed_src_upd3"."trans_timestamp")OVER(PARTITION BY "sat_ed_src_upd3"."contacts_hkey" ORDER BY "sat_ed_src_upd3"."trans_timestamp")
				, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
			, "sat_ed_src_upd3"."delete_flag" AS "delete_flag"
		FROM "moto_scn01_fl"."sat_mm_phones" "sat_ed_src_upd3"
	)
	, "sat_src_upd1" AS 
	( 
		SELECT 
			  "sat_ed_src_upd1"."contacts_hkey" AS "contacts_hkey"
			, "sat_ed_src_upd1"."trans_timestamp" AS "load_date"
			, COALESCE(LEAD("sat_ed_src_upd1"."trans_timestamp")OVER(PARTITION BY "sat_ed_src_upd1"."contacts_hkey" ORDER BY "sat_ed_src_upd1"."trans_timestamp")
				, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
			, "sat_ed_src_upd1"."delete_flag" AS "delete_flag"
		FROM "moto_scn01_fl"."sat_mm_contacts" "sat_ed_src_upd1"
	)
	, "miv_upd" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST( "hub_src_upd"."contacts_hkey"::text || '#' || TO_CHAR("snapshotdates_upd"."snapshot_timestamp",
				 'DD/MM/YYYY HH24:MI:SS.US'::varchar) ,'MD5'),'HEX')) AS "pit_contacts_contacts_hkey"
			, "hub_src_upd"."contacts_hkey" AS "contacts_hkey"
			, "snapshotdates_upd"."snapshot_timestamp" AS "snapshot_timestamp"
			, "bvlci_upd"."load_cycle_id" AS "load_cycle_id"
			, COALESCE("sat_src_upd1"."contacts_hkey","unsat_src_upd1"."contacts_hkey") AS "sat_mm_contacts_hkey"
			, COALESCE("sat_src_upd2"."contacts_hkey","unsat_src_upd2"."contacts_hkey") AS "sat_mm_e_mails_hkey"
			, COALESCE("sat_src_upd3"."contacts_hkey","unsat_src_upd3"."contacts_hkey") AS "sat_mm_phones_hkey"
			, COALESCE("sat_src_upd1"."load_date","unsat_src_upd1"."load_date") AS "sat_mm_contacts_trans_timestamp"
			, COALESCE("sat_src_upd2"."load_date","unsat_src_upd2"."load_date") AS "sat_mm_e_mails_trans_timestamp"
			, COALESCE("sat_src_upd3"."load_date","unsat_src_upd3"."load_date") AS "sat_mm_phones_trans_timestamp"
		FROM "moto_scn01_fl"."hub_contacts" "hub_src_upd"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src_upd" ON  "mex_src_upd"."record_type" = 'U'
		INNER JOIN "moto_scn01_fmc"."load_cycle_info" "bvlci_upd" ON  1 = 1
		INNER JOIN "snapshotdates_upd" "snapshotdates_upd" ON  1 = 1
		LEFT OUTER JOIN "sat_src_upd1" "sat_src_upd1" ON  "hub_src_upd"."contacts_hkey" = "sat_src_upd1"."contacts_hkey" AND "snapshotdates_upd"."snapshot_timestamp" >=
			 "sat_src_upd1"."load_date" AND "snapshotdates_upd"."snapshot_timestamp" < "sat_src_upd1"."load_end_date" AND "sat_src_upd1"."delete_flag" != 'Y'::text
		LEFT OUTER JOIN "sat_src_upd2" "sat_src_upd2" ON  "hub_src_upd"."contacts_hkey" = "sat_src_upd2"."contacts_hkey" AND "snapshotdates_upd"."snapshot_timestamp" >=
			 "sat_src_upd2"."load_date" AND "snapshotdates_upd"."snapshot_timestamp" < "sat_src_upd2"."load_end_date" AND "sat_src_upd2"."delete_flag" != 'Y'::text
		LEFT OUTER JOIN "sat_src_upd3" "sat_src_upd3" ON  "hub_src_upd"."contacts_hkey" = "sat_src_upd3"."contacts_hkey" AND "snapshotdates_upd"."snapshot_timestamp" >=
			 "sat_src_upd3"."load_date" AND "snapshotdates_upd"."snapshot_timestamp" < "sat_src_upd3"."load_end_date" AND "sat_src_upd3"."delete_flag" != 'Y'::text
		INNER JOIN "moto_scn01_fl"."sat_mm_contacts" "unsat_src_upd1" ON  "mex_src_upd"."load_cycle_id"::int = "unsat_src_upd1"."load_cycle_id"
		INNER JOIN "moto_scn01_fl"."sat_mm_e_mails" "unsat_src_upd2" ON  "mex_src_upd"."load_cycle_id"::int = "unsat_src_upd2"."load_cycle_id"
		INNER JOIN "moto_scn01_fl"."sat_mm_phones" "unsat_src_upd3" ON  "mex_src_upd"."load_cycle_id"::int = "unsat_src_upd3"."load_cycle_id"
	)
	, "upd_data_src" AS 
	( 
		SELECT 
			  "miv_upd"."pit_contacts_contacts_hkey" AS "pit_contacts_contacts_hkey"
			, "miv_upd"."contacts_hkey" AS "contacts_hkey"
			, "miv_upd"."snapshot_timestamp" AS "snapshot_timestamp"
			, "miv_upd"."load_cycle_id" AS "load_cycle_id"
			, "miv_upd"."sat_mm_contacts_hkey" AS "sat_mm_contacts_hkey"
			, "miv_upd"."sat_mm_e_mails_hkey" AS "sat_mm_e_mails_hkey"
			, "miv_upd"."sat_mm_phones_hkey" AS "sat_mm_phones_hkey"
			, "miv_upd"."sat_mm_contacts_trans_timestamp" AS "sat_mm_contacts_trans_timestamp"
			, "miv_upd"."sat_mm_e_mails_trans_timestamp" AS "sat_mm_e_mails_trans_timestamp"
			, "miv_upd"."sat_mm_phones_trans_timestamp" AS "sat_mm_phones_trans_timestamp"
		FROM "miv_upd" "miv_upd"
		LEFT OUTER JOIN "moto_scn01_bv"."pit_contacts_contacts" "pit_src_upd" ON  "miv_upd"."pit_contacts_contacts_hkey" = "pit_src_upd"."pit_contacts_contacts_hkey" AND "miv_upd"."sat_mm_contacts_hkey" =
			 "pit_src_upd"."sat_mm_contacts_hkey" AND "miv_upd"."sat_mm_e_mails_hkey" = "pit_src_upd"."sat_mm_e_mails_hkey" AND "miv_upd"."sat_mm_phones_hkey" = "pit_src_upd"."sat_mm_phones_hkey" AND "miv_upd"."sat_mm_contacts_trans_timestamp" = "pit_src_upd"."sat_mm_contacts_trans_timestamp" AND "miv_upd"."sat_mm_e_mails_trans_timestamp" = "pit_src_upd"."sat_mm_e_mails_trans_timestamp" AND "miv_upd"."sat_mm_phones_trans_timestamp" = "pit_src_upd"."sat_mm_phones_trans_timestamp"
		WHERE  "pit_src_upd"."pit_contacts_contacts_hkey" IS NULL
	)
	UPDATE "moto_scn01_bv"."pit_contacts_contacts" "pit_upd_tgt"
	SET 
		 "sat_mm_contacts_hkey" =  "upd_data_src"."sat_mm_contacts_hkey"
		,"sat_mm_e_mails_hkey" =  "upd_data_src"."sat_mm_e_mails_hkey"
		,"sat_mm_phones_hkey" =  "upd_data_src"."sat_mm_phones_hkey"
		,"sat_mm_contacts_trans_timestamp" =  "upd_data_src"."sat_mm_contacts_trans_timestamp"
		,"sat_mm_e_mails_trans_timestamp" =  "upd_data_src"."sat_mm_e_mails_trans_timestamp"
		,"sat_mm_phones_trans_timestamp" =  "upd_data_src"."sat_mm_phones_trans_timestamp"
		,"load_cycle_id" =  "upd_data_src"."load_cycle_id"
	FROM  "upd_data_src"
	WHERE "pit_upd_tgt"."pit_contacts_contacts_hkey" =  "upd_data_src"."pit_contacts_contacts_hkey"
	;
END;


BEGIN -- pit_tgt

	INSERT INTO "moto_scn01_bv"."pit_contacts_contacts"(
		 "pit_contacts_contacts_hkey"
		,"contacts_hkey"
		,"snapshot_timestamp"
		,"load_cycle_id"
		,"sat_mm_contacts_hkey"
		,"sat_mm_e_mails_hkey"
		,"sat_mm_phones_hkey"
		,"sat_mm_contacts_trans_timestamp"
		,"sat_mm_e_mails_trans_timestamp"
		,"sat_mm_phones_trans_timestamp"
	)
	WITH "snapshotdates" AS 
	( 
		SELECT 
			  "ssdv_src"."snapshot_timestamp" AS "snapshot_timestamp"
		FROM "moto_scn01_bv"."view_pit_contacts_contacts_snapshotdates" "ssdv_src"
		INNER JOIN "moto_scn01_fmc"."fmc_bv_loading_window_table" "bvlwt_src" ON  1 = 1
		WHERE  "ssdv_src"."snapshot_timestamp" >= "bvlwt_src"."fmc_begin_lw_timestamp"
	)
	, "sat_src2" AS 
	( 
		SELECT 
			  "sat_ed_src2"."contacts_hkey" AS "contacts_hkey"
			, "sat_ed_src2"."trans_timestamp" AS "load_date"
			, COALESCE(LEAD("sat_ed_src2"."trans_timestamp")OVER(PARTITION BY "sat_ed_src2"."contacts_hkey" ORDER BY "sat_ed_src2"."trans_timestamp")
				, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
			, "sat_ed_src2"."delete_flag" AS "delete_flag"
		FROM "moto_scn01_fl"."sat_mm_e_mails" "sat_ed_src2"
	)
	, "sat_src3" AS 
	( 
		SELECT 
			  "sat_ed_src3"."contacts_hkey" AS "contacts_hkey"
			, "sat_ed_src3"."trans_timestamp" AS "load_date"
			, COALESCE(LEAD("sat_ed_src3"."trans_timestamp")OVER(PARTITION BY "sat_ed_src3"."contacts_hkey" ORDER BY "sat_ed_src3"."trans_timestamp")
				, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
			, "sat_ed_src3"."delete_flag" AS "delete_flag"
		FROM "moto_scn01_fl"."sat_mm_phones" "sat_ed_src3"
	)
	, "sat_src1" AS 
	( 
		SELECT 
			  "sat_ed_src1"."contacts_hkey" AS "contacts_hkey"
			, "sat_ed_src1"."trans_timestamp" AS "load_date"
			, COALESCE(LEAD("sat_ed_src1"."trans_timestamp")OVER(PARTITION BY "sat_ed_src1"."contacts_hkey" ORDER BY "sat_ed_src1"."trans_timestamp")
				, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
			, "sat_ed_src1"."delete_flag" AS "delete_flag"
		FROM "moto_scn01_fl"."sat_mm_contacts" "sat_ed_src1"
	)
	, "miv" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST( "hub_src"."contacts_hkey"::text || '#' || TO_CHAR("snapshotdates"."snapshot_timestamp",
				 'DD/MM/YYYY HH24:MI:SS.US'::varchar) ,'MD5'),'HEX')) AS "pit_contacts_contacts_hkey"
			, "hub_src"."contacts_hkey" AS "contacts_hkey"
			, "snapshotdates"."snapshot_timestamp" AS "snapshot_timestamp"
			, "bvlci_src"."load_cycle_id" AS "load_cycle_id"
			, COALESCE("sat_src1"."contacts_hkey","unsat_src1"."contacts_hkey") AS "sat_mm_contacts_hkey"
			, COALESCE("sat_src2"."contacts_hkey","unsat_src2"."contacts_hkey") AS "sat_mm_e_mails_hkey"
			, COALESCE("sat_src3"."contacts_hkey","unsat_src3"."contacts_hkey") AS "sat_mm_phones_hkey"
			, COALESCE("sat_src1"."load_date","unsat_src1"."load_date") AS "sat_mm_contacts_trans_timestamp"
			, COALESCE("sat_src2"."load_date","unsat_src2"."load_date") AS "sat_mm_e_mails_trans_timestamp"
			, COALESCE("sat_src3"."load_date","unsat_src3"."load_date") AS "sat_mm_phones_trans_timestamp"
		FROM "moto_scn01_fl"."hub_contacts" "hub_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
		INNER JOIN "moto_scn01_fmc"."load_cycle_info" "bvlci_src" ON  1 = 1
		INNER JOIN "snapshotdates" "snapshotdates" ON  1 = 1
		LEFT OUTER JOIN "sat_src1" "sat_src1" ON  "hub_src"."contacts_hkey" = "sat_src1"."contacts_hkey" AND "snapshotdates"."snapshot_timestamp" >= "sat_src1"."load_date" AND 
			"snapshotdates"."snapshot_timestamp" < "sat_src1"."load_end_date" AND "sat_src1"."delete_flag" != 'Y'::text
		LEFT OUTER JOIN "sat_src2" "sat_src2" ON  "hub_src"."contacts_hkey" = "sat_src2"."contacts_hkey" AND "snapshotdates"."snapshot_timestamp" >= "sat_src2"."load_date" AND 
			"snapshotdates"."snapshot_timestamp" < "sat_src2"."load_end_date" AND "sat_src2"."delete_flag" != 'Y'::text
		LEFT OUTER JOIN "sat_src3" "sat_src3" ON  "hub_src"."contacts_hkey" = "sat_src3"."contacts_hkey" AND "snapshotdates"."snapshot_timestamp" >= "sat_src3"."load_date" AND 
			"snapshotdates"."snapshot_timestamp" < "sat_src3"."load_end_date" AND "sat_src3"."delete_flag" != 'Y'::text
		INNER JOIN "moto_scn01_fl"."sat_mm_contacts" "unsat_src1" ON  "mex_src"."load_cycle_id"::int = "unsat_src1"."load_cycle_id"
		INNER JOIN "moto_scn01_fl"."sat_mm_e_mails" "unsat_src2" ON  "mex_src"."load_cycle_id"::int = "unsat_src2"."load_cycle_id"
		INNER JOIN "moto_scn01_fl"."sat_mm_phones" "unsat_src3" ON  "mex_src"."load_cycle_id"::int = "unsat_src3"."load_cycle_id"
		WHERE  COALESCE("sat_src1"."load_date","unsat_src1"."load_date")<= "snapshotdates"."snapshot_timestamp" OR COALESCE("sat_src2"."load_date","unsat_src2"."load_date")<= "snapshotdates"."snapshot_timestamp" OR COALESCE("sat_src3"."load_date","unsat_src3"."load_date")<= "snapshotdates"."snapshot_timestamp"
	)
	, "data_src" AS 
	( 
		SELECT 
			  "miv"."pit_contacts_contacts_hkey" AS "pit_contacts_contacts_hkey"
			, "miv"."contacts_hkey" AS "contacts_hkey"
			, "miv"."snapshot_timestamp" AS "snapshot_timestamp"
			, "miv"."load_cycle_id" AS "load_cycle_id"
			, "miv"."sat_mm_contacts_hkey" AS "sat_mm_contacts_hkey"
			, "miv"."sat_mm_e_mails_hkey" AS "sat_mm_e_mails_hkey"
			, "miv"."sat_mm_phones_hkey" AS "sat_mm_phones_hkey"
			, "miv"."sat_mm_contacts_trans_timestamp" AS "sat_mm_contacts_trans_timestamp"
			, "miv"."sat_mm_e_mails_trans_timestamp" AS "sat_mm_e_mails_trans_timestamp"
			, "miv"."sat_mm_phones_trans_timestamp" AS "sat_mm_phones_trans_timestamp"
		FROM "miv" "miv"
		LEFT OUTER JOIN "moto_scn01_bv"."pit_contacts_contacts" "pit_src" ON  "miv"."pit_contacts_contacts_hkey" = "pit_src"."pit_contacts_contacts_hkey" AND "miv"."sat_mm_contacts_hkey" =
			 "pit_src"."sat_mm_contacts_hkey" AND "miv"."sat_mm_e_mails_hkey" = "pit_src"."sat_mm_e_mails_hkey" AND "miv"."sat_mm_phones_hkey" = "pit_src"."sat_mm_phones_hkey" AND "miv"."sat_mm_contacts_trans_timestamp" = "pit_src"."sat_mm_contacts_trans_timestamp" AND "miv"."sat_mm_e_mails_trans_timestamp" = "pit_src"."sat_mm_e_mails_trans_timestamp" AND "miv"."sat_mm_phones_trans_timestamp" = "pit_src"."sat_mm_phones_trans_timestamp"
		WHERE  "pit_src"."pit_contacts_contacts_hkey" IS NULL
	)
	SELECT 
		  "data_src"."pit_contacts_contacts_hkey" AS "pit_contacts_contacts_hkey"
		, "data_src"."contacts_hkey" AS "contacts_hkey"
		, "data_src"."snapshot_timestamp" AS "snapshot_timestamp"
		, "data_src"."load_cycle_id" AS "load_cycle_id"
		, "data_src"."sat_mm_contacts_hkey" AS "sat_mm_contacts_hkey"
		, "data_src"."sat_mm_e_mails_hkey" AS "sat_mm_e_mails_hkey"
		, "data_src"."sat_mm_phones_hkey" AS "sat_mm_phones_hkey"
		, "data_src"."sat_mm_contacts_trans_timestamp" AS "sat_mm_contacts_trans_timestamp"
		, "data_src"."sat_mm_e_mails_trans_timestamp" AS "sat_mm_e_mails_trans_timestamp"
		, "data_src"."sat_mm_phones_trans_timestamp" AS "sat_mm_phones_trans_timestamp"
	FROM "data_src" "data_src"
	;
END;



END;
$function$;
 
 
