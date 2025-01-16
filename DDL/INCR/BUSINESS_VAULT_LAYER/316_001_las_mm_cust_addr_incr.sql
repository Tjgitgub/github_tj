CREATE OR REPLACE FUNCTION "moto_scn01_proc"."las_mm_cust_addr_incr"() 
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

BEGIN -- las_temp_tgt

	TRUNCATE TABLE "moto_scn01_bv"."las_mm_cust_addr_tmp"  CASCADE;

	INSERT INTO "moto_scn01_bv"."las_mm_cust_addr_tmp"(
		 "lna_cust_addr_hkey"
		,"addresses_hkey"
		,"customers_hkey"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"source"
		,"equal"
		,"delete_flag"
		,"trans_timestamp"
		,"party_number"
		,"address_number"
	)
	WITH "dist_sat" AS 
	( 
		SELECT 
			  "sat_pk_dis"."customers_hkey" AS "customers_hkey"
			, MIN("sat_pk_dis"."load_date") AS "min_load_timestamp"
		FROM "moto_scn01_fl"."sat_mm_customers" "sat_pk_dis"
		INNER JOIN "moto_scn01_fmc"."dv_load_cycle_info" "dvlci_dis" ON  "dvlci_dis"."dv_load_cycle_id" = "sat_pk_dis"."load_cycle_id"
		GROUP BY  "sat_pk_dis"."customers_hkey"
	)
	, "last_las" AS 
	( 
		SELECT 
			  "last_lna_src"."customers_hkey" AS "customers_hkey"
			, MAX("last_las_src"."load_date") AS "max_load_timestamp"
		FROM "moto_scn01_bv"."las_mm_cust_addr" "last_las_src"
		INNER JOIN "moto_scn01_bv"."lna_cust_addr" "last_lna_src" ON  "last_las_src"."lna_cust_addr_hkey" = "last_lna_src"."lna_cust_addr_hkey"
		INNER JOIN "dist_sat" "dist_sat" ON  "last_lna_src"."customers_hkey" = "dist_sat"."customers_hkey"
		WHERE  "last_las_src"."load_date" <= "dist_sat"."min_load_timestamp"
		GROUP BY  "last_lna_src"."customers_hkey"
	)
	, "las_temp_set" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST( "hub_pk_src"."src_bk" || '#' || "hub_pk_src"."customers_bk" || '#' || COALESCE("hub_fk_src"."street_name_bk",
				"unhub_fk_src"."street_name_bk")|| '#' ||  COALESCE("hub_fk_src"."street_number_bk","unhub_fk_src"."street_number_bk")|| '#' ||  COALESCE("hub_fk_src"."postal_code_bk","unhub_fk_src"."postal_code_bk")|| '#' ||  COALESCE("hub_fk_src"."city_bk","unhub_fk_src"."city_bk")|| '#' ,'MD5'),'HEX')) AS "lna_cust_addr_hkey"
			, COALESCE("hub_fk_src"."addresses_hkey","unhub_fk_src"."addresses_hkey") AS "addresses_hkey"
			, "hub_pk_src"."customers_hkey" AS "customers_hkey"
			, "sat_pk_src"."load_date" AS "load_date"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, "bvlci_sat"."load_cycle_id" AS "load_cycle_id"
			, "sat_pk_src"."delete_flag" AS "delete_flag"
			, "sat_pk_src"."trans_timestamp" AS "trans_timestamp"
			, "sat_pk_src"."party_number" AS "party_number"
			, "sat_pk_src"."address_number" AS "address_number"
		FROM "moto_scn01_fl"."sat_mm_customers" "sat_pk_src"
		INNER JOIN "moto_scn01_fl"."hub_customers" "hub_pk_src" ON  "sat_pk_src"."customers_hkey" = "hub_pk_src"."customers_hkey"
		LEFT OUTER JOIN "moto_scn01_fl"."sat_ms_addresses" "sat_fk_src" ON  "sat_pk_src"."address_number" = "sat_fk_src"."address_number"
		LEFT OUTER JOIN "moto_scn01_fl"."hub_addresses" "hub_fk_src" ON  "sat_fk_src"."addresses_hkey" = "hub_fk_src"."addresses_hkey"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		INNER JOIN "moto_scn01_fl"."hub_addresses" "unhub_fk_src" ON  "mex_src"."load_cycle_id"::int = "unhub_fk_src"."load_cycle_id"
		INNER JOIN "moto_scn01_fl"."sat_ms_addresses" "unsat_fk_src" ON  "unsat_fk_src"."addresses_hkey" = "unhub_fk_src"."addresses_hkey"
		INNER JOIN "moto_scn01_fmc"."dv_load_cycle_info" "dvlci_sat" ON  "dvlci_sat"."dv_load_cycle_id" = "sat_pk_src"."load_cycle_id"
		INNER JOIN "moto_scn01_fmc"."load_cycle_info" "bvlci_sat" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'U'
	)
	, "las_ref" AS 
	( 
		SELECT DISTINCT 
 			  UPPER(ENCODE(DIGEST( "hub_set_pk_src"."src_bk" || '#' || "hub_set_pk_src"."customers_bk" || '#' || 
				"hub_set_fk_src"."street_name_bk" || '#' ||  "hub_set_fk_src"."street_number_bk" || '#' ||  "hub_set_fk_src"."postal_code_bk" || '#' ||  "hub_set_fk_src"."city_bk" || '#' ,'MD5'),'HEX')) AS "lna_cust_addr_hkey"
			, "hub_set_fk_src"."addresses_hkey" AS "addresses_hkey"
			, "hub_set_pk_src"."customers_hkey" AS "customers_hkey"
			, "las_ref_src"."load_date" AS "load_date"
			, "las_ref_src"."load_end_date" AS "load_end_date"
			, "bvlci_src"."load_cycle_id" AS "load_cycle_id"
			, "las_ref_src"."delete_flag" AS "delete_flag"
			, "las_ref_src"."trans_timestamp" AS "trans_timestamp"
			, "las_ref_src"."party_number" AS "party_number"
			, "las_ref_src"."address_number" AS "address_number"
		FROM "moto_scn01_bv"."las_mm_cust_addr" "las_ref_src"
		INNER JOIN "moto_scn01_bv"."lna_cust_addr" "lna_ref_src" ON  "las_ref_src"."lna_cust_addr_hkey" = "lna_ref_src"."lna_cust_addr_hkey"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_set_src" ON  1 = 1
		INNER JOIN "moto_scn01_fl"."hub_addresses" "unhub_set_fk_src" ON  "mex_set_src"."load_cycle_id"::int = "unhub_set_fk_src"."load_cycle_id" AND "lna_ref_src"."addresses_hkey" = 
			"unhub_set_fk_src"."addresses_hkey"
		INNER JOIN "moto_scn01_fl"."hub_customers" "hub_set_pk_src" ON  "hub_set_pk_src"."customers_hkey" = "lna_ref_src"."customers_hkey"
		INNER JOIN "moto_scn01_fl"."sat_ms_addresses" "sat_set_fk_src" ON  "sat_set_fk_src"."address_number" = "las_ref_src"."address_number"
		INNER JOIN "moto_scn01_fl"."hub_addresses" "hub_set_fk_src" ON  "hub_set_fk_src"."addresses_hkey" = "sat_set_fk_src"."addresses_hkey"
		INNER JOIN "moto_scn01_fmc"."dv_load_cycle_info" "dvlci_src" ON  "dvlci_src"."dv_load_cycle_id" = "sat_set_fk_src"."load_cycle_id"
		INNER JOIN "moto_scn01_fmc"."load_cycle_info" "bvlci_src" ON  1 = 1
		WHERE  "mex_set_src"."record_type" = 'U'
	)
	, "temp_table_set" AS 
	( 
		SELECT 
			  "las_temp_set"."lna_cust_addr_hkey" AS "lna_cust_addr_hkey"
			, "las_temp_set"."addresses_hkey" AS "addresses_hkey"
			, "las_temp_set"."customers_hkey" AS "customers_hkey"
			, "las_temp_set"."load_date" AS "load_date"
			, "las_temp_set"."load_end_date" AS "load_end_date"
			, "las_temp_set"."load_cycle_id" AS "load_cycle_id"
			, 1 AS "source"
			, "las_temp_set"."delete_flag" AS "delete_flag"
			, "las_temp_set"."trans_timestamp" AS "trans_timestamp"
			, "las_temp_set"."party_number" AS "party_number"
			, "las_temp_set"."address_number" AS "address_number"
		FROM "las_temp_set" "las_temp_set"
		UNION ALL 
		SELECT 
			  "las_src"."lna_cust_addr_hkey" AS "lna_cust_addr_hkey"
			, "lna_src"."addresses_hkey" AS "addresses_hkey"
			, "lna_src"."customers_hkey" AS "customers_hkey"
			, "las_src"."load_date" AS "load_date"
			, "las_src"."load_end_date" AS "load_end_date"
			, "las_src"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "source"
			, "las_src"."delete_flag" AS "delete_flag"
			, "las_src"."trans_timestamp" AS "trans_timestamp"
			, "las_src"."party_number" AS "party_number"
			, "las_src"."address_number" AS "address_number"
		FROM "moto_scn01_bv"."las_mm_cust_addr" "las_src"
		INNER JOIN "moto_scn01_bv"."lna_cust_addr" "lna_src" ON  "las_src"."lna_cust_addr_hkey" = "lna_src"."lna_cust_addr_hkey"
		INNER JOIN "last_las" "last_las" ON  "lna_src"."customers_hkey" = "last_las"."customers_hkey"
		WHERE  "las_src"."load_date" >= "last_las"."max_load_timestamp"
		UNION ALL 
		SELECT 
			  "las_ref"."lna_cust_addr_hkey" AS "lna_cust_addr_hkey"
			, "las_ref"."addresses_hkey" AS "addresses_hkey"
			, "las_ref"."customers_hkey" AS "customers_hkey"
			, "las_ref"."load_date" AS "load_date"
			, "las_ref"."load_end_date" AS "load_end_date"
			, "las_ref"."load_cycle_id" AS "load_cycle_id"
			, 3 AS "source"
			, "las_ref"."delete_flag" AS "delete_flag"
			, "las_ref"."trans_timestamp" AS "trans_timestamp"
			, "las_ref"."party_number" AS "party_number"
			, "las_ref"."address_number" AS "address_number"
		FROM "las_ref" "las_ref"
	)
	SELECT 
		  "temp_table_set"."lna_cust_addr_hkey" AS "lna_cust_addr_hkey"
		, "temp_table_set"."addresses_hkey" AS "addresses_hkey"
		, "temp_table_set"."customers_hkey" AS "customers_hkey"
		, "temp_table_set"."load_date" AS "load_date"
		, "temp_table_set"."load_cycle_id" AS "load_cycle_id"
		, "temp_table_set"."load_end_date" AS "load_end_date"
		, "temp_table_set"."source" ::text AS "source"
		, CASE WHEN "temp_table_set"."source" IN(1,3)AND "temp_table_set"."delete_flag"::text || "temp_table_set"."addresses_hkey"::text =
			LAG("temp_table_set"."delete_flag"::text || "temp_table_set"."addresses_hkey"::text,1)OVER(PARTITION BY "temp_table_set"."customers_hkey" ORDER BY "temp_table_set"."load_date","temp_table_set"."source")THEN 1 ELSE 0 END AS "equal"
		, "temp_table_set"."delete_flag" AS "delete_flag"
		, "temp_table_set"."trans_timestamp" AS "trans_timestamp"
		, "temp_table_set"."party_number" AS "party_number"
		, "temp_table_set"."address_number" AS "address_number"
	FROM "temp_table_set" "temp_table_set"
	;
END;


BEGIN -- las_inur_tgt

	INSERT INTO "moto_scn01_bv"."las_mm_cust_addr"(
		 "lna_cust_addr_hkey"
		,"customers_hkey"
		,"addresses_hkey"
		,"load_date"
		,"load_end_date"
		,"load_cycle_id"
		,"delete_flag"
		,"trans_timestamp"
		,"party_number"
		,"address_number"
	)
	SELECT 
		  "las_temp_src_inur"."lna_cust_addr_hkey" AS "lna_cust_addr_hkey"
		, "las_temp_src_inur"."customers_hkey" AS "customers_hkey"
		, "las_temp_src_inur"."addresses_hkey" AS "addresses_hkey"
		, "las_temp_src_inur"."load_date" AS "load_date"
		, "las_temp_src_inur"."load_end_date" AS "load_end_date"
		, "las_temp_src_inur"."load_cycle_id" AS "load_cycle_id"
		, "las_temp_src_inur"."delete_flag" AS "delete_flag"
		, "las_temp_src_inur"."trans_timestamp" AS "trans_timestamp"
		, "las_temp_src_inur"."party_number" AS "party_number"
		, "las_temp_src_inur"."address_number" AS "address_number"
	FROM "moto_scn01_bv"."las_mm_cust_addr_tmp" "las_temp_src_inur"
	WHERE  "las_temp_src_inur"."source" = '1' AND "las_temp_src_inur"."equal" = 0
	;
END;


BEGIN -- las_ed_tgt

	WITH "calc_duplicates" AS 
	( 
		SELECT 
			  "las_temp_src_dup"."lna_cust_addr_hkey" AS "lna_cust_addr_hkey"
			, "las_temp_src_dup"."customers_hkey" AS "customers_hkey"
			, "las_temp_src_dup"."addresses_hkey" AS "addresses_hkey"
			, "las_temp_src_dup"."load_date" AS "load_date"
			, "las_temp_src_dup"."delete_flag" AS "delete_flag"
			, "las_temp_src_dup"."trans_timestamp" AS "trans_timestamp"
			, ROW_NUMBER()OVER(PARTITION BY "las_temp_src_dup"."customers_hkey","las_temp_src_dup"."load_date" ORDER BY CASE WHEN "las_temp_src_dup"."source" =
				'1' THEN 1 WHEN "las_temp_src_dup"."source" = '3' THEN 2 ELSE 3 END) AS "dummy"
			, "las_temp_src_dup"."source" AS "source"
		FROM "moto_scn01_bv"."las_mm_cust_addr_tmp" "las_temp_src_dup"
		WHERE  "las_temp_src_dup"."equal" = 0
	)
	, "calc_load_end_date" AS 
	( 
		SELECT 
			  "calc_duplicates"."lna_cust_addr_hkey" AS "lna_cust_addr_hkey"
			, "calc_duplicates"."customers_hkey" AS "customers_hkey"
			, "calc_duplicates"."addresses_hkey" AS "addresses_hkey"
			, "calc_duplicates"."load_date" AS "load_date"
			, "lci_src"."load_cycle_id" AS "load_cycle_id"
			, COALESCE(LEAD("calc_duplicates"."load_date",1)OVER(PARTITION BY "calc_duplicates"."customers_hkey" ORDER BY "calc_duplicates"."load_date")
				, TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
			, "calc_duplicates"."delete_flag" AS "delete_flag"
			, "calc_duplicates"."trans_timestamp" AS "trans_timestamp"
			, "calc_duplicates"."source" AS "source"
		FROM "calc_duplicates" "calc_duplicates"
		INNER JOIN "moto_scn01_fmc"."load_cycle_info" "lci_src" ON  1 = 1
		WHERE  "calc_duplicates"."dummy" = 1
	)
	, "filter_changes" AS 
	( 
		SELECT 
			  "calc_load_end_date"."lna_cust_addr_hkey" AS "lna_cust_addr_hkey"
			, "calc_load_end_date"."customers_hkey" AS "customers_hkey"
			, "calc_load_end_date"."addresses_hkey" AS "addresses_hkey"
			, "calc_load_end_date"."load_date" AS "load_date"
			, "calc_load_end_date"."load_end_date" AS "load_end_date"
			, "calc_load_end_date"."load_cycle_id" AS "load_cycle_id"
			, "calc_load_end_date"."delete_flag" AS "delete_flag"
			, "calc_load_end_date"."trans_timestamp" AS "trans_timestamp"
		FROM "calc_load_end_date" "calc_load_end_date"
		WHERE  "calc_load_end_date"."source" = '3' OR( "calc_load_end_date"."load_end_date" != TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar))
	)
	UPDATE "moto_scn01_bv"."las_mm_cust_addr" "las_ed_tgt"
	SET 
		 "lna_cust_addr_hkey" =  "filter_changes"."lna_cust_addr_hkey"
		,"addresses_hkey" =  "filter_changes"."addresses_hkey"
		,"load_end_date" =  "filter_changes"."load_end_date"
		,"load_cycle_id" =  "filter_changes"."load_cycle_id"
	FROM  "filter_changes"
	WHERE "las_ed_tgt"."customers_hkey" =  "filter_changes"."customers_hkey"
	  AND "las_ed_tgt"."load_date" =  "filter_changes"."load_date"
	;
END;



END;
$function$;
 
 
