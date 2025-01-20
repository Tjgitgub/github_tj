CREATE OR REPLACE FUNCTION "moto_scn01_proc"."las_mm_cust_addr_init"() 
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
	WITH "las_set" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST( "hub_pk_src"."src_bk" || '#' || "hub_pk_src"."customers_bk" || '#' || COALESCE("hub_fk_src"."street_name_bk",
				"unhub_fk_src"."street_name_bk")|| '#' ||  COALESCE("hub_fk_src"."street_number_bk","unhub_fk_src"."street_number_bk")|| '#' ||  COALESCE("hub_fk_src"."postal_code_bk","unhub_fk_src"."postal_code_bk")|| '#' ||  COALESCE("hub_fk_src"."city_bk","unhub_fk_src"."city_bk")|| '#' ,'MD5'),'HEX')) AS "lna_cust_addr_hkey"
			, COALESCE("hub_fk_src"."addresses_hkey","unhub_fk_src"."addresses_hkey") AS "addresses_hkey"
			, "hub_pk_src"."customers_hkey" AS "customers_hkey"
			, "sat_pk_src"."load_date" AS "load_date"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, CASE WHEN "mex_ex_src"."load_cycle_id" IS NULL THEN "bvlci_src"."load_cycle_id" ELSE "mex_ex_src"."load_cycle_id"::int END AS "load_cycle_id"
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
		INNER JOIN "moto_scn01_fmc"."load_cycle_info" "bvlci_src" ON  1 = 1
		LEFT OUTER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_ex_src" ON  "mex_ex_src"."load_cycle_id" = "hub_pk_src"."load_cycle_id" ::text
		WHERE  "mex_src"."record_type" = 'U'
	)
	SELECT 
		  "las_set"."lna_cust_addr_hkey" AS "lna_cust_addr_hkey"
		, "las_set"."addresses_hkey" AS "addresses_hkey"
		, "las_set"."customers_hkey" AS "customers_hkey"
		, "las_set"."load_date" AS "load_date"
		, "las_set"."load_cycle_id" AS "load_cycle_id"
		, "las_set"."load_end_date" AS "load_end_date"
		, '1' AS "source"
		, CASE WHEN "las_set"."delete_flag"::text || "las_set"."addresses_hkey"::text = LAG( "las_set"."delete_flag"::text || 
			"las_set"."addresses_hkey"::text,1)OVER(PARTITION BY "las_set"."customers_hkey" ORDER BY "las_set"."load_date")THEN 1 ELSE 0 END AS "equal"
		, "las_set"."delete_flag" AS "delete_flag"
		, "las_set"."trans_timestamp" AS "trans_timestamp"
		, "las_set"."party_number" AS "party_number"
		, "las_set"."address_number" AS "address_number"
	FROM "las_set" "las_set"
	;
END;


BEGIN -- las_inur_tgt

	TRUNCATE TABLE "moto_scn01_bv"."las_mm_cust_addr"  CASCADE;

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
		, COALESCE(LEAD("las_temp_src_inur"."load_date",1)OVER(PARTITION BY "las_temp_src_inur"."customers_hkey" ORDER BY "las_temp_src_inur"."load_date")
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
		, "las_temp_src_inur"."load_cycle_id" AS "load_cycle_id"
		, "las_temp_src_inur"."delete_flag" AS "delete_flag"
		, "las_temp_src_inur"."trans_timestamp" AS "trans_timestamp"
		, "las_temp_src_inur"."party_number" AS "party_number"
		, "las_temp_src_inur"."address_number" AS "address_number"
	FROM "moto_scn01_bv"."las_mm_cust_addr_tmp" "las_temp_src_inur"
	WHERE  "las_temp_src_inur"."equal" = 0
	;
END;



END;
$function$;
 
 
