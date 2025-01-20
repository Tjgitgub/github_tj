CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lna_cust_addr_incr"() 
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

BEGIN -- lna_tgt

	INSERT INTO "moto_scn01_bv"."lna_cust_addr"(
		 "lna_cust_addr_hkey"
		,"customers_hkey"
		,"addresses_hkey"
		,"load_cycle_id"
		,"load_date"
	)
	WITH "change_set" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST( "hub_pk_src_p1"."src_bk" || '#' || "hub_pk_src_p1"."customers_bk" || '#' || COALESCE("hub_fk_src_p1"."street_name_bk",
				"unhub_fk_src_p1"."street_name_bk")|| '#' ||  COALESCE("hub_fk_src_p1"."street_number_bk","unhub_fk_src_p1"."street_number_bk")|| '#' ||  COALESCE("hub_fk_src_p1"."postal_code_bk","unhub_fk_src_p1"."postal_code_bk")|| '#' ||  COALESCE("hub_fk_src_p1"."city_bk","unhub_fk_src_p1"."city_bk")|| '#' ,'MD5'),'HEX')) AS "lna_cust_addr_hkey"
			, "sat_pk_src_p1"."customers_hkey" AS "customers_hkey"
			, COALESCE("hub_fk_src_p1"."addresses_hkey","unhub_fk_src_p1"."addresses_hkey") AS "addresses_hkey"
			, CASE WHEN "mex_ex_src_p1"."load_cycle_id" IS NULL THEN "bvlci_src_p1"."load_cycle_id" ELSE "mex_ex_src_p1"."load_cycle_id"::int END AS "load_cycle_id"
			, "bvlci_src_p1"."load_date" AS "load_date"
		FROM "moto_scn01_fl"."sat_mm_customers" "sat_pk_src_p1"
		INNER JOIN "moto_scn01_fl"."hub_customers" "hub_pk_src_p1" ON  "sat_pk_src_p1"."customers_hkey" = "hub_pk_src_p1"."customers_hkey"
		LEFT OUTER JOIN "moto_scn01_fl"."sat_ms_addresses" "sat_fk_src_p1" ON  "sat_fk_src_p1"."address_number" = "sat_pk_src_p1"."address_number"
		LEFT OUTER JOIN "moto_scn01_fl"."hub_addresses" "hub_fk_src_p1" ON  "sat_fk_src_p1"."addresses_hkey" = "hub_fk_src_p1"."addresses_hkey"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src_p1" ON  1 = 1
		INNER JOIN "moto_scn01_fl"."hub_addresses" "unhub_fk_src_p1" ON  "mex_src_p1"."load_cycle_id"::int = "unhub_fk_src_p1"."load_cycle_id"
		INNER JOIN "moto_scn01_fl"."sat_ms_addresses" "unsat_fk_src_p1" ON  "unsat_fk_src_p1"."addresses_hkey" = "unhub_fk_src_p1"."addresses_hkey"
		INNER JOIN "moto_scn01_fmc"."dv_load_cycle_info" "dvlci_src_p1" ON  "dvlci_src_p1"."dv_load_cycle_id" = "sat_pk_src_p1"."load_cycle_id"
		INNER JOIN "moto_scn01_fmc"."load_cycle_info" "bvlci_src_p1" ON  1 = 1
		LEFT OUTER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_ex_src_p1" ON  "mex_ex_src_p1"."load_cycle_id" = "hub_pk_src_p1"."load_cycle_id" ::text
		WHERE  "mex_src_p1"."record_type" = 'U'
		UNION ALL 
		SELECT 
			  UPPER(ENCODE(DIGEST( "hub_pk_src_f1"."src_bk" || '#' || "hub_pk_src_f1"."customers_bk" || '#' || COALESCE("hub_fk_src_f1"."street_name_bk",
				"unhub_fk_src_f1"."street_name_bk")|| '#' ||  COALESCE("hub_fk_src_f1"."street_number_bk","unhub_fk_src_f1"."street_number_bk")|| '#' ||  COALESCE("hub_fk_src_f1"."postal_code_bk","unhub_fk_src_f1"."postal_code_bk")|| '#' ||  COALESCE("hub_fk_src_f1"."city_bk","unhub_fk_src_f1"."city_bk")|| '#' ,'MD5'),'HEX')) AS "lna_cust_addr_hkey"
			, "sat_pk_src_f1"."customers_hkey" AS "customers_hkey"
			, COALESCE("hub_fk_src_f1"."addresses_hkey","unhub_fk_src_f1"."addresses_hkey") AS "addresses_hkey"
			, CASE WHEN "mex_ex_src_f1"."load_cycle_id" IS NULL THEN "bvlci_src_f1"."load_cycle_id" ELSE "mex_ex_src_f1"."load_cycle_id"::int END AS "load_cycle_id"
			, "bvlci_src_f1"."load_date" AS "load_date"
		FROM "moto_scn01_fl"."sat_mm_customers" "sat_pk_src_f1"
		INNER JOIN "moto_scn01_fl"."hub_customers" "hub_pk_src_f1" ON  "sat_pk_src_f1"."customers_hkey" = "hub_pk_src_f1"."customers_hkey"
		LEFT OUTER JOIN "moto_scn01_fl"."sat_ms_addresses" "sat_fk_src_f1" ON  "sat_fk_src_f1"."address_number" = "sat_pk_src_f1"."address_number"
		LEFT OUTER JOIN "moto_scn01_fl"."hub_addresses" "hub_fk_src_f1" ON  "sat_fk_src_f1"."addresses_hkey" = "hub_fk_src_f1"."addresses_hkey"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src_f1" ON  1 = 1
		INNER JOIN "moto_scn01_fl"."hub_addresses" "unhub_fk_src_f1" ON  "mex_src_f1"."load_cycle_id"::int = "unhub_fk_src_f1"."load_cycle_id"
		INNER JOIN "moto_scn01_fl"."sat_ms_addresses" "unsat_fk_src_f1" ON  "unsat_fk_src_f1"."addresses_hkey" = "unhub_fk_src_f1"."addresses_hkey"
		INNER JOIN "moto_scn01_fmc"."dv_load_cycle_info" "dvlci_src_f1" ON  "dvlci_src_f1"."dv_load_cycle_id" = "sat_fk_src_f1"."load_cycle_id"
		INNER JOIN "moto_scn01_fmc"."load_cycle_info" "bvlci_src_f1" ON  1 = 1
		LEFT OUTER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_ex_src_f1" ON  "mex_ex_src_f1"."load_cycle_id" = "hub_pk_src_f1"."load_cycle_id" ::text
		WHERE  "mex_src_f1"."record_type" = 'U'
	)
	, "min_load_time" AS 
	( 
		SELECT 
			  "change_set"."lna_cust_addr_hkey" AS "lna_cust_addr_hkey"
			, "change_set"."load_date" AS "load_date"
			, "change_set"."load_cycle_id" AS "load_cycle_id"
			, "change_set"."customers_hkey" AS "customers_hkey"
			, "change_set"."addresses_hkey" AS "addresses_hkey"
			, ROW_NUMBER()OVER(PARTITION BY "change_set"."lna_cust_addr_hkey" ORDER BY "change_set"."load_date") AS "dummy"
		FROM "change_set" "change_set"
	)
	SELECT 
		  "min_load_time"."lna_cust_addr_hkey" AS "lna_cust_addr_hkey"
		, "min_load_time"."customers_hkey" AS "customers_hkey"
		, "min_load_time"."addresses_hkey" AS "addresses_hkey"
		, "min_load_time"."load_cycle_id" AS "load_cycle_id"
		, "min_load_time"."load_date" AS "load_date"
	FROM "min_load_time" "min_load_time"
	LEFT OUTER JOIN "moto_scn01_bv"."lna_cust_addr" "lna_src" ON  "min_load_time"."lna_cust_addr_hkey" = "lna_src"."lna_cust_addr_hkey"
	WHERE  "min_load_time"."dummy" = 1 AND "lna_src"."lna_cust_addr_hkey" is NULL
	;
END;



END;
$function$;
 
 
