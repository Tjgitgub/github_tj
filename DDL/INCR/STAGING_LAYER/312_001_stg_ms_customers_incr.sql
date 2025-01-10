CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_ms_customers_incr"() 
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

Vaultspeed version: 5.7.2.14, generation date: 2025/01/09 12:47:43
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/09 09:40:46, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04
 */


BEGIN 

BEGIN -- stg_tgt

	TRUNCATE TABLE "moto_sales_scn01_stg"."customers"  CASCADE;

	INSERT INTO "moto_sales_scn01_stg"."customers"(
		 "customers_hkey"
		,"addresses_iats_hkey"
		,"addresses_ciai_hkey"
		,"lnk_cust_addr_iats_hkey"
		,"lnk_cust_addr_ciai_hkey"
		,"load_date"
		,"load_cycle_id"
		,"src_bk"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"customer_number"
		,"customer_invoice_address_id"
		,"customer_ship_to_address_id"
		,"customers_bk"
		,"national_person_id"
		,"street_name_fk_customershiptoaddressid_bk"
		,"street_number_fk_customershiptoaddressid_bk"
		,"postal_code_fk_customershiptoaddressid_bk"
		,"city_fk_customershiptoaddressid_bk"
		,"street_name_fk_customerinvoiceaddressid_bk"
		,"street_number_fk_customerinvoiceaddressid_bk"
		,"postal_code_fk_customerinvoiceaddressid_bk"
		,"city_fk_customerinvoiceaddressid_bk"
		,"first_name"
		,"last_name"
		,"birthdate"
		,"gender"
		,"update_timestamp"
		,"error_code_cust_addr_iats"
		,"error_code_cust_addr_ciai"
	)
	WITH "dist_fk1" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src1"."customer_ship_to_address_id" AS "customer_ship_to_address_id"
		FROM "moto_sales_scn01_ext"."customers" "ext_dis_src1"
	)
	, "dist_fk2" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src2"."customer_invoice_address_id" AS "customer_invoice_address_id"
		FROM "moto_sales_scn01_ext"."customers" "ext_dis_src2"
	)
	, "prep_find_bk_fk1" AS 
	( 
		SELECT 
			  "hub_src1"."street_name_bk" AS "street_name_bk"
			, "hub_src1"."street_number_bk" AS "street_number_bk"
			, "hub_src1"."postal_code_bk" AS "postal_code_bk"
			, "hub_src1"."city_bk" AS "city_bk"
			, "dist_fk1"."customer_ship_to_address_id" AS "customer_ship_to_address_id"
			, "sat_src1"."load_date" AS "load_date"
			, 1 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_scn01_fl"."sat_ms_addresses" "sat_src1" ON  "dist_fk1"."customer_ship_to_address_id" = "sat_src1"."address_number"
		INNER JOIN "moto_scn01_fl"."hub_addresses" "hub_src1" ON  "hub_src1"."addresses_hkey" = "sat_src1"."addresses_hkey"
		WHERE  "sat_src1"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
		UNION ALL 
		SELECT 
			  "ext_fkbk_src1"."street_name_bk" AS "street_name_bk"
			, "ext_fkbk_src1"."street_number_bk" AS "street_number_bk"
			, "ext_fkbk_src1"."postal_code_bk" AS "postal_code_bk"
			, "ext_fkbk_src1"."city_bk" AS "city_bk"
			, "dist_fk1"."customer_ship_to_address_id" AS "customer_ship_to_address_id"
			, "ext_fkbk_src1"."load_date" AS "load_date"
			, 0 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_sales_scn01_ext"."addresses" "ext_fkbk_src1" ON  "dist_fk1"."customer_ship_to_address_id" = "ext_fkbk_src1"."address_number"
	)
	, "prep_find_bk_fk2" AS 
	( 
		SELECT 
			  "hub_src2"."street_name_bk" AS "street_name_bk"
			, "hub_src2"."street_number_bk" AS "street_number_bk"
			, "hub_src2"."postal_code_bk" AS "postal_code_bk"
			, "hub_src2"."city_bk" AS "city_bk"
			, "dist_fk2"."customer_invoice_address_id" AS "customer_invoice_address_id"
			, "sat_src2"."load_date" AS "load_date"
			, 1 AS "general_order"
		FROM "dist_fk2" "dist_fk2"
		INNER JOIN "moto_scn01_fl"."sat_ms_addresses" "sat_src2" ON  "dist_fk2"."customer_invoice_address_id" = "sat_src2"."address_number"
		INNER JOIN "moto_scn01_fl"."hub_addresses" "hub_src2" ON  "hub_src2"."addresses_hkey" = "sat_src2"."addresses_hkey"
		WHERE  "sat_src2"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
		UNION ALL 
		SELECT 
			  "ext_fkbk_src2"."street_name_bk" AS "street_name_bk"
			, "ext_fkbk_src2"."street_number_bk" AS "street_number_bk"
			, "ext_fkbk_src2"."postal_code_bk" AS "postal_code_bk"
			, "ext_fkbk_src2"."city_bk" AS "city_bk"
			, "dist_fk2"."customer_invoice_address_id" AS "customer_invoice_address_id"
			, "ext_fkbk_src2"."load_date" AS "load_date"
			, 0 AS "general_order"
		FROM "dist_fk2" "dist_fk2"
		INNER JOIN "moto_sales_scn01_ext"."addresses" "ext_fkbk_src2" ON  "dist_fk2"."customer_invoice_address_id" = "ext_fkbk_src2"."address_number"
	)
	, "order_bk_fk1" AS 
	( 
		SELECT 
			  "prep_find_bk_fk1"."street_name_bk" AS "street_name_bk"
			, "prep_find_bk_fk1"."street_number_bk" AS "street_number_bk"
			, "prep_find_bk_fk1"."postal_code_bk" AS "postal_code_bk"
			, "prep_find_bk_fk1"."city_bk" AS "city_bk"
			, "prep_find_bk_fk1"."customer_ship_to_address_id" AS "customer_ship_to_address_id"
			, ROW_NUMBER()OVER(PARTITION BY "prep_find_bk_fk1"."customer_ship_to_address_id" ORDER BY "prep_find_bk_fk1"."general_order",
				"prep_find_bk_fk1"."load_date" DESC) AS "dummy"
		FROM "prep_find_bk_fk1" "prep_find_bk_fk1"
	)
	, "order_bk_fk2" AS 
	( 
		SELECT 
			  "prep_find_bk_fk2"."street_name_bk" AS "street_name_bk"
			, "prep_find_bk_fk2"."street_number_bk" AS "street_number_bk"
			, "prep_find_bk_fk2"."postal_code_bk" AS "postal_code_bk"
			, "prep_find_bk_fk2"."city_bk" AS "city_bk"
			, "prep_find_bk_fk2"."customer_invoice_address_id" AS "customer_invoice_address_id"
			, ROW_NUMBER()OVER(PARTITION BY "prep_find_bk_fk2"."customer_invoice_address_id" ORDER BY "prep_find_bk_fk2"."general_order",
				"prep_find_bk_fk2"."load_date" DESC) AS "dummy"
		FROM "prep_find_bk_fk2" "prep_find_bk_fk2"
	)
	, "find_bk_fk1" AS 
	( 
		SELECT 
			  "order_bk_fk1"."street_name_bk" AS "street_name_bk"
			, "order_bk_fk1"."street_number_bk" AS "street_number_bk"
			, "order_bk_fk1"."postal_code_bk" AS "postal_code_bk"
			, "order_bk_fk1"."city_bk" AS "city_bk"
			, "order_bk_fk1"."customer_ship_to_address_id" AS "customer_ship_to_address_id"
		FROM "order_bk_fk1" "order_bk_fk1"
		WHERE  "order_bk_fk1"."dummy" = 1
	)
	, "find_bk_fk2" AS 
	( 
		SELECT 
			  "order_bk_fk2"."street_name_bk" AS "street_name_bk"
			, "order_bk_fk2"."street_number_bk" AS "street_number_bk"
			, "order_bk_fk2"."postal_code_bk" AS "postal_code_bk"
			, "order_bk_fk2"."city_bk" AS "city_bk"
			, "order_bk_fk2"."customer_invoice_address_id" AS "customer_invoice_address_id"
		FROM "order_bk_fk2" "order_bk_fk2"
		WHERE  "order_bk_fk2"."dummy" = 1
	)
	, "calc_hash_keys" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST( 'moto_sales_scn01' || '#' || "ext_src"."national_person_id_bk" || '#' ,'MD5'),'HEX')) AS "customers_hkey"
			, UPPER(ENCODE(DIGEST( COALESCE("find_bk_fk1"."street_name_bk","mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk1"."street_number_bk",
				"mex_src"."key_attribute_numeric")|| '#' ||  COALESCE("find_bk_fk1"."postal_code_bk","mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk1"."city_bk","mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "addresses_iats_hkey"
			, UPPER(ENCODE(DIGEST( COALESCE("find_bk_fk2"."street_name_bk","mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk2"."street_number_bk",
				"mex_src"."key_attribute_numeric")|| '#' ||  COALESCE("find_bk_fk2"."postal_code_bk","mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk2"."city_bk","mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "addresses_ciai_hkey"
			, UPPER(ENCODE(DIGEST( 'moto_sales_scn01' || '#' || "ext_src"."national_person_id_bk" || '#' || COALESCE("find_bk_fk1"."street_name_bk",
				"mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk1"."street_number_bk","mex_src"."key_attribute_numeric")|| '#' ||  COALESCE("find_bk_fk1"."postal_code_bk","mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk1"."city_bk","mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "lnk_cust_addr_iats_hkey"
			, UPPER(ENCODE(DIGEST( 'moto_sales_scn01' || '#' || "ext_src"."national_person_id_bk" || '#' || COALESCE("find_bk_fk2"."street_name_bk",
				"mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk2"."street_number_bk","mex_src"."key_attribute_numeric")|| '#' ||  COALESCE("find_bk_fk2"."postal_code_bk","mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk2"."city_bk","mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "lnk_cust_addr_ciai_hkey"
			, "ext_src"."load_date" AS "load_date"
			, "ext_src"."load_cycle_id" AS "load_cycle_id"
			, 'moto_sales_scn01' AS "src_bk"
			, "ext_src"."trans_timestamp" AS "trans_timestamp"
			, "ext_src"."operation" AS "operation"
			, "ext_src"."record_type" AS "record_type"
			, "ext_src"."customer_number" AS "customer_number"
			, "ext_src"."customer_invoice_address_id" AS "customer_invoice_address_id"
			, "ext_src"."customer_ship_to_address_id" AS "customer_ship_to_address_id"
			, "ext_src"."national_person_id_bk" AS "customers_bk"
			, "ext_src"."national_person_id" AS "national_person_id"
			, NULL::text AS "street_name_fk_customershiptoaddressid_bk"
			, NULL::text AS "street_number_fk_customershiptoaddressid_bk"
			, NULL::text AS "postal_code_fk_customershiptoaddressid_bk"
			, NULL::text AS "city_fk_customershiptoaddressid_bk"
			, NULL::text AS "street_name_fk_customerinvoiceaddressid_bk"
			, NULL::text AS "street_number_fk_customerinvoiceaddressid_bk"
			, NULL::text AS "postal_code_fk_customerinvoiceaddressid_bk"
			, NULL::text AS "city_fk_customerinvoiceaddressid_bk"
			, "ext_src"."first_name" AS "first_name"
			, "ext_src"."last_name" AS "last_name"
			, "ext_src"."birthdate" AS "birthdate"
			, "ext_src"."gender" AS "gender"
			, "ext_src"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "ext_src"."error_code_cust_addr_iats" = 2 THEN 2 WHEN "ext_src"."error_code_cust_addr_iats" =
				- 1 THEN 0 WHEN "find_bk_fk1"."customer_ship_to_address_id" IS NULL THEN "ext_src"."error_code_cust_addr_iats" ELSE 0 END AS "error_code_cust_addr_iats"
			, CASE WHEN "ext_src"."error_code_cust_addr_ciai" = 2 THEN 2 WHEN "ext_src"."error_code_cust_addr_ciai" =
				- 1 THEN 0 WHEN "find_bk_fk2"."customer_invoice_address_id" IS NULL THEN "ext_src"."error_code_cust_addr_ciai" ELSE 0 END AS "error_code_cust_addr_ciai"
		FROM "moto_sales_scn01_ext"."customers" "ext_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
		LEFT OUTER JOIN "find_bk_fk1" "find_bk_fk1" ON  "ext_src"."customer_ship_to_address_id" = "find_bk_fk1"."customer_ship_to_address_id"
		LEFT OUTER JOIN "find_bk_fk2" "find_bk_fk2" ON  "ext_src"."customer_invoice_address_id" = "find_bk_fk2"."customer_invoice_address_id"
	)
	SELECT 
		  "calc_hash_keys"."customers_hkey" AS "customers_hkey"
		, "calc_hash_keys"."addresses_iats_hkey" AS "addresses_iats_hkey"
		, "calc_hash_keys"."addresses_ciai_hkey" AS "addresses_ciai_hkey"
		, "calc_hash_keys"."lnk_cust_addr_iats_hkey" AS "lnk_cust_addr_iats_hkey"
		, "calc_hash_keys"."lnk_cust_addr_ciai_hkey" AS "lnk_cust_addr_ciai_hkey"
		, "calc_hash_keys"."load_date" AS "load_date"
		, "calc_hash_keys"."load_cycle_id" AS "load_cycle_id"
		, "calc_hash_keys"."src_bk" AS "src_bk"
		, "calc_hash_keys"."trans_timestamp" AS "trans_timestamp"
		, "calc_hash_keys"."operation" AS "operation"
		, "calc_hash_keys"."record_type" AS "record_type"
		, "calc_hash_keys"."customer_number" AS "customer_number"
		, "calc_hash_keys"."customer_invoice_address_id" AS "customer_invoice_address_id"
		, "calc_hash_keys"."customer_ship_to_address_id" AS "customer_ship_to_address_id"
		, "calc_hash_keys"."customers_bk" AS "customers_bk"
		, "calc_hash_keys"."national_person_id" AS "national_person_id"
		, "calc_hash_keys"."street_name_fk_customershiptoaddressid_bk" AS "street_name_fk_customershiptoaddressid_bk"
		, "calc_hash_keys"."street_number_fk_customershiptoaddressid_bk" AS "street_number_fk_customershiptoaddressid_bk"
		, "calc_hash_keys"."postal_code_fk_customershiptoaddressid_bk" AS "postal_code_fk_customershiptoaddressid_bk"
		, "calc_hash_keys"."city_fk_customershiptoaddressid_bk" AS "city_fk_customershiptoaddressid_bk"
		, "calc_hash_keys"."street_name_fk_customerinvoiceaddressid_bk" AS "street_name_fk_customerinvoiceaddressid_bk"
		, "calc_hash_keys"."street_number_fk_customerinvoiceaddressid_bk" AS "street_number_fk_customerinvoiceaddressid_bk"
		, "calc_hash_keys"."postal_code_fk_customerinvoiceaddressid_bk" AS "postal_code_fk_customerinvoiceaddressid_bk"
		, "calc_hash_keys"."city_fk_customerinvoiceaddressid_bk" AS "city_fk_customerinvoiceaddressid_bk"
		, "calc_hash_keys"."first_name" AS "first_name"
		, "calc_hash_keys"."last_name" AS "last_name"
		, "calc_hash_keys"."birthdate" AS "birthdate"
		, "calc_hash_keys"."gender" AS "gender"
		, "calc_hash_keys"."update_timestamp" AS "update_timestamp"
		, "calc_hash_keys"."error_code_cust_addr_iats" AS "error_code_cust_addr_iats"
		, "calc_hash_keys"."error_code_cust_addr_ciai" AS "error_code_cust_addr_ciai"
	FROM "calc_hash_keys" "calc_hash_keys"
	;
END;


BEGIN -- ext_err_tgt

	TRUNCATE TABLE "moto_sales_scn01_ext"."customers_err"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."customers_err"(
		 "load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"customer_number"
		,"customer_invoice_address_id"
		,"customer_ship_to_address_id"
		,"national_person_id"
		,"national_person_id_bk"
		,"first_name"
		,"last_name"
		,"birthdate"
		,"gender"
		,"update_timestamp"
		,"error_code_cust_addr_iats"
		,"error_code_cust_addr_ciai"
	)
	SELECT 
		  CASE WHEN "stg_err_src"."error_code_cust_addr_iats" = 1 OR  "stg_err_src"."error_code_cust_addr_ciai" =
			1 THEN "stg_err_src"."load_date" + interval'1 microsecond'   ELSE "stg_err_src"."load_date" END AS "load_date"
		, "stg_err_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_err_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_err_src"."operation" AS "operation"
		, "stg_err_src"."record_type" AS "record_type"
		, "stg_err_src"."customer_number" AS "customer_number"
		, "stg_err_src"."customer_invoice_address_id" AS "customer_invoice_address_id"
		, "stg_err_src"."customer_ship_to_address_id" AS "customer_ship_to_address_id"
		, "stg_err_src"."national_person_id" AS "national_person_id"
		, CASE WHEN "stg_err_src"."national_person_id" = '' OR "stg_err_src"."national_person_id" IS NULL THEN "err_mex_src"."key_attribute_varchar" ELSE UPPER(REPLACE(TRIM( "stg_err_src"."national_person_id")
			,'#','\' || '#'))END AS "national_person_id_bk"
		, "stg_err_src"."first_name" AS "first_name"
		, "stg_err_src"."last_name" AS "last_name"
		, "stg_err_src"."birthdate" AS "birthdate"
		, "stg_err_src"."gender" AS "gender"
		, "stg_err_src"."update_timestamp" AS "update_timestamp"
		, "stg_err_src"."error_code_cust_addr_iats" AS "error_code_cust_addr_iats"
		, "stg_err_src"."error_code_cust_addr_ciai" AS "error_code_cust_addr_ciai"
	FROM "moto_sales_scn01_stg"."customers" "stg_err_src"
	INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "err_mex_src" ON  1 = 1
	WHERE ( "stg_err_src"."error_code_cust_addr_iats" IN(1,3) OR  "stg_err_src"."error_code_cust_addr_ciai" IN(1,3))AND "stg_err_src"."load_cycle_id" >= 0 AND "err_mex_src"."record_type" = 'N'
	;
END;



END;
$function$;
 
 
