CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_dl_ms_custaddresses_incr"() 
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

BEGIN -- stg_dl_tgt

	TRUNCATE TABLE "moto_sales_scn01_stg"."cust_addresses"  CASCADE;

	INSERT INTO "moto_sales_scn01_stg"."cust_addresses"(
		 "lnd_cust_addresses_hkey"
		,"customers_hkey"
		,"addresses_hkey"
		,"load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"customer_number"
		,"address_number"
		,"national_person_id_fk_customernumber_bk"
		,"street_name_fk_addressnumber_bk"
		,"street_number_fk_addressnumber_bk"
		,"postal_code_fk_addressnumber_bk"
		,"city_fk_addressnumber_bk"
		,"address_type_seq"
		,"update_timestamp"
		,"error_code_cust_addresses"
	)
	WITH "dist_fk1" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src1"."customer_number" AS "customer_number"
		FROM "moto_sales_scn01_ext"."cust_addresses" "ext_dis_src1"
	)
	, "dist_fk2" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src2"."address_number" AS "address_number"
		FROM "moto_sales_scn01_ext"."cust_addresses" "ext_dis_src2"
	)
	, "prep_find_bk_fk1" AS 
	( 
		SELECT 
			  UPPER(REPLACE(TRIM( "sat_src1"."national_person_id"),'#','\' || '#')) AS "national_person_id_bk"
			, "dist_fk1"."customer_number" AS "customer_number"
			, "sat_src1"."load_date" AS "load_date"
			, 1 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_scn01_fl"."sat_ms_customers_name" "sat_src1" ON  "dist_fk1"."customer_number" = "sat_src1"."customer_number"
		INNER JOIN "moto_scn01_fl"."hub_customers" "hub_src1" ON  "hub_src1"."customers_hkey" = "sat_src1"."customers_hkey"
		WHERE  "sat_src1"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
		UNION ALL 
		SELECT 
			  "ext_fkbk_src1"."national_person_id_bk" AS "national_person_id_bk"
			, "dist_fk1"."customer_number" AS "customer_number"
			, "ext_fkbk_src1"."load_date" AS "load_date"
			, 0 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_sales_scn01_ext"."customers" "ext_fkbk_src1" ON  "dist_fk1"."customer_number" = "ext_fkbk_src1"."customer_number"
	)
	, "prep_find_bk_fk2" AS 
	( 
		SELECT 
			  "hub_src2"."street_name_bk" AS "street_name_bk"
			, "hub_src2"."street_number_bk" AS "street_number_bk"
			, "hub_src2"."postal_code_bk" AS "postal_code_bk"
			, "hub_src2"."city_bk" AS "city_bk"
			, "dist_fk2"."address_number" AS "address_number"
			, "sat_src2"."load_date" AS "load_date"
			, 1 AS "general_order"
		FROM "dist_fk2" "dist_fk2"
		INNER JOIN "moto_scn01_fl"."sat_ms_addresses" "sat_src2" ON  "dist_fk2"."address_number" = "sat_src2"."address_number"
		INNER JOIN "moto_scn01_fl"."hub_addresses" "hub_src2" ON  "hub_src2"."addresses_hkey" = "sat_src2"."addresses_hkey"
		WHERE  "sat_src2"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
		UNION ALL 
		SELECT 
			  "ext_fkbk_src2"."street_name_bk" AS "street_name_bk"
			, "ext_fkbk_src2"."street_number_bk" AS "street_number_bk"
			, "ext_fkbk_src2"."postal_code_bk" AS "postal_code_bk"
			, "ext_fkbk_src2"."city_bk" AS "city_bk"
			, "dist_fk2"."address_number" AS "address_number"
			, "ext_fkbk_src2"."load_date" AS "load_date"
			, 0 AS "general_order"
		FROM "dist_fk2" "dist_fk2"
		INNER JOIN "moto_sales_scn01_ext"."addresses" "ext_fkbk_src2" ON  "dist_fk2"."address_number" = "ext_fkbk_src2"."address_number"
	)
	, "order_bk_fk1" AS 
	( 
		SELECT 
			  "prep_find_bk_fk1"."national_person_id_bk" AS "national_person_id_bk"
			, "prep_find_bk_fk1"."customer_number" AS "customer_number"
			, ROW_NUMBER()OVER(PARTITION BY "prep_find_bk_fk1"."customer_number" ORDER BY "prep_find_bk_fk1"."general_order",
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
			, "prep_find_bk_fk2"."address_number" AS "address_number"
			, ROW_NUMBER()OVER(PARTITION BY "prep_find_bk_fk2"."address_number" ORDER BY "prep_find_bk_fk2"."general_order",
				"prep_find_bk_fk2"."load_date" DESC) AS "dummy"
		FROM "prep_find_bk_fk2" "prep_find_bk_fk2"
	)
	, "find_bk_fk1" AS 
	( 
		SELECT 
			  "order_bk_fk1"."national_person_id_bk" AS "national_person_id_bk"
			, "order_bk_fk1"."customer_number" AS "customer_number"
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
			, "order_bk_fk2"."address_number" AS "address_number"
		FROM "order_bk_fk2" "order_bk_fk2"
		WHERE  "order_bk_fk2"."dummy" = 1
	)
	, "calc_hash_keys" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST(  'moto_sales_scn01' || '#' || COALESCE("find_bk_fk1"."national_person_id_bk","mex_src"."key_attribute_varchar")
				|| '#' || COALESCE("find_bk_fk2"."street_name_bk","mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk2"."street_number_bk","mex_src"."key_attribute_numeric")|| '#' ||  COALESCE("find_bk_fk2"."postal_code_bk","mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk2"."city_bk","mex_src"."key_attribute_varchar")|| '#'  ,'MD5'),'HEX')) AS "lnd_cust_addresses_hkey"
			, UPPER(ENCODE(DIGEST( 'moto_sales_scn01' || '#' || COALESCE("find_bk_fk1"."national_person_id_bk","mex_src"."key_attribute_varchar")
				|| '#' ,'MD5'),'HEX')) AS "customers_hkey"
			, UPPER(ENCODE(DIGEST( COALESCE("find_bk_fk2"."street_name_bk","mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk2"."street_number_bk",
				"mex_src"."key_attribute_numeric")|| '#' ||  COALESCE("find_bk_fk2"."postal_code_bk","mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk2"."city_bk","mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "addresses_hkey"
			, "ext_src"."load_date" AS "load_date"
			, "ext_src"."load_cycle_id" AS "load_cycle_id"
			, "ext_src"."trans_timestamp" AS "trans_timestamp"
			, "ext_src"."operation" AS "operation"
			, "ext_src"."record_type" AS "record_type"
			, "ext_src"."customer_number" AS "customer_number"
			, "ext_src"."address_number" AS "address_number"
			, NULL::text AS "national_person_id_fk_customernumber_bk"
			, NULL::text AS "street_name_fk_addressnumber_bk"
			, NULL::text AS "street_number_fk_addressnumber_bk"
			, NULL::text AS "postal_code_fk_addressnumber_bk"
			, NULL::text AS "city_fk_addressnumber_bk"
			, "ext_src"."address_type_seq" AS "address_type_seq"
			, "ext_src"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "ext_src"."error_code_cust_addresses" = 2 THEN 2 WHEN "ext_src"."error_code_cust_addresses" =
				- 1 THEN 0 WHEN  "find_bk_fk1"."customer_number" IS NULL OR "find_bk_fk2"."address_number" IS NULL  THEN "ext_src"."error_code_cust_addresses" ELSE 0 END AS "error_code_cust_addresses"
		FROM "moto_sales_scn01_ext"."cust_addresses" "ext_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
		LEFT OUTER JOIN "find_bk_fk1" "find_bk_fk1" ON  "ext_src"."customer_number" = "find_bk_fk1"."customer_number"
		LEFT OUTER JOIN "find_bk_fk2" "find_bk_fk2" ON  "ext_src"."address_number" = "find_bk_fk2"."address_number"
	)
	SELECT 
		  "calc_hash_keys"."lnd_cust_addresses_hkey" AS "lnd_cust_addresses_hkey"
		, "calc_hash_keys"."customers_hkey" AS "customers_hkey"
		, "calc_hash_keys"."addresses_hkey" AS "addresses_hkey"
		, CASE WHEN "calc_hash_keys"."record_type" = 'E' THEN "calc_hash_keys"."load_date" ELSE CURRENT_TIMESTAMP +
			row_number() over (PARTITION BY  "calc_hash_keys"."customers_hkey" ,  "calc_hash_keys"."addresses_hkey" ,"calc_hash_keys"."address_type_seq"  ORDER BY  "calc_hash_keys"."trans_timestamp") * interval'2 microsecond'   END AS "load_date"
		, "calc_hash_keys"."load_cycle_id" AS "load_cycle_id"
		, "calc_hash_keys"."trans_timestamp" AS "trans_timestamp"
		, "calc_hash_keys"."operation" AS "operation"
		, "calc_hash_keys"."record_type" AS "record_type"
		, "calc_hash_keys"."customer_number" AS "customer_number"
		, "calc_hash_keys"."address_number" AS "address_number"
		, "calc_hash_keys"."national_person_id_fk_customernumber_bk" AS "national_person_id_fk_customernumber_bk"
		, "calc_hash_keys"."street_name_fk_addressnumber_bk" AS "street_name_fk_addressnumber_bk"
		, "calc_hash_keys"."street_number_fk_addressnumber_bk" AS "street_number_fk_addressnumber_bk"
		, "calc_hash_keys"."postal_code_fk_addressnumber_bk" AS "postal_code_fk_addressnumber_bk"
		, "calc_hash_keys"."city_fk_addressnumber_bk" AS "city_fk_addressnumber_bk"
		, "calc_hash_keys"."address_type_seq" AS "address_type_seq"
		, "calc_hash_keys"."update_timestamp" AS "update_timestamp"
		, "calc_hash_keys"."error_code_cust_addresses" AS "error_code_cust_addresses"
	FROM "calc_hash_keys" "calc_hash_keys"
	;
END;


BEGIN -- ext_err_tgt

	TRUNCATE TABLE "moto_sales_scn01_ext"."cust_addresses_err"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."cust_addresses_err"(
		 "load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"customer_number"
		,"address_number"
		,"address_type_seq"
		,"update_timestamp"
		,"error_code_cust_addresses"
	)
	SELECT 
		  CASE WHEN "stg_err_src"."error_code_cust_addresses" = 1 THEN "stg_err_src"."load_date" + interval'1 microsecond'   ELSE "stg_err_src"."load_date" END AS "load_date"
		, "stg_err_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_err_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_err_src"."operation" AS "operation"
		, "stg_err_src"."record_type" AS "record_type"
		, "stg_err_src"."customer_number" AS "customer_number"
		, "stg_err_src"."address_number" AS "address_number"
		, "stg_err_src"."address_type_seq" AS "address_type_seq"
		, "stg_err_src"."update_timestamp" AS "update_timestamp"
		, "stg_err_src"."error_code_cust_addresses" AS "error_code_cust_addresses"
	FROM "moto_sales_scn01_stg"."cust_addresses" "stg_err_src"
	WHERE  "stg_err_src"."error_code_cust_addresses" IN(1,3)AND "stg_err_src"."load_cycle_id" >= 0
	;
END;



END;
$function$;
 
 
