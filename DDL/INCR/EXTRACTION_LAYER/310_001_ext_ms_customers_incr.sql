CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_ms_customers_incr"() 
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

Vaultspeed version: 5.7.2.16, generation date: 2025/01/16 15:00:22
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/16 14:56:23, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08
 */


BEGIN 

BEGIN -- ext_tgt

	TRUNCATE TABLE "moto_sales_scn01_ext"."customers"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."customers"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"customer_number"
		,"customer_invoice_address_id"
		,"customer_ship_to_address_id"
		,"national_person_id_bk"
		,"national_person_id"
		,"first_name"
		,"last_name"
		,"birthdate"
		,"gender"
		,"update_timestamp"
		,"error_code_cust_addr_iats"
		,"error_code_cust_addr_ciai"
	)
	WITH "calculate_bk" AS 
	( 
		SELECT 
			  "tdfv_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("tdfv_src"."operation","mex_src"."attribute_varchar") AS "operation"
			, "tdfv_src"."record_type" AS "record_type"
			, "tdfv_src"."customer_number" AS "customer_number"
			, "tdfv_src"."customer_invoice_address_id" AS "customer_invoice_address_id"
			, "tdfv_src"."customer_ship_to_address_id" AS "customer_ship_to_address_id"
			, CASE WHEN TRIM("tdfv_src"."national_person_id")= '' OR "tdfv_src"."national_person_id" IS NULL THEN "mex_src"."key_attribute_varchar" ELSE UPPER(REPLACE(TRIM( "tdfv_src"."national_person_id")
				,'#','\' || '#'))END AS "national_person_id_bk"
			, "tdfv_src"."national_person_id" AS "national_person_id"
			, "tdfv_src"."first_name" AS "first_name"
			, "tdfv_src"."last_name" AS "last_name"
			, "tdfv_src"."birthdate" AS "birthdate"
			, "tdfv_src"."gender" AS "gender"
			, "tdfv_src"."update_timestamp" AS "update_timestamp"
			, 1 AS "error_code_cust_addr_iats"
			, 1 AS "error_code_cust_addr_ciai"
		FROM "moto_sales_scn01_dfv"."vw_customers" "tdfv_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	, "ext_union" AS 
	( 
		SELECT 
			  "lci_src"."load_cycle_id" AS "load_cycle_id"
			, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "calculate_bk"."national_person_id_bk"  ORDER BY  "calculate_bk"."trans_timestamp")
				* interval'2 microsecond'   AS "load_date"
			, "calculate_bk"."trans_timestamp" AS "trans_timestamp"
			, "calculate_bk"."operation" AS "operation"
			, "calculate_bk"."record_type" AS "record_type"
			, "calculate_bk"."customer_number" AS "customer_number"
			, "calculate_bk"."customer_invoice_address_id" AS "customer_invoice_address_id"
			, "calculate_bk"."customer_ship_to_address_id" AS "customer_ship_to_address_id"
			, "calculate_bk"."national_person_id_bk" AS "national_person_id_bk"
			, "calculate_bk"."national_person_id" AS "national_person_id"
			, "calculate_bk"."first_name" AS "first_name"
			, "calculate_bk"."last_name" AS "last_name"
			, "calculate_bk"."birthdate" AS "birthdate"
			, "calculate_bk"."gender" AS "gender"
			, "calculate_bk"."update_timestamp" AS "update_timestamp"
			, "calculate_bk"."error_code_cust_addr_iats" AS "error_code_cust_addr_iats"
			, "calculate_bk"."error_code_cust_addr_ciai" AS "error_code_cust_addr_ciai"
		FROM "calculate_bk" "calculate_bk"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		UNION 
		SELECT 
			  "err_lci_src"."load_cycle_id" AS "load_cycle_id"
			, "ext_err_src"."load_date" AS "load_date"
			, "ext_err_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("ext_err_src"."operation",'U') AS "operation"
			, 'E' AS "record_type"
			, "ext_err_src"."customer_number" AS "customer_number"
			, "ext_err_src"."customer_invoice_address_id" AS "customer_invoice_address_id"
			, "ext_err_src"."customer_ship_to_address_id" AS "customer_ship_to_address_id"
			, "ext_err_src"."national_person_id_bk" AS "national_person_id_bk"
			, "ext_err_src"."national_person_id" AS "national_person_id"
			, "ext_err_src"."first_name" AS "first_name"
			, "ext_err_src"."last_name" AS "last_name"
			, "ext_err_src"."birthdate" AS "birthdate"
			, "ext_err_src"."gender" AS "gender"
			, "ext_err_src"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "ext_err_src"."error_code_cust_addr_iats" = 0 THEN 2 WHEN "ext_err_src"."error_code_cust_addr_iats" =
				1 THEN 3 ELSE"ext_err_src"."error_code_cust_addr_iats" END AS "error_code_cust_addr_iats"
			, CASE WHEN "ext_err_src"."error_code_cust_addr_ciai" = 0 THEN 2 WHEN "ext_err_src"."error_code_cust_addr_ciai" =
				1 THEN 3 ELSE"ext_err_src"."error_code_cust_addr_ciai" END AS "error_code_cust_addr_ciai"
		FROM "moto_sales_scn01_ext"."customers_err" "ext_err_src"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "err_lci_src" ON  1 = 1
		WHERE  "ext_err_src"."error_code_cust_addr_iats" < 4 AND  "ext_err_src"."error_code_cust_addr_ciai" < 4
	)
	SELECT 
		  "ext_union"."load_cycle_id" AS "load_cycle_id"
		, "ext_union"."load_date" AS "load_date"
		, "ext_union"."trans_timestamp" AS "trans_timestamp"
		, "ext_union"."operation" AS "operation"
		, "ext_union"."record_type" AS "record_type"
		, "ext_union"."customer_number" AS "customer_number"
		, "ext_union"."customer_invoice_address_id" AS "customer_invoice_address_id"
		, "ext_union"."customer_ship_to_address_id" AS "customer_ship_to_address_id"
		, "ext_union"."national_person_id_bk" AS "national_person_id_bk"
		, "ext_union"."national_person_id" AS "national_person_id"
		, "ext_union"."first_name" AS "first_name"
		, "ext_union"."last_name" AS "last_name"
		, "ext_union"."birthdate" AS "birthdate"
		, "ext_union"."gender" AS "gender"
		, "ext_union"."update_timestamp" AS "update_timestamp"
		, "ext_union"."error_code_cust_addr_iats" AS "error_code_cust_addr_iats"
		, "ext_union"."error_code_cust_addr_ciai" AS "error_code_cust_addr_ciai"
	FROM "ext_union" "ext_union"
	;
END;



END;
$function$;
 
 
