CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_ms_customers_init"() 
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
	)
	WITH "load_init_data" AS 
	( 
		SELECT 
			  'I' ::text AS "operation"
			, 'S'::text AS "record_type"
			, COALESCE("ini_src"."customer_number", TO_NUMBER("mex_inr_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "customer_number"
			, COALESCE("ini_src"."customer_ship_to_address_id", TO_NUMBER("mex_inr_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "customer_ship_to_address_id"
			, COALESCE("ini_src"."customer_invoice_address_id", TO_NUMBER("mex_inr_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "customer_invoice_address_id"
			, "ini_src"."national_person_id" AS "national_person_id"
			, "ini_src"."first_name" AS "first_name"
			, "ini_src"."last_name" AS "last_name"
			, "ini_src"."birthdate" AS "birthdate"
			, "ini_src"."gender" AS "gender"
			, "ini_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01"."customers" "ini_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_inr_src" ON  "mex_inr_src"."record_type" = 'N'
	)
	, "prep_excep" AS 
	( 
		SELECT 
			  "load_init_data"."operation" AS "operation"
			, "load_init_data"."record_type" AS "record_type"
			, NULL ::int AS "load_cycle_id"
			, "load_init_data"."customer_number" AS "customer_number"
			, "load_init_data"."customer_invoice_address_id" AS "customer_invoice_address_id"
			, "load_init_data"."customer_ship_to_address_id" AS "customer_ship_to_address_id"
			, "load_init_data"."national_person_id" AS "national_person_id"
			, "load_init_data"."first_name" AS "first_name"
			, "load_init_data"."last_name" AS "last_name"
			, "load_init_data"."birthdate" AS "birthdate"
			, "load_init_data"."gender" AS "gender"
			, "load_init_data"."update_timestamp" AS "update_timestamp"
		FROM "load_init_data" "load_init_data"
		UNION ALL 
		SELECT 
			  'I' ::text AS "operation"
			, "mex_ext_src"."record_type" AS "record_type"
			, "mex_ext_src"."load_cycle_id" ::int AS "load_cycle_id"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "customer_number"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "customer_invoice_address_id"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "customer_ship_to_address_id"
			, "mex_ext_src"."key_attribute_varchar"::text AS "national_person_id"
			, "mex_ext_src"."attribute_varchar"::text AS "first_name"
			, "mex_ext_src"."attribute_varchar"::text AS "last_name"
			, TO_DATE("mex_ext_src"."attribute_date", 'DD/MM/YYYY'::varchar) AS "birthdate"
			, "mex_ext_src"."attribute_character"::text AS "gender"
			, TO_TIMESTAMP("mex_ext_src"."attribute_timestamp", 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "update_timestamp"
		FROM "moto_sales_scn01_mtd"."mtd_exception_records" "mex_ext_src"
	)
	, "calculate_bk" AS 
	( 
		SELECT 
			  COALESCE("prep_excep"."load_cycle_id","lci_src"."load_cycle_id") AS "load_cycle_id"
			, "lci_src"."load_date" AS "load_date"
			, "lci_src"."load_date" AS "trans_timestamp"
			, "prep_excep"."operation" AS "operation"
			, "prep_excep"."record_type" AS "record_type"
			, "prep_excep"."customer_number" AS "customer_number"
			, "prep_excep"."customer_invoice_address_id" AS "customer_invoice_address_id"
			, "prep_excep"."customer_ship_to_address_id" AS "customer_ship_to_address_id"
			, CASE WHEN TRIM("prep_excep"."national_person_id")= '' OR "prep_excep"."national_person_id" IS NULL THEN "mex_src"."key_attribute_varchar" ELSE UPPER(REPLACE(TRIM("prep_excep"."national_person_id")
				,'#','\' || '#'))END AS "national_person_id_bk"
			, "prep_excep"."national_person_id" AS "national_person_id"
			, "prep_excep"."first_name" AS "first_name"
			, "prep_excep"."last_name" AS "last_name"
			, "prep_excep"."birthdate" AS "birthdate"
			, "prep_excep"."gender" AS "gender"
			, "prep_excep"."update_timestamp" AS "update_timestamp"
		FROM "prep_excep" "prep_excep"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	SELECT 
		  "calculate_bk"."load_cycle_id" AS "load_cycle_id"
		, "calculate_bk"."load_date" AS "load_date"
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
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 
