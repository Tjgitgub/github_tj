CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_ms_custaddresses_init"() 
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

	TRUNCATE TABLE "moto_sales_scn01_ext"."cust_addresses"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."cust_addresses"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"customer_number"
		,"address_number"
		,"address_type_seq"
		,"update_timestamp"
	)
	WITH "load_init_data" AS 
	( 
		SELECT 
			  'I' ::text AS "operation"
			, 'S'::text AS "record_type"
			, COALESCE("ini_src"."customer_number", TO_NUMBER("mex_inr_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "customer_number"
			, COALESCE("ini_src"."address_number", TO_NUMBER("mex_inr_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "address_number"
			, CASE WHEN TRIM("ini_src"."address_type")= '' THEN "mex_inr_src"."key_attribute_varchar"::text ELSE COALESCE("ini_src"."address_type",
				"mex_inr_src"."key_attribute_varchar"::text)END AS "address_type_seq"
			, "ini_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01"."cust_addresses" "ini_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_inr_src" ON  "mex_inr_src"."record_type" = 'N'
	)
	, "prep_excep" AS 
	( 
		SELECT 
			  "load_init_data"."operation" AS "operation"
			, "load_init_data"."record_type" AS "record_type"
			, NULL ::int AS "load_cycle_id"
			, "load_init_data"."customer_number" AS "customer_number"
			, "load_init_data"."address_number" AS "address_number"
			, "load_init_data"."address_type_seq" AS "address_type_seq"
			, "load_init_data"."update_timestamp" AS "update_timestamp"
		FROM "load_init_data" "load_init_data"
		UNION ALL 
		SELECT 
			  'I' ::text AS "operation"
			, "mex_ext_src"."record_type" AS "record_type"
			, "mex_ext_src"."load_cycle_id" ::int AS "load_cycle_id"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "customer_number"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "address_number"
			, "mex_ext_src"."key_attribute_varchar"::text AS "address_type_seq"
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
			, "prep_excep"."address_number" AS "address_number"
			, "prep_excep"."address_type_seq" AS "address_type_seq"
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
		, "calculate_bk"."address_number" AS "address_number"
		, "calculate_bk"."address_type_seq" AS "address_type_seq"
		, "calculate_bk"."update_timestamp" AS "update_timestamp"
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 
