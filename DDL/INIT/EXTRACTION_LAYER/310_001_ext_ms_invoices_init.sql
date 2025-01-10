CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_ms_invoices_init"() 
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

BEGIN -- ext_tgt

	TRUNCATE TABLE "moto_sales_scn01_ext"."invoices"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."invoices"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"invoice_number"
		,"invoice_customer_id"
		,"invoice_number_bk"
		,"invoice_date"
		,"amount"
		,"discount"
		,"update_timestamp"
	)
	WITH "load_init_data" AS 
	( 
		SELECT 
			  'I' ::text AS "operation"
			, 'S'::text AS "record_type"
			, COALESCE("ini_src"."invoice_number", TO_NUMBER("mex_inr_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "invoice_number"
			, COALESCE("ini_src"."invoice_customer_id", TO_NUMBER("mex_inr_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "invoice_customer_id"
			, "ini_src"."invoice_date" AS "invoice_date"
			, "ini_src"."amount" AS "amount"
			, "ini_src"."discount" AS "discount"
			, "ini_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01"."invoices" "ini_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_inr_src" ON  "mex_inr_src"."record_type" = 'N'
	)
	, "prep_excep" AS 
	( 
		SELECT 
			  "load_init_data"."operation" AS "operation"
			, "load_init_data"."record_type" AS "record_type"
			, NULL ::int AS "load_cycle_id"
			, "load_init_data"."invoice_number" AS "invoice_number"
			, "load_init_data"."invoice_customer_id" AS "invoice_customer_id"
			, "load_init_data"."invoice_date" AS "invoice_date"
			, "load_init_data"."amount" AS "amount"
			, "load_init_data"."discount" AS "discount"
			, "load_init_data"."update_timestamp" AS "update_timestamp"
		FROM "load_init_data" "load_init_data"
		UNION ALL 
		SELECT 
			  'I' ::text AS "operation"
			, "mex_ext_src"."record_type" AS "record_type"
			, "mex_ext_src"."load_cycle_id" ::int AS "load_cycle_id"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "invoice_number"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "invoice_customer_id"
			, TO_DATE("mex_ext_src"."attribute_date", 'DD/MM/YYYY'::varchar) AS "invoice_date"
			, TO_NUMBER("mex_ext_src"."attribute_numeric", '999999999999D999999999'::varchar) AS "amount"
			, CAST("mex_ext_src"."attribute_integer" AS INTEGER) AS "discount"
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
			, "prep_excep"."invoice_number" AS "invoice_number"
			, "prep_excep"."invoice_customer_id" AS "invoice_customer_id"
			, COALESCE(UPPER( "prep_excep"."invoice_number"::text),"mex_src"."key_attribute_numeric") AS "invoice_number_bk"
			, "prep_excep"."invoice_date" AS "invoice_date"
			, "prep_excep"."amount" AS "amount"
			, "prep_excep"."discount" AS "discount"
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
		, "calculate_bk"."invoice_number" AS "invoice_number"
		, "calculate_bk"."invoice_customer_id" AS "invoice_customer_id"
		, "calculate_bk"."invoice_number_bk" AS "invoice_number_bk"
		, "calculate_bk"."invoice_date" AS "invoice_date"
		, "calculate_bk"."amount" AS "amount"
		, "calculate_bk"."discount" AS "discount"
		, "calculate_bk"."update_timestamp" AS "update_timestamp"
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 
