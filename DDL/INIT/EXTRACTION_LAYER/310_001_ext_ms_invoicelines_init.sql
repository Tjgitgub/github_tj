CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_ms_invoicelines_init"() 
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

	TRUNCATE TABLE "moto_sales_scn01_ext"."invoice_lines"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."invoice_lines"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"invoice_line_number"
		,"invoice_number"
		,"product_id"
		,"part_id"
		,"invoice_number_bk"
		,"invoice_line_number_bk"
		,"invoice_number_fk_invoicenumber_bk"
		,"amount"
		,"quantity"
		,"unit_price"
		,"update_timestamp"
	)
	WITH "load_init_data" AS 
	( 
		SELECT 
			  'I' ::text AS "operation"
			, 'S'::text AS "record_type"
			, COALESCE("ini_src"."invoice_line_number", TO_NUMBER("mex_inr_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "invoice_line_number"
			, COALESCE("ini_src"."invoice_number", TO_NUMBER("mex_inr_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "invoice_number"
			, COALESCE("ini_src"."product_id", TO_NUMBER("mex_inr_src"."key_attribute_numeric", '999999999999D999999999'::varchar)
				) AS "product_id"
			, COALESCE("ini_src"."part_id", TO_NUMBER("mex_inr_src"."key_attribute_numeric", '999999999999D999999999'::varchar)
				) AS "part_id"
			, "ini_src"."amount" AS "amount"
			, "ini_src"."quantity" AS "quantity"
			, "ini_src"."unit_price" AS "unit_price"
			, "ini_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01"."invoice_lines" "ini_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_inr_src" ON  "mex_inr_src"."record_type" = 'N'
	)
	, "prep_excep" AS 
	( 
		SELECT 
			  "load_init_data"."operation" AS "operation"
			, "load_init_data"."record_type" AS "record_type"
			, NULL ::int AS "load_cycle_id"
			, "load_init_data"."invoice_line_number" AS "invoice_line_number"
			, "load_init_data"."invoice_number" AS "invoice_number"
			, "load_init_data"."product_id" AS "product_id"
			, "load_init_data"."part_id" AS "part_id"
			, "load_init_data"."amount" AS "amount"
			, "load_init_data"."quantity" AS "quantity"
			, "load_init_data"."unit_price" AS "unit_price"
			, "load_init_data"."update_timestamp" AS "update_timestamp"
		FROM "load_init_data" "load_init_data"
		UNION ALL 
		SELECT 
			  'I' ::text AS "operation"
			, "mex_ext_src"."record_type" AS "record_type"
			, "mex_ext_src"."load_cycle_id" ::int AS "load_cycle_id"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "invoice_line_number"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "invoice_number"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "product_id"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "part_id"
			, TO_NUMBER("mex_ext_src"."attribute_numeric", '999999999999D999999999'::varchar) AS "amount"
			, TO_NUMBER("mex_ext_src"."attribute_numeric", '999999999999D999999999'::varchar) AS "quantity"
			, TO_NUMBER("mex_ext_src"."attribute_numeric", '999999999999D999999999'::varchar) AS "unit_price"
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
			, "prep_excep"."invoice_line_number" AS "invoice_line_number"
			, "prep_excep"."invoice_number" AS "invoice_number"
			, "prep_excep"."product_id" AS "product_id"
			, "prep_excep"."part_id" AS "part_id"
			, COALESCE(UPPER( "prep_excep"."invoice_number"::text),"mex_src"."key_attribute_numeric") AS "invoice_number_bk"
			, COALESCE(UPPER( "prep_excep"."invoice_line_number"::text),"mex_src"."key_attribute_numeric") AS "invoice_line_number_bk"
			, UPPER( "prep_excep"."invoice_number"::text) AS "invoice_number_fk_invoicenumber_bk"
			, "prep_excep"."amount" AS "amount"
			, "prep_excep"."quantity" AS "quantity"
			, "prep_excep"."unit_price" AS "unit_price"
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
		, "calculate_bk"."invoice_line_number" AS "invoice_line_number"
		, "calculate_bk"."invoice_number" AS "invoice_number"
		, "calculate_bk"."product_id" AS "product_id"
		, "calculate_bk"."part_id" AS "part_id"
		, "calculate_bk"."invoice_number_bk" AS "invoice_number_bk"
		, "calculate_bk"."invoice_line_number_bk" AS "invoice_line_number_bk"
		, "calculate_bk"."invoice_number_fk_invoicenumber_bk" AS "invoice_number_fk_invoicenumber_bk"
		, "calculate_bk"."amount" AS "amount"
		, "calculate_bk"."quantity" AS "quantity"
		, "calculate_bk"."unit_price" AS "unit_price"
		, "calculate_bk"."update_timestamp" AS "update_timestamp"
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 
