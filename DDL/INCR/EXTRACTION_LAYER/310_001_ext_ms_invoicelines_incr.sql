CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_ms_invoicelines_incr"() 
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
		,"error_code_inli_prod"
		,"error_code_inli_part"
	)
	WITH "calculate_bk" AS 
	( 
		SELECT 
			  "tdfv_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("tdfv_src"."operation","mex_src"."attribute_varchar") AS "operation"
			, "tdfv_src"."record_type" AS "record_type"
			, "tdfv_src"."invoice_line_number" AS "invoice_line_number"
			, "tdfv_src"."invoice_number" AS "invoice_number"
			, "tdfv_src"."product_id" AS "product_id"
			, "tdfv_src"."part_id" AS "part_id"
			, COALESCE(UPPER( "tdfv_src"."invoice_number"::text),"mex_src"."key_attribute_numeric") AS "invoice_number_bk"
			, COALESCE(UPPER( "tdfv_src"."invoice_line_number"::text),"mex_src"."key_attribute_numeric") AS "invoice_line_number_bk"
			, UPPER( "tdfv_src"."invoice_number"::text) AS "invoice_number_fk_invoicenumber_bk"
			, "tdfv_src"."amount" AS "amount"
			, "tdfv_src"."quantity" AS "quantity"
			, "tdfv_src"."unit_price" AS "unit_price"
			, "tdfv_src"."update_timestamp" AS "update_timestamp"
			, 1 AS "error_code_inli_prod"
			, 1 AS "error_code_inli_part"
		FROM "moto_sales_scn01_dfv"."vw_invoice_lines" "tdfv_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	, "ext_union" AS 
	( 
		SELECT 
			  "lci_src"."load_cycle_id" AS "load_cycle_id"
			, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "calculate_bk"."invoice_number_bk" ,  "calculate_bk"."invoice_line_number_bk"  ORDER BY  "calculate_bk"."trans_timestamp")
				* interval'2 microsecond'   AS "load_date"
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
			, "calculate_bk"."error_code_inli_prod" AS "error_code_inli_prod"
			, "calculate_bk"."error_code_inli_part" AS "error_code_inli_part"
		FROM "calculate_bk" "calculate_bk"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		UNION 
		SELECT 
			  "err_lci_src"."load_cycle_id" AS "load_cycle_id"
			, "ext_err_src"."load_date" AS "load_date"
			, "ext_err_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("ext_err_src"."operation",'U') AS "operation"
			, 'E' AS "record_type"
			, "ext_err_src"."invoice_line_number" AS "invoice_line_number"
			, "ext_err_src"."invoice_number" AS "invoice_number"
			, "ext_err_src"."product_id" AS "product_id"
			, "ext_err_src"."part_id" AS "part_id"
			, "ext_err_src"."invoice_number_bk" AS "invoice_number_bk"
			, "ext_err_src"."invoice_line_number_bk" AS "invoice_line_number_bk"
			, "ext_err_src"."invoice_number_fk_invoicenumber_bk" AS "invoice_number_fk_invoicenumber_bk"
			, "ext_err_src"."amount" AS "amount"
			, "ext_err_src"."quantity" AS "quantity"
			, "ext_err_src"."unit_price" AS "unit_price"
			, "ext_err_src"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "ext_err_src"."error_code_inli_prod" = 0 THEN 2 WHEN "ext_err_src"."error_code_inli_prod" =
				1 THEN 3 ELSE "ext_err_src"."error_code_inli_prod" END AS "error_code_inli_prod"
			, CASE WHEN "ext_err_src"."error_code_inli_part" = 0 THEN 2 WHEN "ext_err_src"."error_code_inli_part" =
				1 THEN 3 ELSE "ext_err_src"."error_code_inli_part" END AS "error_code_inli_part"
		FROM "moto_sales_scn01_ext"."invoice_lines_err" "ext_err_src"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "err_lci_src" ON  1 = 1
		WHERE  "ext_err_src"."error_code_inli_prod" < 4 AND  "ext_err_src"."error_code_inli_part" < 4
	)
	SELECT 
		  "ext_union"."load_cycle_id" AS "load_cycle_id"
		, "ext_union"."load_date" AS "load_date"
		, "ext_union"."trans_timestamp" AS "trans_timestamp"
		, "ext_union"."operation" AS "operation"
		, "ext_union"."record_type" AS "record_type"
		, "ext_union"."invoice_line_number" AS "invoice_line_number"
		, "ext_union"."invoice_number" AS "invoice_number"
		, "ext_union"."product_id" AS "product_id"
		, "ext_union"."part_id" AS "part_id"
		, "ext_union"."invoice_number_bk" AS "invoice_number_bk"
		, "ext_union"."invoice_line_number_bk" AS "invoice_line_number_bk"
		, "ext_union"."invoice_number_fk_invoicenumber_bk" AS "invoice_number_fk_invoicenumber_bk"
		, "ext_union"."amount" AS "amount"
		, "ext_union"."quantity" AS "quantity"
		, "ext_union"."unit_price" AS "unit_price"
		, "ext_union"."update_timestamp" AS "update_timestamp"
		, "ext_union"."error_code_inli_prod" AS "error_code_inli_prod"
		, "ext_union"."error_code_inli_part" AS "error_code_inli_part"
	FROM "ext_union" "ext_union"
	;
END;



END;
$function$;
 
 
