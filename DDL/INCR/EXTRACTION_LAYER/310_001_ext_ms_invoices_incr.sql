CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_ms_invoices_incr"() 
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
		,"error_code_invo_cust"
	)
	WITH "calculate_bk" AS 
	( 
		SELECT 
			  "tdfv_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("tdfv_src"."operation","mex_src"."attribute_varchar") AS "operation"
			, "tdfv_src"."record_type" AS "record_type"
			, "tdfv_src"."invoice_number" AS "invoice_number"
			, "tdfv_src"."invoice_customer_id" AS "invoice_customer_id"
			, COALESCE(UPPER( "tdfv_src"."invoice_number"::text),"mex_src"."key_attribute_numeric") AS "invoice_number_bk"
			, "tdfv_src"."invoice_date" AS "invoice_date"
			, "tdfv_src"."amount" AS "amount"
			, "tdfv_src"."discount" AS "discount"
			, "tdfv_src"."update_timestamp" AS "update_timestamp"
			, 1 AS "error_code_invo_cust"
		FROM "moto_sales_scn01_dfv"."vw_invoices" "tdfv_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	, "ext_union" AS 
	( 
		SELECT 
			  "lci_src"."load_cycle_id" AS "load_cycle_id"
			, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "calculate_bk"."invoice_number_bk"  ORDER BY  "calculate_bk"."trans_timestamp")
				* interval'2 microsecond'   AS "load_date"
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
			, "calculate_bk"."error_code_invo_cust" AS "error_code_invo_cust"
		FROM "calculate_bk" "calculate_bk"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		UNION 
		SELECT 
			  "err_lci_src"."load_cycle_id" AS "load_cycle_id"
			, "ext_err_src"."load_date" AS "load_date"
			, "ext_err_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("ext_err_src"."operation",'U') AS "operation"
			, 'E' AS "record_type"
			, "ext_err_src"."invoice_number" AS "invoice_number"
			, "ext_err_src"."invoice_customer_id" AS "invoice_customer_id"
			, "ext_err_src"."invoice_number_bk" AS "invoice_number_bk"
			, "ext_err_src"."invoice_date" AS "invoice_date"
			, "ext_err_src"."amount" AS "amount"
			, "ext_err_src"."discount" AS "discount"
			, "ext_err_src"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "ext_err_src"."error_code_invo_cust" = 0 THEN 2 WHEN "ext_err_src"."error_code_invo_cust" =
				1 THEN 3 ELSE"ext_err_src"."error_code_invo_cust" END AS "error_code_invo_cust"
		FROM "moto_sales_scn01_ext"."invoices_err" "ext_err_src"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "err_lci_src" ON  1 = 1
		WHERE  "ext_err_src"."error_code_invo_cust" < 4
	)
	SELECT 
		  "ext_union"."load_cycle_id" AS "load_cycle_id"
		, "ext_union"."load_date" AS "load_date"
		, "ext_union"."trans_timestamp" AS "trans_timestamp"
		, "ext_union"."operation" AS "operation"
		, "ext_union"."record_type" AS "record_type"
		, "ext_union"."invoice_number" AS "invoice_number"
		, "ext_union"."invoice_customer_id" AS "invoice_customer_id"
		, "ext_union"."invoice_number_bk" AS "invoice_number_bk"
		, "ext_union"."invoice_date" AS "invoice_date"
		, "ext_union"."amount" AS "amount"
		, "ext_union"."discount" AS "discount"
		, "ext_union"."update_timestamp" AS "update_timestamp"
		, "ext_union"."error_code_invo_cust" AS "error_code_invo_cust"
	FROM "ext_union" "ext_union"
	;
END;



END;
$function$;
 
 
