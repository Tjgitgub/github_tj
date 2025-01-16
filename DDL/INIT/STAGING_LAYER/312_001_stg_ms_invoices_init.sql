CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_ms_invoices_init"() 
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

BEGIN -- stg_tgt

	TRUNCATE TABLE "moto_sales_scn01_stg"."invoices"  CASCADE;

	INSERT INTO "moto_sales_scn01_stg"."invoices"(
		 "invoices_hkey"
		,"customers_hkey"
		,"lnk_invo_cust_hkey"
		,"load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"invoice_number"
		,"invoice_customer_id"
		,"invoice_number_bk"
		,"national_person_id_fk_invoicecustomerid_bk"
		,"invoice_date"
		,"amount"
		,"discount"
		,"update_timestamp"
		,"error_code_invo_cust"
	)
	WITH "dist_fk1" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src1"."invoice_customer_id" AS "invoice_customer_id"
		FROM "moto_sales_scn01_ext"."invoices" "ext_dis_src1"
	)
	, "prep_find_bk_fk1" AS 
	( 
		SELECT 
			  UPPER(REPLACE(TRIM( "sat_src1"."national_person_id"),'#','\' || '#')) AS "national_person_id_bk"
			, "dist_fk1"."invoice_customer_id" AS "invoice_customer_id"
			, "sat_src1"."load_date" AS "load_date"
			, 1 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_scn01_fl"."sat_ms_customers_name" "sat_src1" ON  "dist_fk1"."invoice_customer_id" = "sat_src1"."customer_number"
		INNER JOIN "moto_scn01_fl"."hub_customers" "hub_src1" ON  "hub_src1"."customers_hkey" = "sat_src1"."customers_hkey"
		WHERE  "sat_src1"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
		UNION ALL 
		SELECT 
			  "ext_fkbk_src1"."national_person_id_bk" AS "national_person_id_bk"
			, "dist_fk1"."invoice_customer_id" AS "invoice_customer_id"
			, "ext_fkbk_src1"."load_date" AS "load_date"
			, 0 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_sales_scn01_ext"."customers" "ext_fkbk_src1" ON  "dist_fk1"."invoice_customer_id" = "ext_fkbk_src1"."customer_number"
	)
	, "order_bk_fk1" AS 
	( 
		SELECT 
			  "prep_find_bk_fk1"."national_person_id_bk" AS "national_person_id_bk"
			, "prep_find_bk_fk1"."invoice_customer_id" AS "invoice_customer_id"
			, ROW_NUMBER()OVER(PARTITION BY "prep_find_bk_fk1"."invoice_customer_id" ORDER BY "prep_find_bk_fk1"."general_order",
				"prep_find_bk_fk1"."load_date" DESC) AS "dummy"
		FROM "prep_find_bk_fk1" "prep_find_bk_fk1"
	)
	, "find_bk_fk1" AS 
	( 
		SELECT 
			  "order_bk_fk1"."national_person_id_bk" AS "national_person_id_bk"
			, "order_bk_fk1"."invoice_customer_id" AS "invoice_customer_id"
		FROM "order_bk_fk1" "order_bk_fk1"
		WHERE  "order_bk_fk1"."dummy" = 1
	)
	SELECT 
		  UPPER(ENCODE(DIGEST( "ext_src"."invoice_number_bk" || '#' ,'MD5'),'HEX')) AS "invoices_hkey"
		, UPPER(ENCODE(DIGEST( 'moto_sales_scn01' || '#' || COALESCE("find_bk_fk1"."national_person_id_bk","mex_src"."key_attribute_varchar")
			||'#' ,'MD5'),'HEX')) AS "customers_hkey"
		, UPPER(ENCODE(DIGEST( "ext_src"."invoice_number_bk" || '#' || 'moto_sales_scn01' || '#' || COALESCE("find_bk_fk1"."national_person_id_bk",
			"mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "lnk_invo_cust_hkey"
		, "ext_src"."load_date" AS "load_date"
		, "ext_src"."load_cycle_id" AS "load_cycle_id"
		, "ext_src"."trans_timestamp" AS "trans_timestamp"
		, "ext_src"."operation" AS "operation"
		, "ext_src"."record_type" AS "record_type"
		, "ext_src"."invoice_number" AS "invoice_number"
		, "ext_src"."invoice_customer_id" AS "invoice_customer_id"
		, "ext_src"."invoice_number_bk" AS "invoice_number_bk"
		, NULL::text AS "national_person_id_fk_invoicecustomerid_bk"
		, "ext_src"."invoice_date" AS "invoice_date"
		, "ext_src"."amount" AS "amount"
		, "ext_src"."discount" AS "discount"
		, "ext_src"."update_timestamp" AS "update_timestamp"
		, CASE WHEN "find_bk_fk1"."invoice_customer_id" IS NULL THEN 1 ELSE 0 END AS "error_code_invo_cust"
	FROM "moto_sales_scn01_ext"."invoices" "ext_src"
	INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
	LEFT OUTER JOIN "find_bk_fk1" "find_bk_fk1" ON  "ext_src"."invoice_customer_id" = "find_bk_fk1"."invoice_customer_id"
	;
END;


BEGIN -- ext_err_tgt

	TRUNCATE TABLE "moto_sales_scn01_ext"."invoices_err"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."invoices_err"(
		 "load_date"
		,"load_cycle_id"
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
	SELECT 
		  CASE WHEN "stg_err_src"."error_code_invo_cust" = 1 THEN "stg_err_src"."load_date" + interval'1 microsecond'   ELSE "stg_err_src"."load_date" END AS "load_date"
		, "stg_err_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_err_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_err_src"."operation" AS "operation"
		, "stg_err_src"."record_type" AS "record_type"
		, "stg_err_src"."invoice_number" AS "invoice_number"
		, "stg_err_src"."invoice_customer_id" AS "invoice_customer_id"
		, "stg_err_src"."invoice_number_bk" AS "invoice_number_bk"
		, "stg_err_src"."invoice_date" AS "invoice_date"
		, "stg_err_src"."amount" AS "amount"
		, "stg_err_src"."discount" AS "discount"
		, "stg_err_src"."update_timestamp" AS "update_timestamp"
		, "stg_err_src"."error_code_invo_cust" AS "error_code_invo_cust"
	FROM "moto_sales_scn01_stg"."invoices" "stg_err_src"
	WHERE ( "stg_err_src"."error_code_invo_cust" > 0)AND "stg_err_src"."load_cycle_id" >= 0
	;
END;



END;
$function$;
 
 
