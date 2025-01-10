CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lks_ms_invo_cust_init"() 
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

BEGIN -- lks_tgt

	TRUNCATE TABLE "moto_scn01_fl"."lks_ms_invo_cust"  CASCADE;

	INSERT INTO "moto_scn01_fl"."lks_ms_invo_cust"(
		 "lnk_invo_cust_hkey"
		,"customers_hkey"
		,"invoices_hkey"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"delete_flag"
		,"trans_timestamp"
		,"invoice_number"
		,"invoice_customer_id"
	)
	WITH "stg_src" AS 
	( 
		SELECT 
			  "stg_inr_src"."lnk_invo_cust_hkey" AS "lnk_invo_cust_hkey"
			, "stg_inr_src"."invoices_hkey" AS "invoices_hkey"
			, "stg_inr_src"."customers_hkey" AS "customers_hkey"
			, "stg_inr_src"."load_date" AS "load_date"
			, "stg_inr_src"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, 'N'::text AS "delete_flag"
			, "stg_inr_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_inr_src"."invoice_number" AS "invoice_number"
			, "stg_inr_src"."invoice_customer_id" AS "invoice_customer_id"
			, ROW_NUMBER()OVER(PARTITION BY "stg_inr_src"."invoices_hkey" ORDER BY "stg_inr_src"."load_date") AS "dummy"
		FROM "moto_sales_scn01_stg"."invoices" "stg_inr_src"
	)
	SELECT 
		  "stg_src"."lnk_invo_cust_hkey" AS "lnk_invo_cust_hkey"
		, "stg_src"."customers_hkey" AS "customers_hkey"
		, "stg_src"."invoices_hkey" AS "invoices_hkey"
		, "stg_src"."load_date" AS "load_date"
		, "stg_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_src"."load_end_date" AS "load_end_date"
		, "stg_src"."delete_flag" AS "delete_flag"
		, "stg_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_src"."invoice_number" AS "invoice_number"
		, "stg_src"."invoice_customer_id" AS "invoice_customer_id"
	FROM "stg_src" "stg_src"
	WHERE  "stg_src"."dummy" = 1
	;
END;



END;
$function$;
 
 
