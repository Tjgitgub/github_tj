CREATE OR REPLACE FUNCTION "moto_scn01_proc"."nhl_ms_payments_init"() 
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

BEGIN -- nhl_tgt

	TRUNCATE TABLE "moto_scn01_fl"."nhl_ms_payments"  CASCADE;

	INSERT INTO "moto_scn01_fl"."nhl_ms_payments"(
		 "nhl_payments_hkey"
		,"customers_hkey"
		,"invoices_hkey"
		,"load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"customer_number"
		,"invoice_number"
		,"amount"
		,"date_time"
		,"transaction_id"
		,"update_timestamp"
	)
	WITH "stg_dl_src" AS 
	( 
		SELECT 
			  "stg_dl_inr_src"."nhl_payments_hkey" AS "nhl_payments_hkey"
			, "stg_dl_inr_src"."customers_hkey" AS "customers_hkey"
			, "stg_dl_inr_src"."invoices_hkey" AS "invoices_hkey"
			, "stg_dl_inr_src"."load_date" AS "load_date"
			, "stg_dl_inr_src"."load_cycle_id" AS "load_cycle_id"
			, "stg_dl_inr_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_dl_inr_src"."operation" AS "operation"
			, "stg_dl_inr_src"."invoice_number" AS "invoice_number"
			, "stg_dl_inr_src"."customer_number" AS "customer_number"
			, "stg_dl_inr_src"."transaction_id" AS "transaction_id"
			, "stg_dl_inr_src"."date_time" AS "date_time"
			, "stg_dl_inr_src"."amount" AS "amount"
			, "stg_dl_inr_src"."update_timestamp" AS "update_timestamp"
			, ROW_NUMBER()OVER(PARTITION BY "stg_dl_inr_src"."nhl_payments_hkey","stg_dl_inr_src"."load_date" ORDER BY "stg_dl_inr_src"."load_date") AS "dummy"
		FROM "moto_sales_scn01_stg"."payments" "stg_dl_inr_src"
	)
	SELECT 
		  "stg_dl_src"."nhl_payments_hkey" AS "nhl_payments_hkey"
		, "stg_dl_src"."customers_hkey" AS "customers_hkey"
		, "stg_dl_src"."invoices_hkey" AS "invoices_hkey"
		, "stg_dl_src"."load_date" AS "load_date"
		, "stg_dl_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_dl_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_dl_src"."customer_number" AS "customer_number"
		, "stg_dl_src"."invoice_number" AS "invoice_number"
		, "stg_dl_src"."amount" AS "amount"
		, "stg_dl_src"."date_time" AS "date_time"
		, "stg_dl_src"."transaction_id" AS "transaction_id"
		, "stg_dl_src"."update_timestamp" AS "update_timestamp"
	FROM "stg_dl_src" "stg_dl_src"
	WHERE  "stg_dl_src"."dummy" = 1
	;
END;



END;
$function$;
 
 
