CREATE OR REPLACE FUNCTION "moto_scn01_proc"."nhl_ms_payments_incr"() 
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
	FROM "moto_sales_scn01_stg"."payments" "stg_dl_src"
	LEFT OUTER JOIN "moto_scn01_fl"."nhl_ms_payments" "nhl_src" ON  "stg_dl_src"."nhl_payments_hkey" = "nhl_src"."nhl_payments_hkey" AND "stg_dl_src"."load_date" = "nhl_src"."load_date"
	WHERE  "nhl_src"."nhl_payments_hkey" IS NULL AND "stg_dl_src"."error_code_payments" in(0,1)
	;
END;



END;
$function$;
 
 
