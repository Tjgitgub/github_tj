CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lnk_ms_invo_cust_init"() 
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

BEGIN -- lnk_tgt

	TRUNCATE TABLE "moto_scn01_fl"."lnk_invo_cust"  CASCADE;

	INSERT INTO "moto_scn01_fl"."lnk_invo_cust"(
		 "lnk_invo_cust_hkey"
		,"load_date"
		,"load_cycle_id"
		,"customers_hkey"
		,"invoices_hkey"
	)
	WITH "change_set" AS 
	( 
		SELECT 
			  "stg_src1"."lnk_invo_cust_hkey" AS "lnk_invo_cust_hkey"
			, "stg_src1"."load_date" AS "load_date"
			, "stg_src1"."load_cycle_id" AS "load_cycle_id"
			, "stg_src1"."customers_hkey" AS "customers_hkey"
			, "stg_src1"."invoices_hkey" AS "invoices_hkey"
		FROM "moto_sales_scn01_stg"."invoices" "stg_src1"
	)
	, "min_load_time" AS 
	( 
		SELECT 
			  "change_set"."lnk_invo_cust_hkey" AS "lnk_invo_cust_hkey"
			, "change_set"."load_date" AS "load_date"
			, "change_set"."load_cycle_id" AS "load_cycle_id"
			, "change_set"."customers_hkey" AS "customers_hkey"
			, "change_set"."invoices_hkey" AS "invoices_hkey"
			, ROW_NUMBER()OVER(PARTITION BY "change_set"."lnk_invo_cust_hkey" ORDER BY "change_set"."load_cycle_id",
				"change_set"."load_date") AS "dummy"
		FROM "change_set" "change_set"
	)
	SELECT 
		  "min_load_time"."lnk_invo_cust_hkey" AS "lnk_invo_cust_hkey"
		, "min_load_time"."load_date" AS "load_date"
		, "min_load_time"."load_cycle_id" AS "load_cycle_id"
		, "min_load_time"."customers_hkey" AS "customers_hkey"
		, "min_load_time"."invoices_hkey" AS "invoices_hkey"
	FROM "min_load_time" "min_load_time"
	WHERE  "min_load_time"."dummy" = 1
	;
END;



END;
$function$;
 
 
