CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lnk_ms_inli_invo_init"() 
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

BEGIN -- lnk_tgt

	TRUNCATE TABLE "moto_scn01_fl"."lnk_inli_invo"  CASCADE;

	INSERT INTO "moto_scn01_fl"."lnk_inli_invo"(
		 "lnk_inli_invo_hkey"
		,"load_date"
		,"load_cycle_id"
		,"invoices_hkey"
		,"invoice_lines_hkey"
	)
	WITH "change_set" AS 
	( 
		SELECT 
			  "stg_src1"."lnk_inli_invo_hkey" AS "lnk_inli_invo_hkey"
			, "stg_src1"."load_date" AS "load_date"
			, "stg_src1"."load_cycle_id" AS "load_cycle_id"
			, "stg_src1"."invoices_hkey" AS "invoices_hkey"
			, "stg_src1"."invoice_lines_hkey" AS "invoice_lines_hkey"
		FROM "moto_sales_scn01_stg"."invoice_lines" "stg_src1"
	)
	, "min_load_time" AS 
	( 
		SELECT 
			  "change_set"."lnk_inli_invo_hkey" AS "lnk_inli_invo_hkey"
			, "change_set"."load_date" AS "load_date"
			, "change_set"."load_cycle_id" AS "load_cycle_id"
			, "change_set"."invoices_hkey" AS "invoices_hkey"
			, "change_set"."invoice_lines_hkey" AS "invoice_lines_hkey"
			, ROW_NUMBER()OVER(PARTITION BY "change_set"."lnk_inli_invo_hkey" ORDER BY "change_set"."load_cycle_id",
				"change_set"."load_date") AS "dummy"
		FROM "change_set" "change_set"
	)
	SELECT 
		  "min_load_time"."lnk_inli_invo_hkey" AS "lnk_inli_invo_hkey"
		, "min_load_time"."load_date" AS "load_date"
		, "min_load_time"."load_cycle_id" AS "load_cycle_id"
		, "min_load_time"."invoices_hkey" AS "invoices_hkey"
		, "min_load_time"."invoice_lines_hkey" AS "invoice_lines_hkey"
	FROM "min_load_time" "min_load_time"
	WHERE  "min_load_time"."dummy" = 1
	;
END;



END;
$function$;
 
 
