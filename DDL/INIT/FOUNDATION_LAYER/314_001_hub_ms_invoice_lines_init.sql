CREATE OR REPLACE FUNCTION "moto_scn01_proc"."hub_ms_invoice_lines_init"() 
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

BEGIN -- hub_tgt

	TRUNCATE TABLE "moto_scn01_fl"."hub_invoice_lines"  CASCADE;

	INSERT INTO "moto_scn01_fl"."hub_invoice_lines"(
		 "invoice_lines_hkey"
		,"load_date"
		,"load_cycle_id"
		,"invoice_number_bk"
		,"invoice_line_number_bk"
	)
	WITH "change_set" AS 
	( 
		SELECT 
			  "stg_src1"."invoice_lines_hkey" AS "invoice_lines_hkey"
			, "stg_src1"."load_date" AS "load_date"
			, "stg_src1"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "logposition"
			, "stg_src1"."invoice_number_bk" AS "invoice_number_bk"
			, "stg_src1"."invoice_line_number_bk" AS "invoice_line_number_bk"
			, 0 AS "general_order"
		FROM "moto_sales_scn01_stg"."invoice_lines" "stg_src1"
	)
	, "min_load_time" AS 
	( 
		SELECT 
			  "change_set"."invoice_lines_hkey" AS "invoice_lines_hkey"
			, "change_set"."load_date" AS "load_date"
			, "change_set"."load_cycle_id" AS "load_cycle_id"
			, "change_set"."invoice_number_bk" AS "invoice_number_bk"
			, "change_set"."invoice_line_number_bk" AS "invoice_line_number_bk"
			, ROW_NUMBER()OVER(PARTITION BY "change_set"."invoice_lines_hkey" ORDER BY "change_set"."general_order",
				"change_set"."load_date","change_set"."logposition") AS "dummy"
		FROM "change_set" "change_set"
	)
	SELECT 
		  "min_load_time"."invoice_lines_hkey" AS "invoice_lines_hkey"
		, "min_load_time"."load_date" AS "load_date"
		, "min_load_time"."load_cycle_id" AS "load_cycle_id"
		, "min_load_time"."invoice_number_bk" AS "invoice_number_bk"
		, "min_load_time"."invoice_line_number_bk" AS "invoice_line_number_bk"
	FROM "min_load_time" "min_load_time"
	WHERE  "min_load_time"."dummy" = 1
	;
END;



END;
$function$;
 
 
