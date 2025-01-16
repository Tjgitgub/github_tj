CREATE OR REPLACE FUNCTION "moto_scn01_proc"."sat_ms_inli_init"() 
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

BEGIN -- sat_tgt

	TRUNCATE TABLE "moto_scn01_fl"."sat_ms_invoice_lines"  CASCADE;

	INSERT INTO "moto_scn01_fl"."sat_ms_invoice_lines"(
		 "invoice_lines_hkey"
		,"load_date"
		,"load_end_date"
		,"load_cycle_id"
		,"hash_diff"
		,"delete_flag"
		,"trans_timestamp"
		,"invoice_line_number"
		,"invoice_number"
		,"amount"
		,"quantity"
		,"unit_price"
		,"update_timestamp"
	)
	WITH "stg_src" AS 
	( 
		SELECT 
			  "stg_inr_src"."invoice_lines_hkey" AS "invoice_lines_hkey"
			, "stg_inr_src"."load_date" AS "load_date"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, "stg_inr_src"."load_cycle_id" AS "load_cycle_id"
			, UPPER(ENCODE(DIGEST(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "stg_inr_src"."amount"::text),'~'),'#','\' || 
				'#'))|| '#' ||  UPPER(REPLACE(COALESCE(TRIM( "stg_inr_src"."quantity"::text),'~'),'#','\' || '#'))|| '#' ||  UPPER(REPLACE(COALESCE(TRIM( "stg_inr_src"."unit_price"::text),'~'),'#','\' || '#'))|| '#','#' || '~'),'~') ,'MD5'),'HEX')) AS "hash_diff"
			, 'N'::text AS "delete_flag"
			, "stg_inr_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_inr_src"."invoice_line_number" AS "invoice_line_number"
			, "stg_inr_src"."invoice_number" AS "invoice_number"
			, "stg_inr_src"."amount" AS "amount"
			, "stg_inr_src"."quantity" AS "quantity"
			, "stg_inr_src"."unit_price" AS "unit_price"
			, "stg_inr_src"."update_timestamp" AS "update_timestamp"
			, ROW_NUMBER()OVER(PARTITION BY "stg_inr_src"."invoice_lines_hkey" ORDER BY "stg_inr_src"."load_date") AS "dummy"
		FROM "moto_sales_scn01_stg"."invoice_lines" "stg_inr_src"
	)
	SELECT 
		  "stg_src"."invoice_lines_hkey" AS "invoice_lines_hkey"
		, "stg_src"."load_date" AS "load_date"
		, "stg_src"."load_end_date" AS "load_end_date"
		, "stg_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_src"."hash_diff" AS "hash_diff"
		, "stg_src"."delete_flag" AS "delete_flag"
		, "stg_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_src"."invoice_line_number" AS "invoice_line_number"
		, "stg_src"."invoice_number" AS "invoice_number"
		, "stg_src"."amount" AS "amount"
		, "stg_src"."quantity" AS "quantity"
		, "stg_src"."unit_price" AS "unit_price"
		, "stg_src"."update_timestamp" AS "update_timestamp"
	FROM "stg_src" "stg_src"
	WHERE  "stg_src"."dummy" = 1
	;
END;



END;
$function$;
 
 
