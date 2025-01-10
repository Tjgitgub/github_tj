CREATE OR REPLACE FUNCTION "moto_scn01_proc"."sat_ms_prod_init"() 
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

BEGIN -- sat_tgt

	TRUNCATE TABLE "moto_scn01_fl"."sat_ms_products"  CASCADE;

	INSERT INTO "moto_scn01_fl"."sat_ms_products"(
		 "products_hkey"
		,"load_date"
		,"load_end_date"
		,"load_cycle_id"
		,"hash_diff"
		,"delete_flag"
		,"trans_timestamp"
		,"product_id"
		,"product_intro_date"
		,"product_name"
		,"update_timestamp"
	)
	WITH "stg_src" AS 
	( 
		SELECT 
			  "stg_inr_src"."products_hkey" AS "products_hkey"
			, "stg_inr_src"."load_date" AS "load_date"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, "stg_inr_src"."load_cycle_id" AS "load_cycle_id"
			, UPPER(ENCODE(DIGEST(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("stg_inr_src"."product_intro_date",
				 'DD/MM/YYYY'::varchar)),'~'),'#','\' || '#'))|| '#' ||  UPPER(REPLACE(COALESCE(TRIM( "stg_inr_src"."product_name"),'~'),'#','\' || '#'))|| '#','#' || '~'),'~') ,'MD5'),'HEX')) AS "hash_diff"
			, 'N'::text AS "delete_flag"
			, "stg_inr_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_inr_src"."product_id" AS "product_id"
			, "stg_inr_src"."product_intro_date" AS "product_intro_date"
			, "stg_inr_src"."product_name" AS "product_name"
			, "stg_inr_src"."update_timestamp" AS "update_timestamp"
			, ROW_NUMBER()OVER(PARTITION BY "stg_inr_src"."products_hkey" ORDER BY "stg_inr_src"."load_date") AS "dummy"
		FROM "moto_sales_scn01_stg"."products" "stg_inr_src"
	)
	SELECT 
		  "stg_src"."products_hkey" AS "products_hkey"
		, "stg_src"."load_date" AS "load_date"
		, "stg_src"."load_end_date" AS "load_end_date"
		, "stg_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_src"."hash_diff" AS "hash_diff"
		, "stg_src"."delete_flag" AS "delete_flag"
		, "stg_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_src"."product_id" AS "product_id"
		, "stg_src"."product_intro_date" AS "product_intro_date"
		, "stg_src"."product_name" AS "product_name"
		, "stg_src"."update_timestamp" AS "update_timestamp"
	FROM "stg_src" "stg_src"
	WHERE  "stg_src"."dummy" = 1
	;
END;



END;
$function$;
 
 
