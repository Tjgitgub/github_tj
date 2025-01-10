CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lks_ms_prod_prod_rpid_init"() 
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

	TRUNCATE TABLE "moto_scn01_fl"."lks_ms_prod_prod_rpid"  CASCADE;

	INSERT INTO "moto_scn01_fl"."lks_ms_prod_prod_rpid"(
		 "lnk_prod_prod_rpid_hkey"
		,"products_rpid_hkey"
		,"products_hkey"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"delete_flag"
		,"trans_timestamp"
		,"product_id"
		,"replacement_product_id"
	)
	WITH "stg_src" AS 
	( 
		SELECT 
			  "stg_inr_src"."lnk_prod_prod_rpid_hkey" AS "lnk_prod_prod_rpid_hkey"
			, "stg_inr_src"."products_hkey" AS "products_hkey"
			, "stg_inr_src"."products_rpid_hkey" AS "products_rpid_hkey"
			, "stg_inr_src"."load_date" AS "load_date"
			, "stg_inr_src"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, 'N'::text AS "delete_flag"
			, "stg_inr_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_inr_src"."product_id" AS "product_id"
			, "stg_inr_src"."replacement_product_id" AS "replacement_product_id"
			, ROW_NUMBER()OVER(PARTITION BY "stg_inr_src"."products_hkey" ORDER BY "stg_inr_src"."load_date") AS "dummy"
		FROM "moto_sales_scn01_stg"."products" "stg_inr_src"
	)
	SELECT 
		  "stg_src"."lnk_prod_prod_rpid_hkey" AS "lnk_prod_prod_rpid_hkey"
		, "stg_src"."products_rpid_hkey" AS "products_rpid_hkey"
		, "stg_src"."products_hkey" AS "products_hkey"
		, "stg_src"."load_date" AS "load_date"
		, "stg_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_src"."load_end_date" AS "load_end_date"
		, "stg_src"."delete_flag" AS "delete_flag"
		, "stg_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_src"."product_id" AS "product_id"
		, "stg_src"."replacement_product_id" AS "replacement_product_id"
	FROM "stg_src" "stg_src"
	WHERE  "stg_src"."dummy" = 1
	;
END;



END;
$function$;
 
 
