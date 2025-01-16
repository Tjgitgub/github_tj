CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lks_ms_prse_prod_init"() 
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

BEGIN -- lks_tgt

	TRUNCATE TABLE "moto_scn01_fl"."lks_ms_prse_prod"  CASCADE;

	INSERT INTO "moto_scn01_fl"."lks_ms_prse_prod"(
		 "lnk_prse_prod_hkey"
		,"products_hkey"
		,"product_sensors_hkey"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"delete_flag"
		,"vehicle_number"
		,"product_number"
	)
	WITH "stg_src" AS 
	( 
		SELECT 
			  "stg_inr_src"."lnk_prse_prod_hkey" AS "lnk_prse_prod_hkey"
			, "stg_inr_src"."product_sensors_hkey" AS "product_sensors_hkey"
			, "stg_inr_src"."products_hkey" AS "products_hkey"
			, "stg_inr_src"."load_date" AS "load_date"
			, "stg_inr_src"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, 'N'::text AS "delete_flag"
			, "stg_inr_src"."vehicle_number" AS "vehicle_number"
			, "stg_inr_src"."product_number" AS "product_number"
			, ROW_NUMBER()OVER(PARTITION BY "stg_inr_src"."product_sensors_hkey" ORDER BY "stg_inr_src"."load_date") AS "dummy"
		FROM "moto_sales_scn01_stg"."product_sensors" "stg_inr_src"
	)
	SELECT 
		  "stg_src"."lnk_prse_prod_hkey" AS "lnk_prse_prod_hkey"
		, "stg_src"."products_hkey" AS "products_hkey"
		, "stg_src"."product_sensors_hkey" AS "product_sensors_hkey"
		, "stg_src"."load_date" AS "load_date"
		, "stg_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_src"."load_end_date" AS "load_end_date"
		, "stg_src"."delete_flag" AS "delete_flag"
		, "stg_src"."vehicle_number" AS "vehicle_number"
		, "stg_src"."product_number" AS "product_number"
	FROM "stg_src" "stg_src"
	WHERE  "stg_src"."dummy" = 1
	;
END;



END;
$function$;
 
 
