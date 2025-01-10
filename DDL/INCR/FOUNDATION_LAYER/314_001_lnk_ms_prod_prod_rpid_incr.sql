CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lnk_ms_prod_prod_rpid_incr"() 
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

	INSERT INTO "moto_scn01_fl"."lnk_prod_prod_rpid"(
		 "lnk_prod_prod_rpid_hkey"
		,"load_date"
		,"load_cycle_id"
		,"products_rpid_hkey"
		,"products_hkey"
	)
	WITH "change_set" AS 
	( 
		SELECT 
			  "stg_src1"."lnk_prod_prod_rpid_hkey" AS "lnk_prod_prod_rpid_hkey"
			, "stg_src1"."load_date" AS "load_date"
			, "stg_src1"."load_cycle_id" AS "load_cycle_id"
			, "stg_src1"."products_rpid_hkey" AS "products_rpid_hkey"
			, "stg_src1"."products_hkey" AS "products_hkey"
			, 0 AS "logposition"
		FROM "moto_sales_scn01_stg"."products" "stg_src1"
		WHERE  "stg_src1"."error_code_prod_prod_rpid" in(0,1)
	)
	, "min_load_time" AS 
	( 
		SELECT 
			  "change_set"."lnk_prod_prod_rpid_hkey" AS "lnk_prod_prod_rpid_hkey"
			, "change_set"."load_date" AS "load_date"
			, "change_set"."load_cycle_id" AS "load_cycle_id"
			, "change_set"."products_rpid_hkey" AS "products_rpid_hkey"
			, "change_set"."products_hkey" AS "products_hkey"
			, ROW_NUMBER()OVER(PARTITION BY "change_set"."lnk_prod_prod_rpid_hkey" ORDER BY "change_set"."load_date",
				"change_set"."logposition") AS "dummy"
		FROM "change_set" "change_set"
	)
	SELECT 
		  "min_load_time"."lnk_prod_prod_rpid_hkey" AS "lnk_prod_prod_rpid_hkey"
		, "min_load_time"."load_date" AS "load_date"
		, "min_load_time"."load_cycle_id" AS "load_cycle_id"
		, "min_load_time"."products_rpid_hkey" AS "products_rpid_hkey"
		, "min_load_time"."products_hkey" AS "products_hkey"
	FROM "min_load_time" "min_load_time"
	LEFT OUTER JOIN "moto_scn01_fl"."lnk_prod_prod_rpid" "lnk_src" ON  "min_load_time"."lnk_prod_prod_rpid_hkey" = "lnk_src"."lnk_prod_prod_rpid_hkey"
	WHERE  "lnk_src"."lnk_prod_prod_rpid_hkey" IS NULL AND "min_load_time"."dummy" = 1
	;
END;



END;
$function$;
 
 
