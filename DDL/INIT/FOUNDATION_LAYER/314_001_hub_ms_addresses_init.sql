CREATE OR REPLACE FUNCTION "moto_scn01_proc"."hub_ms_addresses_init"() 
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

BEGIN -- hub_tgt

	INSERT INTO "moto_scn01_fl"."hub_addresses"(
		 "addresses_hkey"
		,"load_date"
		,"load_cycle_id"
		,"street_name_bk"
		,"street_number_bk"
		,"postal_code_bk"
		,"city_bk"
	)
	WITH "change_set" AS 
	( 
		SELECT 
			  "stg_src1"."addresses_hkey" AS "addresses_hkey"
			, "stg_src1"."load_date" AS "load_date"
			, "stg_src1"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "logposition"
			, "stg_src1"."street_name_bk" AS "street_name_bk"
			, "stg_src1"."street_number_bk" AS "street_number_bk"
			, "stg_src1"."postal_code_bk" AS "postal_code_bk"
			, "stg_src1"."city_bk" AS "city_bk"
			, 0 AS "general_order"
		FROM "moto_sales_scn01_stg"."addresses" "stg_src1"
	)
	, "min_load_time" AS 
	( 
		SELECT 
			  "change_set"."addresses_hkey" AS "addresses_hkey"
			, "change_set"."load_date" AS "load_date"
			, "change_set"."load_cycle_id" AS "load_cycle_id"
			, "change_set"."street_name_bk" AS "street_name_bk"
			, "change_set"."street_number_bk" AS "street_number_bk"
			, "change_set"."postal_code_bk" AS "postal_code_bk"
			, "change_set"."city_bk" AS "city_bk"
			, ROW_NUMBER()OVER(PARTITION BY "change_set"."addresses_hkey" ORDER BY "change_set"."general_order",
				"change_set"."load_date","change_set"."logposition") AS "dummy"
		FROM "change_set" "change_set"
	)
	SELECT 
		  "min_load_time"."addresses_hkey" AS "addresses_hkey"
		, "min_load_time"."load_date" AS "load_date"
		, "min_load_time"."load_cycle_id" AS "load_cycle_id"
		, "min_load_time"."street_name_bk" AS "street_name_bk"
		, "min_load_time"."street_number_bk" AS "street_number_bk"
		, "min_load_time"."postal_code_bk" AS "postal_code_bk"
		, "min_load_time"."city_bk" AS "city_bk"
	FROM "min_load_time" "min_load_time"
	LEFT OUTER JOIN "moto_scn01_fl"."hub_addresses" "hub_src" ON  "min_load_time"."addresses_hkey" = "hub_src"."addresses_hkey"
	WHERE  "min_load_time"."dummy" = 1 AND "hub_src"."addresses_hkey" IS NULL
	;
END;



END;
$function$;
 
 
