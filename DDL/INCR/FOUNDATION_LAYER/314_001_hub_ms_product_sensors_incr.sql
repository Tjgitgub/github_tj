CREATE OR REPLACE FUNCTION "moto_scn01_proc"."hub_ms_product_sensors_incr"() 
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

	INSERT INTO "moto_scn01_fl"."hub_product_sensors"(
		 "product_sensors_hkey"
		,"load_date"
		,"load_cycle_id"
		,"vehicle_number_bk"
	)
	WITH "change_set" AS 
	( 
		SELECT 
			  "stg_src1"."product_sensors_hkey" AS "product_sensors_hkey"
			, "stg_src1"."load_date" AS "load_date"
			, "stg_src1"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "logposition"
			, "stg_src1"."vehicle_number_bk" AS "vehicle_number_bk"
			, 0 AS "general_order"
		FROM "moto_sales_scn01_stg"."product_sensors" "stg_src1"
		WHERE  "stg_src1"."record_type" = 'S'
	)
	, "min_load_time" AS 
	( 
		SELECT 
			  "change_set"."product_sensors_hkey" AS "product_sensors_hkey"
			, "change_set"."load_date" AS "load_date"
			, "change_set"."load_cycle_id" AS "load_cycle_id"
			, "change_set"."vehicle_number_bk" AS "vehicle_number_bk"
			, ROW_NUMBER()OVER(PARTITION BY "change_set"."product_sensors_hkey" ORDER BY "change_set"."general_order",
				"change_set"."load_date","change_set"."logposition") AS "dummy"
		FROM "change_set" "change_set"
	)
	SELECT 
		  "min_load_time"."product_sensors_hkey" AS "product_sensors_hkey"
		, "min_load_time"."load_date" AS "load_date"
		, "min_load_time"."load_cycle_id" AS "load_cycle_id"
		, "min_load_time"."vehicle_number_bk" AS "vehicle_number_bk"
	FROM "min_load_time" "min_load_time"
	LEFT OUTER JOIN "moto_scn01_fl"."hub_product_sensors" "hub_src" ON  "min_load_time"."product_sensors_hkey" = "hub_src"."product_sensors_hkey"
	WHERE  "min_load_time"."dummy" = 1 AND "hub_src"."product_sensors_hkey" is NULL
	;
END;



END;
$function$;
 
 
