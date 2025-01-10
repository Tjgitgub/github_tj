CREATE OR REPLACE FUNCTION "moto_scn01_proc"."hub_ms_parts_incr"() 
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

	INSERT INTO "moto_scn01_fl"."hub_parts"(
		 "parts_hkey"
		,"load_date"
		,"load_cycle_id"
		,"part_number_bk"
		,"part_langua_code_bk"
	)
	WITH "change_set" AS 
	( 
		SELECT 
			  "stg_src1"."parts_hkey" AS "parts_hkey"
			, "stg_src1"."load_date" AS "load_date"
			, "stg_src1"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "logposition"
			, "stg_src1"."part_number_bk" AS "part_number_bk"
			, "stg_src1"."part_langua_code_bk" AS "part_langua_code_bk"
			, 0 AS "general_order"
		FROM "moto_sales_scn01_stg"."parts" "stg_src1"
		WHERE  "stg_src1"."record_type" = 'S'
	)
	, "min_load_time" AS 
	( 
		SELECT 
			  "change_set"."parts_hkey" AS "parts_hkey"
			, "change_set"."load_date" AS "load_date"
			, "change_set"."load_cycle_id" AS "load_cycle_id"
			, "change_set"."part_number_bk" AS "part_number_bk"
			, "change_set"."part_langua_code_bk" AS "part_langua_code_bk"
			, ROW_NUMBER()OVER(PARTITION BY "change_set"."parts_hkey" ORDER BY "change_set"."general_order","change_set"."load_date",
				"change_set"."logposition") AS "dummy"
		FROM "change_set" "change_set"
	)
	SELECT 
		  "min_load_time"."parts_hkey" AS "parts_hkey"
		, "min_load_time"."load_date" AS "load_date"
		, "min_load_time"."load_cycle_id" AS "load_cycle_id"
		, "min_load_time"."part_number_bk" AS "part_number_bk"
		, "min_load_time"."part_langua_code_bk" AS "part_langua_code_bk"
	FROM "min_load_time" "min_load_time"
	LEFT OUTER JOIN "moto_scn01_fl"."hub_parts" "hub_src" ON  "min_load_time"."parts_hkey" = "hub_src"."parts_hkey"
	WHERE  "min_load_time"."dummy" = 1 AND "hub_src"."parts_hkey" is NULL
	;
END;



END;
$function$;
 
 
