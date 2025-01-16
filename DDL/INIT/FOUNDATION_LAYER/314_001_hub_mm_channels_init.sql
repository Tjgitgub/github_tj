CREATE OR REPLACE FUNCTION "moto_scn01_proc"."hub_mm_channels_init"() 
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

Vaultspeed version: 5.7.2.16, generation date: 2025/01/16 15:01:59
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/16 14:56:23, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46
 */


BEGIN 

BEGIN -- hub_tgt

	TRUNCATE TABLE "moto_scn01_fl"."hub_channels"  CASCADE;

	INSERT INTO "moto_scn01_fl"."hub_channels"(
		 "channels_hkey"
		,"load_date"
		,"load_cycle_id"
		,"channel_code_bk"
	)
	WITH "change_set" AS 
	( 
		SELECT 
			  "stg_src1"."channels_hkey" AS "channels_hkey"
			, "stg_src1"."load_date" AS "load_date"
			, "stg_src1"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "logposition"
			, "stg_src1"."channel_code_bk" AS "channel_code_bk"
			, 0 AS "general_order"
		FROM "moto_mktg_scn01_stg"."channels" "stg_src1"
	)
	, "min_load_time" AS 
	( 
		SELECT 
			  "change_set"."channels_hkey" AS "channels_hkey"
			, "change_set"."load_date" AS "load_date"
			, "change_set"."load_cycle_id" AS "load_cycle_id"
			, "change_set"."channel_code_bk" AS "channel_code_bk"
			, ROW_NUMBER()OVER(PARTITION BY "change_set"."channels_hkey" ORDER BY "change_set"."general_order",
				"change_set"."load_date","change_set"."logposition") AS "dummy"
		FROM "change_set" "change_set"
	)
	SELECT 
		  "min_load_time"."channels_hkey" AS "channels_hkey"
		, "min_load_time"."load_date" AS "load_date"
		, "min_load_time"."load_cycle_id" AS "load_cycle_id"
		, "min_load_time"."channel_code_bk" AS "channel_code_bk"
	FROM "min_load_time" "min_load_time"
	WHERE  "min_load_time"."dummy" = 1
	;
END;



END;
$function$;
 
 
