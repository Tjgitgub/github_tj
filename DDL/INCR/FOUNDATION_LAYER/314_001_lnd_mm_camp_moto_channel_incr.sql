CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lnd_mm_camp_moto_channel_incr"() 
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

Vaultspeed version: 5.7.2.14, generation date: 2025/01/09 12:48:54
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/09 09:40:46, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51
 */


BEGIN 

BEGIN -- lnd_tgt

	INSERT INTO "moto_scn01_fl"."lnd_camp_moto_channel"(
		 "lnd_camp_moto_channel_hkey"
		,"load_date"
		,"load_cycle_id"
		,"channels_hkey"
		,"campaigns_hkey"
	)
	WITH "change_set" AS 
	( 
		SELECT 
			  "stg_src1"."lnd_camp_moto_channel_hkey" AS "lnd_camp_moto_channel_hkey"
			, "stg_src1"."load_date" AS "load_date"
			, "stg_src1"."load_cycle_id" AS "load_cycle_id"
			, "stg_src1"."channels_hkey" AS "channels_hkey"
			, "stg_src1"."campaigns_hkey" AS "campaigns_hkey"
			, 0 AS "logposition"
		FROM "moto_mktg_scn01_stg"."camp_moto_channel" "stg_src1"
		WHERE  "stg_src1"."error_code_camp_moto_channel" = 0
	)
	, "min_load_time" AS 
	( 
		SELECT 
			  "change_set"."lnd_camp_moto_channel_hkey" AS "lnd_camp_moto_channel_hkey"
			, "change_set"."load_date" AS "load_date"
			, "change_set"."load_cycle_id" AS "load_cycle_id"
			, "change_set"."channels_hkey" AS "channels_hkey"
			, "change_set"."campaigns_hkey" AS "campaigns_hkey"
			, ROW_NUMBER()OVER(PARTITION BY "change_set"."lnd_camp_moto_channel_hkey" ORDER BY "change_set"."load_date",
				"change_set"."logposition") AS "dummy"
		FROM "change_set" "change_set"
	)
	SELECT 
		  "min_load_time"."lnd_camp_moto_channel_hkey" AS "lnd_camp_moto_channel_hkey"
		, "min_load_time"."load_date" AS "load_date"
		, "min_load_time"."load_cycle_id" AS "load_cycle_id"
		, "min_load_time"."channels_hkey" AS "channels_hkey"
		, "min_load_time"."campaigns_hkey" AS "campaigns_hkey"
	FROM "min_load_time" "min_load_time"
	LEFT OUTER JOIN "moto_scn01_fl"."lnd_camp_moto_channel" "lnd_src" ON  "min_load_time"."lnd_camp_moto_channel_hkey" = "lnd_src"."lnd_camp_moto_channel_hkey"
	WHERE  "lnd_src"."lnd_camp_moto_channel_hkey" IS NULL AND "min_load_time"."dummy" = 1
	;
END;



END;
$function$;
 
 