CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_mm_channels_init"() 
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

BEGIN -- stg_tgt

	TRUNCATE TABLE "moto_mktg_scn01_stg"."channels"  CASCADE;

	INSERT INTO "moto_mktg_scn01_stg"."channels"(
		 "channels_hkey"
		,"load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"channel_id"
		,"channel_code_bk"
		,"channel_description"
		,"update_timestamp"
	)
	SELECT 
		  UPPER(ENCODE(DIGEST( "ext_src"."channel_code_bk" || '#' ,'MD5'),'HEX')) AS "channels_hkey"
		, "ext_src"."load_date" AS "load_date"
		, "ext_src"."load_cycle_id" AS "load_cycle_id"
		, "ext_src"."trans_timestamp" AS "trans_timestamp"
		, "ext_src"."operation" AS "operation"
		, "ext_src"."record_type" AS "record_type"
		, "ext_src"."channel_id" AS "channel_id"
		, "ext_src"."channel_code_bk" AS "channel_code_bk"
		, "ext_src"."channel_description" AS "channel_description"
		, "ext_src"."update_timestamp" AS "update_timestamp"
	FROM "moto_mktg_scn01_ext"."channels" "ext_src"
	INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
	;
END;



END;
$function$;
 
 
