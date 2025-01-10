CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_mm_campaigns_incr"() 
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

	TRUNCATE TABLE "moto_mktg_scn01_stg"."campaigns"  CASCADE;

	INSERT INTO "moto_mktg_scn01_stg"."campaigns"(
		 "campaigns_hkey"
		,"load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"campaign_code"
		,"campaign_start_date"
		,"campaign_code_bk"
		,"campaign_start_date_bk"
		,"campaign_name"
		,"update_timestamp"
	)
	WITH "calc_hash_keys" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST( "ext_src"."campaign_code_bk" || '#' ||  "ext_src"."campaign_start_date_bk" || '#' ,
				'MD5'),'HEX')) AS "campaigns_hkey"
			, "ext_src"."load_date" AS "load_date"
			, "ext_src"."load_cycle_id" AS "load_cycle_id"
			, "ext_src"."trans_timestamp" AS "trans_timestamp"
			, "ext_src"."operation" AS "operation"
			, "ext_src"."record_type" AS "record_type"
			, "ext_src"."campaign_code" AS "campaign_code"
			, "ext_src"."campaign_start_date" AS "campaign_start_date"
			, "ext_src"."campaign_code_bk" AS "campaign_code_bk"
			, "ext_src"."campaign_start_date_bk" AS "campaign_start_date_bk"
			, "ext_src"."campaign_name" AS "campaign_name"
			, "ext_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_mktg_scn01_ext"."campaigns" "ext_src"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
	)
	SELECT 
		  "calc_hash_keys"."campaigns_hkey" AS "campaigns_hkey"
		, "calc_hash_keys"."load_date" AS "load_date"
		, "calc_hash_keys"."load_cycle_id" AS "load_cycle_id"
		, "calc_hash_keys"."trans_timestamp" AS "trans_timestamp"
		, "calc_hash_keys"."operation" AS "operation"
		, "calc_hash_keys"."record_type" AS "record_type"
		, "calc_hash_keys"."campaign_code" AS "campaign_code"
		, "calc_hash_keys"."campaign_start_date" AS "campaign_start_date"
		, "calc_hash_keys"."campaign_code_bk" AS "campaign_code_bk"
		, "calc_hash_keys"."campaign_start_date_bk" AS "campaign_start_date_bk"
		, "calc_hash_keys"."campaign_name" AS "campaign_name"
		, "calc_hash_keys"."update_timestamp" AS "update_timestamp"
	FROM "calc_hash_keys" "calc_hash_keys"
	;
END;



END;
$function$;
 
 
