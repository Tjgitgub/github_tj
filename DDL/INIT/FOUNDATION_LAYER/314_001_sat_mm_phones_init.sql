CREATE OR REPLACE FUNCTION "moto_scn01_proc"."sat_mm_phones_init"() 
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

BEGIN -- sat_tgt

	TRUNCATE TABLE "moto_scn01_fl"."sat_mm_phones"  CASCADE;

	INSERT INTO "moto_scn01_fl"."sat_mm_phones"(
		 "contacts_hkey"
		,"load_date"
		,"load_end_date"
		,"load_cycle_id"
		,"hash_diff"
		,"delete_flag"
		,"trans_timestamp"
		,"contact_id"
		,"phone_number"
		,"update_timestamp"
	)
	WITH "stg_src" AS 
	( 
		SELECT 
			  "stg_inr_src"."contacts_hkey" AS "contacts_hkey"
			, "stg_inr_src"."load_date" AS "load_date"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, "stg_inr_src"."load_cycle_id" AS "load_cycle_id"
			, UPPER(ENCODE(DIGEST(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "stg_inr_src"."phone_number"),'~'),'#','\' || 
				'#'))|| '#','#' || '~'),'~') ,'MD5'),'HEX')) AS "hash_diff"
			, 'N'::text AS "delete_flag"
			, "stg_inr_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_inr_src"."contact_id" AS "contact_id"
			, "stg_inr_src"."phone_number" AS "phone_number"
			, "stg_inr_src"."update_timestamp" AS "update_timestamp"
			, ROW_NUMBER()OVER(PARTITION BY "stg_inr_src"."contacts_hkey" ORDER BY "stg_inr_src"."load_date") AS "dummy"
		FROM "moto_mktg_scn01_stg"."phones" "stg_inr_src"
	)
	SELECT 
		  "stg_src"."contacts_hkey" AS "contacts_hkey"
		, "stg_src"."load_date" AS "load_date"
		, "stg_src"."load_end_date" AS "load_end_date"
		, "stg_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_src"."hash_diff" AS "hash_diff"
		, "stg_src"."delete_flag" AS "delete_flag"
		, "stg_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_src"."contact_id" AS "contact_id"
		, "stg_src"."phone_number" AS "phone_number"
		, "stg_src"."update_timestamp" AS "update_timestamp"
	FROM "stg_src" "stg_src"
	WHERE  "stg_src"."dummy" = 1
	;
END;



END;
$function$;
 
 
