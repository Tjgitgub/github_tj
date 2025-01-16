CREATE OR REPLACE FUNCTION "moto_scn01_proc"."sat_mm_cust_init"() 
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

BEGIN -- sat_tgt

	TRUNCATE TABLE "moto_scn01_fl"."sat_mm_customers"  CASCADE;

	INSERT INTO "moto_scn01_fl"."sat_mm_customers"(
		 "customers_hkey"
		,"load_date"
		,"load_end_date"
		,"load_cycle_id"
		,"hash_diff"
		,"delete_flag"
		,"trans_timestamp"
		,"party_number"
		,"address_number"
		,"parent_party_number"
		,"name"
		,"birthdate"
		,"gender"
		,"party_type_code"
		,"comments"
		,"update_timestamp"
	)
	WITH "stg_src" AS 
	( 
		SELECT 
			  "stg_inr_src"."customers_hkey" AS "customers_hkey"
			, "stg_inr_src"."load_date" AS "load_date"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, "stg_inr_src"."load_cycle_id" AS "load_cycle_id"
			, UPPER(ENCODE(DIGEST(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "stg_inr_src"."comments"),'~'),'#','\' || 
				'#'))|| '#' ||  UPPER(REPLACE(COALESCE(TRIM( "stg_inr_src"."address_number"::text),'~'),'#','\' || '#'))|| '#','#' || '~'),'~') ,'MD5'),'HEX')) AS "hash_diff"
			, 'N'::text AS "delete_flag"
			, "stg_inr_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_inr_src"."party_number" AS "party_number"
			, "stg_inr_src"."address_number" AS "address_number"
			, "stg_inr_src"."parent_party_number" AS "parent_party_number"
			, "stg_inr_src"."name" AS "name"
			, "stg_inr_src"."birthdate" AS "birthdate"
			, "stg_inr_src"."gender" AS "gender"
			, "stg_inr_src"."party_type_code" AS "party_type_code"
			, "stg_inr_src"."comments" AS "comments"
			, "stg_inr_src"."update_timestamp" AS "update_timestamp"
			, ROW_NUMBER()OVER(PARTITION BY "stg_inr_src"."customers_hkey" ORDER BY "stg_inr_src"."load_date") AS "dummy"
		FROM "moto_mktg_scn01_stg"."party" "stg_inr_src"
	)
	SELECT 
		  "stg_src"."customers_hkey" AS "customers_hkey"
		, "stg_src"."load_date" AS "load_date"
		, "stg_src"."load_end_date" AS "load_end_date"
		, "stg_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_src"."hash_diff" AS "hash_diff"
		, "stg_src"."delete_flag" AS "delete_flag"
		, "stg_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_src"."party_number" AS "party_number"
		, "stg_src"."address_number" AS "address_number"
		, "stg_src"."parent_party_number" AS "parent_party_number"
		, "stg_src"."name" AS "name"
		, "stg_src"."birthdate" AS "birthdate"
		, "stg_src"."gender" AS "gender"
		, "stg_src"."party_type_code" AS "party_type_code"
		, "stg_src"."comments" AS "comments"
		, "stg_src"."update_timestamp" AS "update_timestamp"
	FROM "stg_src" "stg_src"
	WHERE  "stg_src"."dummy" = 1
	;
END;



END;
$function$;
 
 
