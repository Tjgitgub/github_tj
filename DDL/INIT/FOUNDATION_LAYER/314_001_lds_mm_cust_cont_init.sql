CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lds_mm_cust_cont_init"() 
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

BEGIN -- lds_tgt

	TRUNCATE TABLE "moto_scn01_fl"."lds_mm_cust_cont"  CASCADE;

	INSERT INTO "moto_scn01_fl"."lds_mm_cust_cont"(
		 "lnd_cust_cont_hkey"
		,"customers_hkey"
		,"contacts_hkey"
		,"party_number"
		,"contact_id"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"hash_diff"
		,"delete_flag"
		,"trans_timestamp"
		,"update_timestamp"
	)
	WITH "stg_dl_src" AS 
	( 
		SELECT 
			  "stg_dl_inr_src"."lnd_cust_cont_hkey" AS "lnd_cust_cont_hkey"
			, "stg_dl_inr_src"."customers_hkey" AS "customers_hkey"
			, "stg_dl_inr_src"."contacts_hkey" AS "contacts_hkey"
			, "stg_dl_inr_src"."party_number" AS "party_number"
			, "stg_dl_inr_src"."contact_id" AS "contact_id"
			, "stg_dl_inr_src"."load_date" AS "load_date"
			, "stg_dl_inr_src"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, UPPER(ENCODE(DIGEST(COALESCE('','~') ,'MD5'),'HEX')) AS "hash_diff"
			, 'N'::text AS "delete_flag"
			, "stg_dl_inr_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_dl_inr_src"."update_timestamp" AS "update_timestamp"
			, ROW_NUMBER()OVER(PARTITION BY "stg_dl_inr_src"."lnd_cust_cont_hkey" ORDER BY "stg_dl_inr_src"."load_date") AS "dummy"
		FROM "moto_mktg_scn01_stg"."party_contacts" "stg_dl_inr_src"
		WHERE  "stg_dl_inr_src"."error_code_cust_cont" = 0
	)
	SELECT 
		  "stg_dl_src"."lnd_cust_cont_hkey" AS "lnd_cust_cont_hkey"
		, "stg_dl_src"."customers_hkey" AS "customers_hkey"
		, "stg_dl_src"."contacts_hkey" AS "contacts_hkey"
		, "stg_dl_src"."party_number" AS "party_number"
		, "stg_dl_src"."contact_id" AS "contact_id"
		, "stg_dl_src"."load_date" AS "load_date"
		, "stg_dl_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_dl_src"."load_end_date" AS "load_end_date"
		, "stg_dl_src"."hash_diff" AS "hash_diff"
		, "stg_dl_src"."delete_flag" AS "delete_flag"
		, "stg_dl_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_dl_src"."update_timestamp" AS "update_timestamp"
	FROM "stg_dl_src" "stg_dl_src"
	WHERE  "stg_dl_src"."dummy" = 1
	;
END;



END;
$function$;
 
 
