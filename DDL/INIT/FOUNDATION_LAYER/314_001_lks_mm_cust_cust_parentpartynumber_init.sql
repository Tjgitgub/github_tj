CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lks_mm_cust_cust_parentpartynumber_init"() 
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

BEGIN -- lks_tgt

	TRUNCATE TABLE "moto_scn01_fl"."lks_mm_cust_cust_parentpartynumber"  CASCADE;

	INSERT INTO "moto_scn01_fl"."lks_mm_cust_cust_parentpartynumber"(
		 "lnk_cust_cust_parentpartynumber_hkey"
		,"customers_parentpartynumber_hkey"
		,"customers_hkey"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"delete_flag"
		,"trans_timestamp"
		,"party_number"
		,"parent_party_number"
	)
	WITH "stg_src" AS 
	( 
		SELECT 
			  "stg_inr_src"."lnk_cust_cust_parentpartynumber_hkey" AS "lnk_cust_cust_parentpartynumber_hkey"
			, "stg_inr_src"."customers_hkey" AS "customers_hkey"
			, "stg_inr_src"."customers_parentpartynumber_hkey" AS "customers_parentpartynumber_hkey"
			, "stg_inr_src"."load_date" AS "load_date"
			, "stg_inr_src"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, 'N'::text AS "delete_flag"
			, "stg_inr_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_inr_src"."party_number" AS "party_number"
			, "stg_inr_src"."parent_party_number" AS "parent_party_number"
			, ROW_NUMBER()OVER(PARTITION BY "stg_inr_src"."customers_hkey" ORDER BY "stg_inr_src"."load_date") AS "dummy"
		FROM "moto_mktg_scn01_stg"."party" "stg_inr_src"
		WHERE  "stg_inr_src"."error_code_cust_cust_parentpartynumber" = 0
	)
	SELECT 
		  "stg_src"."lnk_cust_cust_parentpartynumber_hkey" AS "lnk_cust_cust_parentpartynumber_hkey"
		, "stg_src"."customers_parentpartynumber_hkey" AS "customers_parentpartynumber_hkey"
		, "stg_src"."customers_hkey" AS "customers_hkey"
		, "stg_src"."load_date" AS "load_date"
		, "stg_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_src"."load_end_date" AS "load_end_date"
		, "stg_src"."delete_flag" AS "delete_flag"
		, "stg_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_src"."party_number" AS "party_number"
		, "stg_src"."parent_party_number" AS "parent_party_number"
	FROM "stg_src" "stg_src"
	WHERE  "stg_src"."dummy" = 1
	;
END;



END;
$function$;
 
 
