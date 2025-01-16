CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lds_ms_custaddresses_init"() 
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

BEGIN -- lds_tgt

	TRUNCATE TABLE "moto_scn01_fl"."lds_ms_cust_addresses"  CASCADE;

	INSERT INTO "moto_scn01_fl"."lds_ms_cust_addresses"(
		 "lnd_cust_addresses_hkey"
		,"customers_hkey"
		,"addresses_hkey"
		,"address_number"
		,"customer_number"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"hash_diff"
		,"delete_flag"
		,"trans_timestamp"
		,"address_type_seq"
		,"update_timestamp"
	)
	WITH "stg_dl_src" AS 
	( 
		SELECT 
			  "stg_dl_inr_src"."lnd_cust_addresses_hkey" AS "lnd_cust_addresses_hkey"
			, "stg_dl_inr_src"."customers_hkey" AS "customers_hkey"
			, "stg_dl_inr_src"."addresses_hkey" AS "addresses_hkey"
			, "stg_dl_inr_src"."address_number" AS "address_number"
			, "stg_dl_inr_src"."customer_number" AS "customer_number"
			, "stg_dl_inr_src"."load_date" AS "load_date"
			, "stg_dl_inr_src"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, UPPER(ENCODE(DIGEST(COALESCE('','~') ,'MD5'),'HEX')) AS "hash_diff"
			, 'N'::text AS "delete_flag"
			, "stg_dl_inr_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_dl_inr_src"."address_type_seq" AS "address_type_seq"
			, "stg_dl_inr_src"."update_timestamp" AS "update_timestamp"
			, ROW_NUMBER()OVER(PARTITION BY "stg_dl_inr_src"."lnd_cust_addresses_hkey","stg_dl_inr_src"."address_type_seq" ORDER BY "stg_dl_inr_src"."load_date") AS "dummy"
		FROM "moto_sales_scn01_stg"."cust_addresses" "stg_dl_inr_src"
	)
	SELECT 
		  "stg_dl_src"."lnd_cust_addresses_hkey" AS "lnd_cust_addresses_hkey"
		, "stg_dl_src"."customers_hkey" AS "customers_hkey"
		, "stg_dl_src"."addresses_hkey" AS "addresses_hkey"
		, "stg_dl_src"."address_number" AS "address_number"
		, "stg_dl_src"."customer_number" AS "customer_number"
		, "stg_dl_src"."load_date" AS "load_date"
		, "stg_dl_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_dl_src"."load_end_date" AS "load_end_date"
		, "stg_dl_src"."hash_diff" AS "hash_diff"
		, "stg_dl_src"."delete_flag" AS "delete_flag"
		, "stg_dl_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_dl_src"."address_type_seq" AS "address_type_seq"
		, "stg_dl_src"."update_timestamp" AS "update_timestamp"
	FROM "stg_dl_src" "stg_dl_src"
	WHERE  "stg_dl_src"."dummy" = 1
	;
END;



END;
$function$;
 
 
