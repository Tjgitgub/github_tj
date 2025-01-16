CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lds_ms_custaddresses_incr"() 
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

BEGIN -- lds_temp_tgt

	TRUNCATE TABLE "moto_sales_scn01_stg"."lds_ms_cust_addresses_tmp"  CASCADE;

	INSERT INTO "moto_sales_scn01_stg"."lds_ms_cust_addresses_tmp"(
		 "lnd_cust_addresses_hkey"
		,"customers_hkey"
		,"addresses_hkey"
		,"address_number"
		,"customer_number"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"hash_diff"
		,"record_type"
		,"source"
		,"equal"
		,"delete_flag"
		,"trans_timestamp"
		,"address_type_seq"
		,"update_timestamp"
	)
	WITH "dist_stg" AS 
	( 
		SELECT 
			  "stg_dis_src"."lnd_cust_addresses_hkey" AS "lnd_cust_addresses_hkey"
			, "stg_dis_src"."load_cycle_id" AS "load_cycle_id"
			, "stg_dis_src"."address_type_seq" AS "address_type_seq"
			, MIN("stg_dis_src"."load_date") AS "min_load_timestamp"
			, MIN("stg_dis_src"."error_code_cust_addresses") AS "error_code_cust_addresses"
		FROM "moto_sales_scn01_stg"."cust_addresses" "stg_dis_src"
		GROUP BY  "stg_dis_src"."lnd_cust_addresses_hkey",  "stg_dis_src"."load_cycle_id",  "stg_dis_src"."address_type_seq"
	)
	, "last_lds" AS 
	( 
		SELECT 
			  "last_lds_src"."lnd_cust_addresses_hkey" AS "lnd_cust_addresses_hkey"
			, "dist_stg"."load_cycle_id" AS "load_cycle_id"
			, "dist_stg"."address_type_seq" AS "address_type_seq"
			, MAX("last_lds_src"."load_date") AS "max_load_timestamp"
			, MIN("dist_stg"."error_code_cust_addresses") AS "error_code_cust_addresses"
		FROM "moto_scn01_fl"."lds_ms_cust_addresses" "last_lds_src"
		INNER JOIN "moto_scn01_fl"."lnd_cust_addresses" "last_lnd_src" ON  "last_lds_src"."lnd_cust_addresses_hkey" = "last_lnd_src"."lnd_cust_addresses_hkey"
		INNER JOIN "dist_stg" "dist_stg" ON  "last_lnd_src"."lnd_cust_addresses_hkey" = "dist_stg"."lnd_cust_addresses_hkey"
		WHERE  "last_lds_src"."load_date" <= "dist_stg"."min_load_timestamp"
		GROUP BY  "last_lds_src"."lnd_cust_addresses_hkey",  "dist_stg"."load_cycle_id",  "dist_stg"."address_type_seq"
	)
	, "temp_table_set" AS 
	( 
		SELECT 
			  "stg_temp_src"."lnd_cust_addresses_hkey" AS "lnd_cust_addresses_hkey"
			, "stg_temp_src"."customers_hkey" AS "customers_hkey"
			, "stg_temp_src"."addresses_hkey" AS "addresses_hkey"
			, "stg_temp_src"."address_number" AS "address_number"
			, "stg_temp_src"."customer_number" AS "customer_number"
			, "stg_temp_src"."load_date" AS "load_date"
			, "stg_temp_src"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, UPPER(ENCODE(DIGEST(COALESCE('','~') ,'MD5'),'HEX')) AS "hash_diff"
			, "stg_temp_src"."record_type" AS "record_type"
			, 'STG' AS "source"
			, 1 AS "origin_id"
			, CASE WHEN "stg_temp_src"."operation" = 'D' THEN 'Y'::text ELSE 'N'::text END AS "delete_flag"
			, "stg_temp_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_temp_src"."address_type_seq" AS "address_type_seq"
			, "stg_temp_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01_stg"."cust_addresses" "stg_temp_src"
		WHERE  "stg_temp_src"."error_code_cust_addresses" in(0,1)
		UNION ALL 
		SELECT 
			  "lds_src"."lnd_cust_addresses_hkey" AS "lnd_cust_addresses_hkey"
			, "lnd_src"."customers_hkey" AS "customers_hkey"
			, "lnd_src"."addresses_hkey" AS "addresses_hkey"
			, "lds_src"."address_number" AS "address_number"
			, "lds_src"."customer_number" AS "customer_number"
			, "lds_src"."load_date" AS "load_date"
			, "lds_src"."load_cycle_id" AS "load_cycle_id"
			, "lds_src"."load_end_date" AS "load_end_date"
			, "lds_src"."hash_diff" AS "hash_diff"
			, 'SAT' AS "record_type"
			, 'LDS' AS "source"
			, 0 AS "origin_id"
			, "lds_src"."delete_flag" AS "delete_flag"
			, "lds_src"."trans_timestamp" AS "trans_timestamp"
			, "lds_src"."address_type_seq" AS "address_type_seq"
			, "lds_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_scn01_fl"."lds_ms_cust_addresses" "lds_src"
		INNER JOIN "moto_scn01_fl"."lnd_cust_addresses" "lnd_src" ON  "lds_src"."lnd_cust_addresses_hkey" = "lnd_src"."lnd_cust_addresses_hkey"
		INNER JOIN "last_lds" "last_lds" ON  "lds_src"."lnd_cust_addresses_hkey" = "last_lds"."lnd_cust_addresses_hkey" AND "lds_src"."address_type_seq" = 
			"last_lds"."address_type_seq"
		WHERE  "lds_src"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) OR("last_lds"."error_code_cust_addresses" = 0 AND "lds_src"."load_date" >= "last_lds"."max_load_timestamp")
	)
	SELECT 
		  "temp_table_set"."lnd_cust_addresses_hkey" AS "lnd_cust_addresses_hkey"
		, "temp_table_set"."customers_hkey" AS "customers_hkey"
		, "temp_table_set"."addresses_hkey" AS "addresses_hkey"
		, "temp_table_set"."address_number" AS "address_number"
		, "temp_table_set"."customer_number" AS "customer_number"
		, "temp_table_set"."load_date" AS "load_date"
		, "temp_table_set"."load_cycle_id" AS "load_cycle_id"
		, "temp_table_set"."load_end_date" AS "load_end_date"
		, "temp_table_set"."hash_diff" AS "hash_diff"
		, "temp_table_set"."record_type" AS "record_type"
		, "temp_table_set"."source" AS "source"
		, CASE WHEN "temp_table_set"."source" = 'STG' AND "temp_table_set"."delete_flag"::text || "temp_table_set"."hash_diff"::text =
			LAG("temp_table_set"."delete_flag"::text || "temp_table_set"."hash_diff"::text,1)OVER(PARTITION BY "temp_table_set"."lnd_cust_addresses_hkey","temp_table_set"."address_type_seq" ORDER BY "temp_table_set"."load_date","temp_table_set"."origin_id")THEN 1 ELSE 0 END AS "equal"
		, "temp_table_set"."delete_flag" AS "delete_flag"
		, "temp_table_set"."trans_timestamp" AS "trans_timestamp"
		, "temp_table_set"."address_type_seq" AS "address_type_seq"
		, "temp_table_set"."update_timestamp" AS "update_timestamp"
	FROM "temp_table_set" "temp_table_set"
	;
END;


BEGIN -- lds_inur_tgt

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
	SELECT 
		  "lds_temp_src_inur"."lnd_cust_addresses_hkey" AS "lnd_cust_addresses_hkey"
		, "lds_temp_src_inur"."customers_hkey" AS "customers_hkey"
		, "lds_temp_src_inur"."addresses_hkey" AS "addresses_hkey"
		, "lds_temp_src_inur"."address_number" AS "address_number"
		, "lds_temp_src_inur"."customer_number" AS "customer_number"
		, "lds_temp_src_inur"."load_date" AS "load_date"
		, "lds_temp_src_inur"."load_cycle_id" AS "load_cycle_id"
		, "lds_temp_src_inur"."load_end_date" AS "load_end_date"
		, "lds_temp_src_inur"."hash_diff" AS "hash_diff"
		, "lds_temp_src_inur"."delete_flag" AS "delete_flag"
		, "lds_temp_src_inur"."trans_timestamp" AS "trans_timestamp"
		, "lds_temp_src_inur"."address_type_seq" AS "address_type_seq"
		, "lds_temp_src_inur"."update_timestamp" AS "update_timestamp"
	FROM "moto_sales_scn01_stg"."lds_ms_cust_addresses_tmp" "lds_temp_src_inur"
	WHERE  "lds_temp_src_inur"."source" = 'STG' AND "lds_temp_src_inur"."equal" = 0
	;
END;


BEGIN -- lds_ed_tgt

	WITH "calc_load_end_date" AS 
	( 
		SELECT 
			  "lds_temp_src_us"."lnd_cust_addresses_hkey" AS "lnd_cust_addresses_hkey"
			, "lds_temp_src_us"."load_date" AS "load_date"
			, CASE WHEN "lds_temp_src_us"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 
				'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN "lci_src"."load_cycle_id" ELSE "lds_temp_src_us"."load_cycle_id" END AS "load_cycle_id"
			, COALESCE(LEAD("lds_temp_src_us"."load_date",1)OVER(PARTITION BY "lds_temp_src_us"."lnd_cust_addresses_hkey",
				"lds_temp_src_us"."address_type_seq" ORDER BY "lds_temp_src_us"."load_date","lds_temp_src_us"."load_cycle_id"), TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
			, "lds_temp_src_us"."address_type_seq" AS "address_type_seq"
		FROM "moto_sales_scn01_stg"."lds_ms_cust_addresses_tmp" "lds_temp_src_us"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		WHERE  "lds_temp_src_us"."equal" = 0
	)
	, "filter_load_end_date" AS 
	( 
		SELECT 
			  "calc_load_end_date"."lnd_cust_addresses_hkey" AS "lnd_cust_addresses_hkey"
			, "calc_load_end_date"."load_date" AS "load_date"
			, "calc_load_end_date"."load_cycle_id" AS "load_cycle_id"
			, "calc_load_end_date"."load_end_date" AS "load_end_date"
			, "calc_load_end_date"."address_type_seq" AS "address_type_seq"
		FROM "calc_load_end_date" "calc_load_end_date"
		WHERE  "calc_load_end_date"."load_end_date" != TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
	)
	UPDATE "moto_scn01_fl"."lds_ms_cust_addresses" "lds_ed_tgt"
	SET 
		 "load_cycle_id" =  "filter_load_end_date"."load_cycle_id"
		,"load_end_date" =  "filter_load_end_date"."load_end_date"
	FROM  "filter_load_end_date"
	WHERE "lds_ed_tgt"."lnd_cust_addresses_hkey" =  "filter_load_end_date"."lnd_cust_addresses_hkey"
	  AND "lds_ed_tgt"."load_date" =  "filter_load_end_date"."load_date"
	  AND "lds_ed_tgt"."address_type_seq" =  "filter_load_end_date"."address_type_seq"
	;
END;



END;
$function$;
 
 
