CREATE OR REPLACE FUNCTION "moto_scn01_proc"."sat_mm_cust_incr"() 
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

BEGIN -- sat_inr_tgt

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
	SELECT 
		  "stg_inr_src"."customers_hkey" AS "customers_hkey"
		, "stg_inr_src"."load_date" AS "load_date"
		, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
		, "stg_inr_src"."load_cycle_id" AS "load_cycle_id"
		, UPPER(ENCODE(DIGEST(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "stg_inr_src"."comments"),'~'),'#','\' || '#')
			)|| '#' ||  UPPER(REPLACE(COALESCE(TRIM( "stg_inr_src"."address_number"::text),'~'),'#','\' || '#'))|| '#','#' || '~'),'~') ,'MD5'),'HEX')) AS "hash_diff"
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
	FROM "moto_mktg_scn01_stg"."party" "stg_inr_src"
	LEFT OUTER JOIN "moto_scn01_fl"."sat_mm_customers" "sat_inr_src" ON  "stg_inr_src"."customers_hkey" = "sat_inr_src"."customers_hkey" AND "stg_inr_src"."load_date" = "sat_inr_src"."load_date"
	WHERE  "stg_inr_src"."record_type" = 'S' AND "sat_inr_src"."customers_hkey" IS NULL AND "stg_inr_src"."operation" = 'I'
	;
END;


BEGIN -- sat_temp_tgt

	TRUNCATE TABLE "moto_mktg_scn01_stg"."sat_mm_customers_tmp"  CASCADE;

	INSERT INTO "moto_mktg_scn01_stg"."sat_mm_customers_tmp"(
		 "customers_hkey"
		,"load_date"
		,"load_end_date"
		,"load_cycle_id"
		,"record_type"
		,"source"
		,"equal"
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
	WITH "dist_stg" AS 
	( 
		SELECT DISTINCT 
 			  "stg_dis_src"."customers_hkey" AS "customers_hkey"
			, "stg_dis_src"."load_cycle_id" AS "load_cycle_id"
		FROM "moto_mktg_scn01_stg"."party" "stg_dis_src"
		WHERE  "stg_dis_src"."record_type" = 'S'
	)
	, "temp_table_set" AS 
	( 
		SELECT 
			  "stg_temp_src"."customers_hkey" AS "customers_hkey"
			, "stg_temp_src"."load_date" AS "load_date"
			, "stg_temp_src"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, CASE WHEN "stg_temp_src"."operation" = 'I' THEN 'T' ELSE "stg_temp_src"."record_type" END AS "record_type"
			, CASE WHEN "stg_temp_src"."operation" = 'I' THEN 'SAT' ELSE 'STG' END AS "source"
			, CASE WHEN "stg_temp_src"."operation" = 'I' THEN 0 ELSE 1 END AS "origin_id"
			, UPPER(ENCODE(DIGEST(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "stg_temp_src"."comments"),'~'),'#','\' || 
				'#'))|| '#' ||  UPPER(REPLACE(COALESCE(TRIM( "stg_temp_src"."address_number"::text),'~'),'#','\' || '#'))|| '#','#' || '~'),'~') ,'MD5'),'HEX')) AS "hash_diff"
			, CASE WHEN "stg_temp_src"."operation" = 'D' THEN 'Y'::text ELSE 'N'::text END AS "delete_flag"
			, "stg_temp_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_temp_src"."party_number" AS "party_number"
			, "stg_temp_src"."address_number" AS "address_number"
			, "stg_temp_src"."parent_party_number" AS "parent_party_number"
			, "stg_temp_src"."name" AS "name"
			, "stg_temp_src"."birthdate" AS "birthdate"
			, "stg_temp_src"."gender" AS "gender"
			, "stg_temp_src"."party_type_code" AS "party_type_code"
			, "stg_temp_src"."comments" AS "comments"
			, "stg_temp_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_mktg_scn01_stg"."party" "stg_temp_src"
		WHERE  "stg_temp_src"."record_type" = 'S'
		UNION ALL 
		SELECT 
			  "sat_src"."customers_hkey" AS "customers_hkey"
			, "sat_src"."load_date" AS "load_date"
			, "sat_src"."load_cycle_id" AS "load_cycle_id"
			, "sat_src"."load_end_date" AS "load_end_date"
			, 'T' AS "record_type"
			, 'SAT' AS "source"
			, 0 AS "origin_id"
			, "sat_src"."hash_diff" AS "hash_diff"
			, "sat_src"."delete_flag" AS "delete_flag"
			, "sat_src"."trans_timestamp" AS "trans_timestamp"
			, "sat_src"."party_number" AS "party_number"
			, "sat_src"."address_number" AS "address_number"
			, "sat_src"."parent_party_number" AS "parent_party_number"
			, "sat_src"."name" AS "name"
			, "sat_src"."birthdate" AS "birthdate"
			, "sat_src"."gender" AS "gender"
			, "sat_src"."party_type_code" AS "party_type_code"
			, "sat_src"."comments" AS "comments"
			, "sat_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_scn01_fl"."sat_mm_customers" "sat_src"
		INNER JOIN "dist_stg" "dist_stg" ON  "sat_src"."customers_hkey" = "dist_stg"."customers_hkey" AND "dist_stg"."load_cycle_id" != "sat_src"."load_cycle_id"
		WHERE  "sat_src"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
	)
	SELECT 
		  "temp_table_set"."customers_hkey" AS "customers_hkey"
		, "temp_table_set"."load_date" AS "load_date"
		, "temp_table_set"."load_end_date" AS "load_end_date"
		, "temp_table_set"."load_cycle_id" AS "load_cycle_id"
		, "temp_table_set"."record_type" AS "record_type"
		, "temp_table_set"."source" AS "source"
		, CASE WHEN "temp_table_set"."source" = 'STG' AND "temp_table_set"."delete_flag"::text || "temp_table_set"."hash_diff"::text =
			LAG( "temp_table_set"."delete_flag"::text || "temp_table_set"."hash_diff"::text,1)OVER(PARTITION BY "temp_table_set"."customers_hkey" ORDER BY "temp_table_set"."load_date","temp_table_set"."origin_id")THEN 1 ELSE 0 END AS "equal"
		, "temp_table_set"."hash_diff" AS "hash_diff"
		, "temp_table_set"."delete_flag" AS "delete_flag"
		, "temp_table_set"."trans_timestamp" AS "trans_timestamp"
		, "temp_table_set"."party_number" AS "party_number"
		, "temp_table_set"."address_number" AS "address_number"
		, "temp_table_set"."parent_party_number" AS "parent_party_number"
		, "temp_table_set"."name" AS "name"
		, "temp_table_set"."birthdate" AS "birthdate"
		, "temp_table_set"."gender" AS "gender"
		, "temp_table_set"."party_type_code" AS "party_type_code"
		, "temp_table_set"."comments" AS "comments"
		, "temp_table_set"."update_timestamp" AS "update_timestamp"
	FROM "temp_table_set" "temp_table_set"
	;
END;


BEGIN -- sat_inur_tgt

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
	SELECT 
		  "sat_temp_src_inur"."customers_hkey" AS "customers_hkey"
		, "sat_temp_src_inur"."load_date" AS "load_date"
		, "sat_temp_src_inur"."load_end_date" AS "load_end_date"
		, "sat_temp_src_inur"."load_cycle_id" AS "load_cycle_id"
		, "sat_temp_src_inur"."hash_diff" AS "hash_diff"
		, "sat_temp_src_inur"."delete_flag" AS "delete_flag"
		, "sat_temp_src_inur"."trans_timestamp" AS "trans_timestamp"
		, "sat_temp_src_inur"."party_number" AS "party_number"
		, "sat_temp_src_inur"."address_number" AS "address_number"
		, "sat_temp_src_inur"."parent_party_number" AS "parent_party_number"
		, "sat_temp_src_inur"."name" AS "name"
		, "sat_temp_src_inur"."birthdate" AS "birthdate"
		, "sat_temp_src_inur"."gender" AS "gender"
		, "sat_temp_src_inur"."party_type_code" AS "party_type_code"
		, "sat_temp_src_inur"."comments" AS "comments"
		, "sat_temp_src_inur"."update_timestamp" AS "update_timestamp"
	FROM "moto_mktg_scn01_stg"."sat_mm_customers_tmp" "sat_temp_src_inur"
	WHERE  "sat_temp_src_inur"."source" = 'STG' AND "sat_temp_src_inur"."equal" = 0
	;
END;


BEGIN -- sat_ed_tgt

	WITH "calc_load_end_date" AS 
	( 
		SELECT 
			  "sat_temp_src_us"."customers_hkey" AS "customers_hkey"
			, "sat_temp_src_us"."load_date" AS "load_date"
			, "lci_src"."load_cycle_id" AS "load_cycle_id"
			, COALESCE(LEAD("sat_temp_src_us"."load_date",1)OVER(PARTITION BY "sat_temp_src_us"."customers_hkey" ORDER BY "sat_temp_src_us"."load_date")
				, TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
		FROM "moto_mktg_scn01_stg"."sat_mm_customers_tmp" "sat_temp_src_us"
		INNER JOIN "moto_mktg_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		WHERE  "sat_temp_src_us"."equal" = 0
	)
	, "filter_load_end_date" AS 
	( 
		SELECT 
			  "calc_load_end_date"."customers_hkey" AS "customers_hkey"
			, "calc_load_end_date"."load_date" AS "load_date"
			, "calc_load_end_date"."load_cycle_id" AS "load_cycle_id"
			, "calc_load_end_date"."load_end_date" AS "load_end_date"
		FROM "calc_load_end_date" "calc_load_end_date"
		WHERE  "calc_load_end_date"."load_end_date" != TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
	)
	UPDATE "moto_scn01_fl"."sat_mm_customers" "sat_ed_tgt"
	SET 
		 "load_end_date" =  "filter_load_end_date"."load_end_date"
		,"load_cycle_id" =  "filter_load_end_date"."load_cycle_id"
	FROM  "filter_load_end_date"
	WHERE "sat_ed_tgt"."customers_hkey" =  "filter_load_end_date"."customers_hkey"
	  AND "sat_ed_tgt"."load_date" =  "filter_load_end_date"."load_date"
	;
END;



END;
$function$;
 
 
