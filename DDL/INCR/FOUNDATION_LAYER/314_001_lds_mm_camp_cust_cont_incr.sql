CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lds_mm_camp_cust_cont_incr"() 
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

BEGIN -- lds_inr_tgt

	INSERT INTO "moto_scn01_fl"."lds_mm_camp_cust_cont"(
		 "lnd_camp_cust_cont_hkey"
		,"contacts_hkey"
		,"customers_hkey"
		,"campaigns_hkey"
		,"contact_id"
		,"party_number"
		,"campaign_code"
		,"campaign_start_date"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"hash_diff"
		,"delete_flag"
		,"trans_timestamp"
		,"marketing_program_code"
		,"marketing_program_name"
		,"update_timestamp"
	)
	SELECT 
		  "stg_dl_inr_src"."lnd_camp_cust_cont_hkey" AS "lnd_camp_cust_cont_hkey"
		, "stg_dl_inr_src"."contacts_hkey" AS "contacts_hkey"
		, "stg_dl_inr_src"."customers_hkey" AS "customers_hkey"
		, "stg_dl_inr_src"."campaigns_hkey" AS "campaigns_hkey"
		, "stg_dl_inr_src"."contact_id" AS "contact_id"
		, "stg_dl_inr_src"."party_number" AS "party_number"
		, "stg_dl_inr_src"."campaign_code" AS "campaign_code"
		, "stg_dl_inr_src"."campaign_start_date" AS "campaign_start_date"
		, "stg_dl_inr_src"."load_date" AS "load_date"
		, "stg_dl_inr_src"."load_cycle_id" AS "load_cycle_id"
		, TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
		, UPPER(ENCODE(DIGEST(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "stg_dl_inr_src"."marketing_program_code"),
			'~'),'#','\' || '#'))|| '#' ||  UPPER(REPLACE(COALESCE(TRIM( "stg_dl_inr_src"."marketing_program_name"),'~'),'#','\' || '#'))|| '#','#' || '~'),'~') ,'MD5'),'HEX')) AS "hash_diff"
		, 'N'::text AS "delete_flag"
		, "stg_dl_inr_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_dl_inr_src"."marketing_program_code" AS "marketing_program_code"
		, "stg_dl_inr_src"."marketing_program_name" AS "marketing_program_name"
		, "stg_dl_inr_src"."update_timestamp" AS "update_timestamp"
	FROM "moto_mktg_scn01_stg"."camp_part_cont" "stg_dl_inr_src"
	LEFT OUTER JOIN "moto_scn01_fl"."lds_mm_camp_cust_cont" "lds_inr_src" ON  "stg_dl_inr_src"."lnd_camp_cust_cont_hkey" = "lds_inr_src"."lnd_camp_cust_cont_hkey" AND "stg_dl_inr_src"."load_date" =
		 "lds_inr_src"."load_date"
	WHERE  "stg_dl_inr_src"."operation" = 'I' AND "lds_inr_src"."lnd_camp_cust_cont_hkey" IS NULL AND "stg_dl_inr_src"."error_code_camp_cust_cont" = 0
	;
END;


BEGIN -- lds_temp_tgt

	TRUNCATE TABLE "moto_mktg_scn01_stg"."lds_mm_camp_cust_cont_tmp"  CASCADE;

	INSERT INTO "moto_mktg_scn01_stg"."lds_mm_camp_cust_cont_tmp"(
		 "lnd_camp_cust_cont_hkey"
		,"contacts_hkey"
		,"customers_hkey"
		,"campaigns_hkey"
		,"contact_id"
		,"party_number"
		,"campaign_code"
		,"campaign_start_date"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"hash_diff"
		,"record_type"
		,"source"
		,"equal"
		,"delete_flag"
		,"trans_timestamp"
		,"marketing_program_code"
		,"marketing_program_name"
		,"update_timestamp"
	)
	WITH "dist_stg" AS 
	( 
		SELECT 
			  "stg_dis_src"."lnd_camp_cust_cont_hkey" AS "lnd_camp_cust_cont_hkey"
			, "stg_dis_src"."load_cycle_id" AS "load_cycle_id"
			, MIN("stg_dis_src"."load_date") AS "min_load_timestamp"
			, MIN("stg_dis_src"."error_code_camp_cust_cont") AS "error_code_camp_cust_cont"
		FROM "moto_mktg_scn01_stg"."camp_part_cont" "stg_dis_src"
		WHERE  "stg_dis_src"."error_code_camp_cust_cont" = 0
		GROUP BY  "stg_dis_src"."lnd_camp_cust_cont_hkey",  "stg_dis_src"."load_cycle_id"
	)
	, "last_lds" AS 
	( 
		SELECT 
			  "last_lds_src"."lnd_camp_cust_cont_hkey" AS "lnd_camp_cust_cont_hkey"
			, "dist_stg"."load_cycle_id" AS "load_cycle_id"
			, MAX("last_lds_src"."load_date") AS "max_load_timestamp"
			, MIN("dist_stg"."error_code_camp_cust_cont") AS "error_code_camp_cust_cont"
		FROM "moto_scn01_fl"."lds_mm_camp_cust_cont" "last_lds_src"
		INNER JOIN "moto_scn01_fl"."lnd_camp_cust_cont" "last_lnd_src" ON  "last_lds_src"."lnd_camp_cust_cont_hkey" = "last_lnd_src"."lnd_camp_cust_cont_hkey"
		INNER JOIN "dist_stg" "dist_stg" ON  "last_lnd_src"."lnd_camp_cust_cont_hkey" = "dist_stg"."lnd_camp_cust_cont_hkey"
		WHERE  "last_lds_src"."load_date" <= "dist_stg"."min_load_timestamp"
		GROUP BY  "last_lds_src"."lnd_camp_cust_cont_hkey",  "dist_stg"."load_cycle_id"
	)
	, "temp_table_set" AS 
	( 
		SELECT 
			  "stg_temp_src"."lnd_camp_cust_cont_hkey" AS "lnd_camp_cust_cont_hkey"
			, "stg_temp_src"."contacts_hkey" AS "contacts_hkey"
			, "stg_temp_src"."customers_hkey" AS "customers_hkey"
			, "stg_temp_src"."campaigns_hkey" AS "campaigns_hkey"
			, "stg_temp_src"."contact_id" AS "contact_id"
			, "stg_temp_src"."party_number" AS "party_number"
			, "stg_temp_src"."campaign_code" AS "campaign_code"
			, "stg_temp_src"."campaign_start_date" AS "campaign_start_date"
			, "stg_temp_src"."load_date" AS "load_date"
			, "stg_temp_src"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, UPPER(ENCODE(DIGEST(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "stg_temp_src"."marketing_program_code"),
				'~'),'#','\' || '#'))|| '#' ||  UPPER(REPLACE(COALESCE(TRIM( "stg_temp_src"."marketing_program_name"),'~'),'#','\' || '#'))|| '#','#' || '~'),'~') ,'MD5'),'HEX')) AS "hash_diff"
			, CASE WHEN "stg_temp_src"."operation" = 'I' THEN 'T' ELSE "stg_temp_src"."record_type" END AS "record_type"
			, CASE WHEN "stg_temp_src"."operation" = 'I' THEN 'LDS' ELSE 'STG' END AS "source"
			, CASE WHEN "stg_temp_src"."operation" = 'I' THEN 0 ELSE 1 END AS "origin_id"
			, CASE WHEN "stg_temp_src"."operation" = 'D' THEN 'Y'::text ELSE 'N'::text END AS "delete_flag"
			, "stg_temp_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_temp_src"."marketing_program_code" AS "marketing_program_code"
			, "stg_temp_src"."marketing_program_name" AS "marketing_program_name"
			, "stg_temp_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_mktg_scn01_stg"."camp_part_cont" "stg_temp_src"
		WHERE  "stg_temp_src"."error_code_camp_cust_cont" = 0
		UNION ALL 
		SELECT 
			  "lds_src"."lnd_camp_cust_cont_hkey" AS "lnd_camp_cust_cont_hkey"
			, "lnd_src"."contacts_hkey" AS "contacts_hkey"
			, "lnd_src"."customers_hkey" AS "customers_hkey"
			, "lnd_src"."campaigns_hkey" AS "campaigns_hkey"
			, "lds_src"."contact_id" AS "contact_id"
			, "lds_src"."party_number" AS "party_number"
			, "lds_src"."campaign_code" AS "campaign_code"
			, "lds_src"."campaign_start_date" AS "campaign_start_date"
			, "lds_src"."load_date" AS "load_date"
			, "lds_src"."load_cycle_id" AS "load_cycle_id"
			, "lds_src"."load_end_date" AS "load_end_date"
			, "lds_src"."hash_diff" AS "hash_diff"
			, 'T' AS "record_type"
			, 'LDS' AS "source"
			, 0 AS "origin_id"
			, "lds_src"."delete_flag" AS "delete_flag"
			, "lds_src"."trans_timestamp" AS "trans_timestamp"
			, "lds_src"."marketing_program_code" AS "marketing_program_code"
			, "lds_src"."marketing_program_name" AS "marketing_program_name"
			, "lds_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_scn01_fl"."lds_mm_camp_cust_cont" "lds_src"
		INNER JOIN "moto_scn01_fl"."lnd_camp_cust_cont" "lnd_src" ON  "lds_src"."lnd_camp_cust_cont_hkey" = "lnd_src"."lnd_camp_cust_cont_hkey"
		INNER JOIN "last_lds" "last_lds" ON  "lds_src"."lnd_camp_cust_cont_hkey" = "last_lds"."lnd_camp_cust_cont_hkey"
		WHERE ( "lds_src"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) OR("last_lds"."error_code_camp_cust_cont" = 0 AND "lds_src"."load_date" >= "last_lds"."max_load_timestamp"))AND "last_lds"."load_cycle_id" != "lds_src"."load_cycle_id"
	)
	SELECT 
		  "temp_table_set"."lnd_camp_cust_cont_hkey" AS "lnd_camp_cust_cont_hkey"
		, "temp_table_set"."contacts_hkey" AS "contacts_hkey"
		, "temp_table_set"."customers_hkey" AS "customers_hkey"
		, "temp_table_set"."campaigns_hkey" AS "campaigns_hkey"
		, "temp_table_set"."contact_id" AS "contact_id"
		, "temp_table_set"."party_number" AS "party_number"
		, "temp_table_set"."campaign_code" AS "campaign_code"
		, "temp_table_set"."campaign_start_date" AS "campaign_start_date"
		, "temp_table_set"."load_date" AS "load_date"
		, "temp_table_set"."load_cycle_id" AS "load_cycle_id"
		, "temp_table_set"."load_end_date" AS "load_end_date"
		, "temp_table_set"."hash_diff" AS "hash_diff"
		, "temp_table_set"."record_type" AS "record_type"
		, "temp_table_set"."source" AS "source"
		, CASE WHEN "temp_table_set"."source" = 'STG' AND "temp_table_set"."delete_flag"::text || "temp_table_set"."hash_diff"::text =
			LAG( "temp_table_set"."delete_flag"::text || "temp_table_set"."hash_diff"::text,1)OVER(PARTITION BY "temp_table_set"."lnd_camp_cust_cont_hkey" ORDER BY "temp_table_set"."load_date","temp_table_set"."origin_id")THEN 1 ELSE 0 END AS "equal"
		, "temp_table_set"."delete_flag" AS "delete_flag"
		, "temp_table_set"."trans_timestamp" AS "trans_timestamp"
		, "temp_table_set"."marketing_program_code" AS "marketing_program_code"
		, "temp_table_set"."marketing_program_name" AS "marketing_program_name"
		, "temp_table_set"."update_timestamp" AS "update_timestamp"
	FROM "temp_table_set" "temp_table_set"
	;
END;


BEGIN -- lds_inur_tgt

	INSERT INTO "moto_scn01_fl"."lds_mm_camp_cust_cont"(
		 "lnd_camp_cust_cont_hkey"
		,"contacts_hkey"
		,"customers_hkey"
		,"campaigns_hkey"
		,"contact_id"
		,"party_number"
		,"campaign_code"
		,"campaign_start_date"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"hash_diff"
		,"delete_flag"
		,"trans_timestamp"
		,"marketing_program_code"
		,"marketing_program_name"
		,"update_timestamp"
	)
	SELECT 
		  "lds_temp_src_inur"."lnd_camp_cust_cont_hkey" AS "lnd_camp_cust_cont_hkey"
		, "lds_temp_src_inur"."contacts_hkey" AS "contacts_hkey"
		, "lds_temp_src_inur"."customers_hkey" AS "customers_hkey"
		, "lds_temp_src_inur"."campaigns_hkey" AS "campaigns_hkey"
		, "lds_temp_src_inur"."contact_id" AS "contact_id"
		, "lds_temp_src_inur"."party_number" AS "party_number"
		, "lds_temp_src_inur"."campaign_code" AS "campaign_code"
		, "lds_temp_src_inur"."campaign_start_date" AS "campaign_start_date"
		, "lds_temp_src_inur"."load_date" AS "load_date"
		, "lds_temp_src_inur"."load_cycle_id" AS "load_cycle_id"
		, "lds_temp_src_inur"."load_end_date" AS "load_end_date"
		, "lds_temp_src_inur"."hash_diff" AS "hash_diff"
		, "lds_temp_src_inur"."delete_flag" AS "delete_flag"
		, "lds_temp_src_inur"."trans_timestamp" AS "trans_timestamp"
		, "lds_temp_src_inur"."marketing_program_code" AS "marketing_program_code"
		, "lds_temp_src_inur"."marketing_program_name" AS "marketing_program_name"
		, "lds_temp_src_inur"."update_timestamp" AS "update_timestamp"
	FROM "moto_mktg_scn01_stg"."lds_mm_camp_cust_cont_tmp" "lds_temp_src_inur"
	WHERE  "lds_temp_src_inur"."source" = 'STG' AND "lds_temp_src_inur"."equal" = 0
	;
END;


BEGIN -- lds_ed_tgt

	WITH "calc_load_end_date" AS 
	( 
		SELECT 
			  "lds_temp_src_us"."lnd_camp_cust_cont_hkey" AS "lnd_camp_cust_cont_hkey"
			, "lds_temp_src_us"."load_date" AS "load_date"
			, CASE WHEN "lds_temp_src_us"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 
				'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN "lci_src"."load_cycle_id" ELSE "lds_temp_src_us"."load_cycle_id" END AS "load_cycle_id"
			, COALESCE(LEAD("lds_temp_src_us"."load_date",1)OVER(PARTITION BY "lds_temp_src_us"."lnd_camp_cust_cont_hkey" ORDER BY "lds_temp_src_us"."load_date",
				"lds_temp_src_us"."load_cycle_id"), TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
		FROM "moto_mktg_scn01_stg"."lds_mm_camp_cust_cont_tmp" "lds_temp_src_us"
		INNER JOIN "moto_mktg_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		WHERE  "lds_temp_src_us"."equal" = 0
	)
	, "filter_load_end_date" AS 
	( 
		SELECT 
			  "calc_load_end_date"."lnd_camp_cust_cont_hkey" AS "lnd_camp_cust_cont_hkey"
			, "calc_load_end_date"."load_date" AS "load_date"
			, "calc_load_end_date"."load_cycle_id" AS "load_cycle_id"
			, "calc_load_end_date"."load_end_date" AS "load_end_date"
		FROM "calc_load_end_date" "calc_load_end_date"
		WHERE  "calc_load_end_date"."load_end_date" != TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
	)
	UPDATE "moto_scn01_fl"."lds_mm_camp_cust_cont" "lds_ed_tgt"
	SET 
		 "load_cycle_id" =  "filter_load_end_date"."load_cycle_id"
		,"load_end_date" =  "filter_load_end_date"."load_end_date"
	FROM  "filter_load_end_date"
	WHERE "lds_ed_tgt"."lnd_camp_cust_cont_hkey" =  "filter_load_end_date"."lnd_camp_cust_cont_hkey"
	  AND "lds_ed_tgt"."load_date" =  "filter_load_end_date"."load_date"
	;
END;



END;
$function$;
 
 
