CREATE OR REPLACE FUNCTION "moto_scn01_proc"."sat_ms_inli_incr"() 
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

Vaultspeed version: 5.7.2.14, generation date: 2025/01/09 12:47:43
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/09 09:40:46, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04
 */


BEGIN 

BEGIN -- sat_temp_tgt

	TRUNCATE TABLE "moto_sales_scn01_stg"."sat_ms_invoice_lines_tmp"  CASCADE;

	INSERT INTO "moto_sales_scn01_stg"."sat_ms_invoice_lines_tmp"(
		 "invoice_lines_hkey"
		,"load_date"
		,"load_end_date"
		,"load_cycle_id"
		,"record_type"
		,"source"
		,"equal"
		,"hash_diff"
		,"delete_flag"
		,"trans_timestamp"
		,"invoice_line_number"
		,"invoice_number"
		,"amount"
		,"quantity"
		,"unit_price"
		,"update_timestamp"
	)
	WITH "dist_stg" AS 
	( 
		SELECT DISTINCT 
 			  "stg_dis_src"."invoice_lines_hkey" AS "invoice_lines_hkey"
			, "stg_dis_src"."load_cycle_id" AS "load_cycle_id"
		FROM "moto_sales_scn01_stg"."invoice_lines" "stg_dis_src"
		WHERE  "stg_dis_src"."record_type" = 'S'
	)
	, "temp_table_set" AS 
	( 
		SELECT 
			  "stg_temp_src"."invoice_lines_hkey" AS "invoice_lines_hkey"
			, "stg_temp_src"."load_date" AS "load_date"
			, "stg_temp_src"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, "stg_temp_src"."record_type" AS "record_type"
			, 'STG' AS "source"
			, 1 AS "origin_id"
			, UPPER(ENCODE(DIGEST(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "stg_temp_src"."amount"::text),'~'),'#','\' || 
				'#'))|| '#' ||  UPPER(REPLACE(COALESCE(TRIM( "stg_temp_src"."quantity"::text),'~'),'#','\' || '#'))|| '#' ||  UPPER(REPLACE(COALESCE(TRIM( "stg_temp_src"."unit_price"::text),'~'),'#','\' || '#'))|| '#','#' || '~'),'~') ,'MD5'),'HEX')) AS "hash_diff"
			, CASE WHEN "stg_temp_src"."operation" = 'D' THEN 'Y'::text ELSE 'N'::text END AS "delete_flag"
			, "stg_temp_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_temp_src"."invoice_line_number" AS "invoice_line_number"
			, "stg_temp_src"."invoice_number" AS "invoice_number"
			, "stg_temp_src"."amount" AS "amount"
			, "stg_temp_src"."quantity" AS "quantity"
			, "stg_temp_src"."unit_price" AS "unit_price"
			, "stg_temp_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01_stg"."invoice_lines" "stg_temp_src"
		WHERE  "stg_temp_src"."record_type" = 'S'
		UNION ALL 
		SELECT 
			  "sat_src"."invoice_lines_hkey" AS "invoice_lines_hkey"
			, "sat_src"."load_date" AS "load_date"
			, "sat_src"."load_cycle_id" AS "load_cycle_id"
			, "sat_src"."load_end_date" AS "load_end_date"
			, 'SAT' AS "record_type"
			, 'SAT' AS "source"
			, 0 AS "origin_id"
			, "sat_src"."hash_diff" AS "hash_diff"
			, "sat_src"."delete_flag" AS "delete_flag"
			, "sat_src"."trans_timestamp" AS "trans_timestamp"
			, "sat_src"."invoice_line_number" AS "invoice_line_number"
			, "sat_src"."invoice_number" AS "invoice_number"
			, "sat_src"."amount" AS "amount"
			, "sat_src"."quantity" AS "quantity"
			, "sat_src"."unit_price" AS "unit_price"
			, "sat_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_scn01_fl"."sat_ms_invoice_lines" "sat_src"
		INNER JOIN "dist_stg" "dist_stg" ON  "sat_src"."invoice_lines_hkey" = "dist_stg"."invoice_lines_hkey"
		WHERE  "sat_src"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
	)
	SELECT 
		  "temp_table_set"."invoice_lines_hkey" AS "invoice_lines_hkey"
		, "temp_table_set"."load_date" AS "load_date"
		, "temp_table_set"."load_end_date" AS "load_end_date"
		, "temp_table_set"."load_cycle_id" AS "load_cycle_id"
		, "temp_table_set"."record_type" AS "record_type"
		, "temp_table_set"."source" AS "source"
		, CASE WHEN "temp_table_set"."source" = 'STG' AND "temp_table_set"."delete_flag"::text || "temp_table_set"."hash_diff"::text =
			LAG( "temp_table_set"."delete_flag"::text || "temp_table_set"."hash_diff"::text,1)OVER(PARTITION BY "temp_table_set"."invoice_lines_hkey" ORDER BY "temp_table_set"."load_date","temp_table_set"."origin_id")THEN 1 ELSE 0 END AS "equal"
		, "temp_table_set"."hash_diff" AS "hash_diff"
		, "temp_table_set"."delete_flag" AS "delete_flag"
		, "temp_table_set"."trans_timestamp" AS "trans_timestamp"
		, "temp_table_set"."invoice_line_number" AS "invoice_line_number"
		, "temp_table_set"."invoice_number" AS "invoice_number"
		, "temp_table_set"."amount" AS "amount"
		, "temp_table_set"."quantity" AS "quantity"
		, "temp_table_set"."unit_price" AS "unit_price"
		, "temp_table_set"."update_timestamp" AS "update_timestamp"
	FROM "temp_table_set" "temp_table_set"
	;
END;


BEGIN -- sat_inur_tgt

	INSERT INTO "moto_scn01_fl"."sat_ms_invoice_lines"(
		 "invoice_lines_hkey"
		,"load_date"
		,"load_end_date"
		,"load_cycle_id"
		,"hash_diff"
		,"delete_flag"
		,"trans_timestamp"
		,"invoice_line_number"
		,"invoice_number"
		,"amount"
		,"quantity"
		,"unit_price"
		,"update_timestamp"
	)
	SELECT 
		  "sat_temp_src_inur"."invoice_lines_hkey" AS "invoice_lines_hkey"
		, "sat_temp_src_inur"."load_date" AS "load_date"
		, "sat_temp_src_inur"."load_end_date" AS "load_end_date"
		, "sat_temp_src_inur"."load_cycle_id" AS "load_cycle_id"
		, "sat_temp_src_inur"."hash_diff" AS "hash_diff"
		, "sat_temp_src_inur"."delete_flag" AS "delete_flag"
		, "sat_temp_src_inur"."trans_timestamp" AS "trans_timestamp"
		, "sat_temp_src_inur"."invoice_line_number" AS "invoice_line_number"
		, "sat_temp_src_inur"."invoice_number" AS "invoice_number"
		, "sat_temp_src_inur"."amount" AS "amount"
		, "sat_temp_src_inur"."quantity" AS "quantity"
		, "sat_temp_src_inur"."unit_price" AS "unit_price"
		, "sat_temp_src_inur"."update_timestamp" AS "update_timestamp"
	FROM "moto_sales_scn01_stg"."sat_ms_invoice_lines_tmp" "sat_temp_src_inur"
	WHERE  "sat_temp_src_inur"."source" = 'STG' AND "sat_temp_src_inur"."equal" = 0
	;
END;


BEGIN -- sat_ed_tgt

	WITH "calc_load_end_date" AS 
	( 
		SELECT 
			  "sat_temp_src_us"."invoice_lines_hkey" AS "invoice_lines_hkey"
			, "sat_temp_src_us"."load_date" AS "load_date"
			, "lci_src"."load_cycle_id" AS "load_cycle_id"
			, COALESCE(LEAD("sat_temp_src_us"."load_date",1)OVER(PARTITION BY "sat_temp_src_us"."invoice_lines_hkey" ORDER BY "sat_temp_src_us"."load_date")
				, TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
		FROM "moto_sales_scn01_stg"."sat_ms_invoice_lines_tmp" "sat_temp_src_us"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		WHERE  "sat_temp_src_us"."equal" = 0
	)
	, "filter_load_end_date" AS 
	( 
		SELECT 
			  "calc_load_end_date"."invoice_lines_hkey" AS "invoice_lines_hkey"
			, "calc_load_end_date"."load_date" AS "load_date"
			, "calc_load_end_date"."load_cycle_id" AS "load_cycle_id"
			, "calc_load_end_date"."load_end_date" AS "load_end_date"
		FROM "calc_load_end_date" "calc_load_end_date"
		WHERE  "calc_load_end_date"."load_end_date" != TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
	)
	UPDATE "moto_scn01_fl"."sat_ms_invoice_lines" "sat_ed_tgt"
	SET 
		 "load_end_date" =  "filter_load_end_date"."load_end_date"
		,"load_cycle_id" =  "filter_load_end_date"."load_cycle_id"
	FROM  "filter_load_end_date"
	WHERE "sat_ed_tgt"."invoice_lines_hkey" =  "filter_load_end_date"."invoice_lines_hkey"
	  AND "sat_ed_tgt"."load_date" =  "filter_load_end_date"."load_date"
	;
END;



END;
$function$;
 
 
