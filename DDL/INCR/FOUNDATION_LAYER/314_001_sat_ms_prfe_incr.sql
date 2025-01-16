CREATE OR REPLACE FUNCTION "moto_scn01_proc"."sat_ms_prfe_incr"() 
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

BEGIN -- sat_temp_tgt

	TRUNCATE TABLE "moto_sales_scn01_stg"."sat_ms_product_features_tmp"  CASCADE;

	INSERT INTO "moto_sales_scn01_stg"."sat_ms_product_features_tmp"(
		 "product_features_hkey"
		,"load_date"
		,"load_end_date"
		,"load_cycle_id"
		,"record_type"
		,"source"
		,"equal"
		,"hash_diff"
		,"delete_flag"
		,"trans_timestamp"
		,"product_feature_id"
		,"product_feature_code"
		,"pro_fea_lan_code_seq"
		,"product_feature_language_code"
		,"product_feature_description"
		,"update_timestamp"
	)
	WITH "dist_stg" AS 
	( 
		SELECT DISTINCT 
 			  "stg_dis_src"."product_features_hkey" AS "product_features_hkey"
			, "stg_dis_src"."pro_fea_lan_code_seq" AS "pro_fea_lan_code_seq"
			, "stg_dis_src"."load_cycle_id" AS "load_cycle_id"
		FROM "moto_sales_scn01_stg"."product_features" "stg_dis_src"
		WHERE  "stg_dis_src"."record_type" = 'S'
	)
	, "temp_table_set" AS 
	( 
		SELECT 
			  "stg_temp_src"."product_features_hkey" AS "product_features_hkey"
			, "stg_temp_src"."load_date" AS "load_date"
			, "stg_temp_src"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, "stg_temp_src"."record_type" AS "record_type"
			, 'STG' AS "source"
			, 1 AS "origin_id"
			, UPPER(ENCODE(DIGEST(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "stg_temp_src"."product_feature_description")
				,'~'),'#','\' || '#'))|| '#','#' || '~'),'~') ,'MD5'),'HEX')) AS "hash_diff"
			, CASE WHEN "stg_temp_src"."operation" = 'D' THEN 'Y'::text ELSE 'N'::text END AS "delete_flag"
			, "stg_temp_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_temp_src"."product_feature_id" AS "product_feature_id"
			, "stg_temp_src"."product_feature_code" AS "product_feature_code"
			, "stg_temp_src"."pro_fea_lan_code_seq" AS "pro_fea_lan_code_seq"
			, "stg_temp_src"."product_feature_language_code" AS "product_feature_language_code"
			, "stg_temp_src"."product_feature_description" AS "product_feature_description"
			, "stg_temp_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01_stg"."product_features" "stg_temp_src"
		WHERE  "stg_temp_src"."record_type" = 'S'
		UNION ALL 
		SELECT 
			  "sat_src"."product_features_hkey" AS "product_features_hkey"
			, "sat_src"."load_date" AS "load_date"
			, "sat_src"."load_cycle_id" AS "load_cycle_id"
			, "sat_src"."load_end_date" AS "load_end_date"
			, 'SAT' AS "record_type"
			, 'SAT' AS "source"
			, 0 AS "origin_id"
			, "sat_src"."hash_diff" AS "hash_diff"
			, "sat_src"."delete_flag" AS "delete_flag"
			, "sat_src"."trans_timestamp" AS "trans_timestamp"
			, "sat_src"."product_feature_id" AS "product_feature_id"
			, "sat_src"."product_feature_code" AS "product_feature_code"
			, "sat_src"."pro_fea_lan_code_seq" AS "pro_fea_lan_code_seq"
			, "sat_src"."product_feature_language_code" AS "product_feature_language_code"
			, "sat_src"."product_feature_description" AS "product_feature_description"
			, "sat_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_scn01_fl"."sat_ms_product_features" "sat_src"
		INNER JOIN "dist_stg" "dist_stg" ON  "sat_src"."product_features_hkey" = "dist_stg"."product_features_hkey" AND "sat_src"."pro_fea_lan_code_seq" = 
			"dist_stg"."pro_fea_lan_code_seq"
		WHERE  "sat_src"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
	)
	SELECT 
		  "temp_table_set"."product_features_hkey" AS "product_features_hkey"
		, "temp_table_set"."load_date" AS "load_date"
		, "temp_table_set"."load_end_date" AS "load_end_date"
		, "temp_table_set"."load_cycle_id" AS "load_cycle_id"
		, "temp_table_set"."record_type" AS "record_type"
		, "temp_table_set"."source" AS "source"
		, CASE WHEN "temp_table_set"."source" = 'STG' AND "temp_table_set"."delete_flag"::text || "temp_table_set"."hash_diff"::text =
			LAG("temp_table_set"."delete_flag"::text || "temp_table_set"."hash_diff"::text,1)OVER(PARTITION BY "temp_table_set"."product_features_hkey","temp_table_set"."pro_fea_lan_code_seq" ORDER BY "temp_table_set"."load_date","temp_table_set"."origin_id")THEN 1 ELSE 0 END AS "equal"
		, "temp_table_set"."hash_diff" AS "hash_diff"
		, "temp_table_set"."delete_flag" AS "delete_flag"
		, "temp_table_set"."trans_timestamp" AS "trans_timestamp"
		, "temp_table_set"."product_feature_id" AS "product_feature_id"
		, "temp_table_set"."product_feature_code" AS "product_feature_code"
		, "temp_table_set"."pro_fea_lan_code_seq" AS "pro_fea_lan_code_seq"
		, "temp_table_set"."product_feature_language_code" AS "product_feature_language_code"
		, "temp_table_set"."product_feature_description" AS "product_feature_description"
		, "temp_table_set"."update_timestamp" AS "update_timestamp"
	FROM "temp_table_set" "temp_table_set"
	;
END;


BEGIN -- sat_inur_tgt

	INSERT INTO "moto_scn01_fl"."sat_ms_product_features"(
		 "product_features_hkey"
		,"load_date"
		,"load_end_date"
		,"load_cycle_id"
		,"hash_diff"
		,"delete_flag"
		,"trans_timestamp"
		,"product_feature_id"
		,"product_feature_code"
		,"pro_fea_lan_code_seq"
		,"product_feature_language_code"
		,"product_feature_description"
		,"update_timestamp"
	)
	SELECT 
		  "sat_temp_src_inur"."product_features_hkey" AS "product_features_hkey"
		, "sat_temp_src_inur"."load_date" AS "load_date"
		, "sat_temp_src_inur"."load_end_date" AS "load_end_date"
		, "sat_temp_src_inur"."load_cycle_id" AS "load_cycle_id"
		, "sat_temp_src_inur"."hash_diff" AS "hash_diff"
		, "sat_temp_src_inur"."delete_flag" AS "delete_flag"
		, "sat_temp_src_inur"."trans_timestamp" AS "trans_timestamp"
		, "sat_temp_src_inur"."product_feature_id" AS "product_feature_id"
		, "sat_temp_src_inur"."product_feature_code" AS "product_feature_code"
		, "sat_temp_src_inur"."pro_fea_lan_code_seq" AS "pro_fea_lan_code_seq"
		, "sat_temp_src_inur"."product_feature_language_code" AS "product_feature_language_code"
		, "sat_temp_src_inur"."product_feature_description" AS "product_feature_description"
		, "sat_temp_src_inur"."update_timestamp" AS "update_timestamp"
	FROM "moto_sales_scn01_stg"."sat_ms_product_features_tmp" "sat_temp_src_inur"
	WHERE  "sat_temp_src_inur"."source" = 'STG' AND "sat_temp_src_inur"."equal" = 0
	;
END;


BEGIN -- sat_ed_tgt

	WITH "calc_load_end_date" AS 
	( 
		SELECT 
			  "sat_temp_src_us"."product_features_hkey" AS "product_features_hkey"
			, "sat_temp_src_us"."load_date" AS "load_date"
			, "lci_src"."load_cycle_id" AS "load_cycle_id"
			, COALESCE(LEAD("sat_temp_src_us"."load_date",1)OVER(PARTITION BY "sat_temp_src_us"."product_features_hkey",
				"sat_temp_src_us"."pro_fea_lan_code_seq" ORDER BY "sat_temp_src_us"."load_date"), TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
			, "sat_temp_src_us"."pro_fea_lan_code_seq" AS "pro_fea_lan_code_seq"
			, "sat_temp_src_us"."product_feature_language_code" AS "product_feature_language_code"
		FROM "moto_sales_scn01_stg"."sat_ms_product_features_tmp" "sat_temp_src_us"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		WHERE  "sat_temp_src_us"."equal" = 0
	)
	, "filter_load_end_date" AS 
	( 
		SELECT 
			  "calc_load_end_date"."product_features_hkey" AS "product_features_hkey"
			, "calc_load_end_date"."load_date" AS "load_date"
			, "calc_load_end_date"."load_cycle_id" AS "load_cycle_id"
			, "calc_load_end_date"."load_end_date" AS "load_end_date"
			, "calc_load_end_date"."pro_fea_lan_code_seq" AS "pro_fea_lan_code_seq"
			, "calc_load_end_date"."product_feature_language_code" AS "product_feature_language_code"
		FROM "calc_load_end_date" "calc_load_end_date"
		WHERE  "calc_load_end_date"."load_end_date" != TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
	)
	UPDATE "moto_scn01_fl"."sat_ms_product_features" "sat_ed_tgt"
	SET 
		 "load_end_date" =  "filter_load_end_date"."load_end_date"
		,"load_cycle_id" =  "filter_load_end_date"."load_cycle_id"
	FROM  "filter_load_end_date"
	WHERE "sat_ed_tgt"."product_features_hkey" =  "filter_load_end_date"."product_features_hkey"
	  AND "sat_ed_tgt"."pro_fea_lan_code_seq" =  "filter_load_end_date"."pro_fea_lan_code_seq"
	  AND "sat_ed_tgt"."load_date" =  "filter_load_end_date"."load_date"
	;
END;



END;
$function$;
 
 
