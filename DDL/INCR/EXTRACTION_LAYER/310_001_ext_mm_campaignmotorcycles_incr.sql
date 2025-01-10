CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_mm_campaignmotorcycles_incr"() 
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

BEGIN -- ext_tgt

	TRUNCATE TABLE "moto_mktg_scn01_ext"."campaign_motorcycles"  CASCADE;

	INSERT INTO "moto_mktg_scn01_ext"."campaign_motorcycles"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"campaign_code"
		,"campaign_start_date"
		,"motorcycle_id"
		,"campaign_code_fk_campaignstartdate_bk"
		,"campaign_start_date_fk_campaignstartdate_bk"
		,"motorcycle_class_desc"
		,"motorcycle_subclass_desc"
		,"motorcycle_emotion_desc"
		,"motorcycle_comment"
		,"update_timestamp"
		,"error_code_camp_prod"
	)
	WITH "calculate_variables" AS 
	( 
		SELECT 
			  "lci_src"."load_cycle_id" AS "load_cycle_id"
			, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "tdfv_src"."campaign_code" ,  "tdfv_src"."campaign_start_date" ,
				  "tdfv_src"."motorcycle_id"  ORDER BY  "tdfv_src"."trans_timestamp") * interval'2 microsecond'   AS "load_date"
			, "tdfv_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("tdfv_src"."operation","mex_src"."attribute_varchar") AS "operation"
			, "tdfv_src"."record_type" AS "record_type"
			, "tdfv_src"."campaign_code" AS "campaign_code"
			, "tdfv_src"."campaign_start_date" AS "campaign_start_date"
			, "tdfv_src"."motorcycle_id" AS "motorcycle_id"
			, "tdfv_src"."motorcycle_class_desc" AS "motorcycle_class_desc"
			, "tdfv_src"."motorcycle_subclass_desc" AS "motorcycle_subclass_desc"
			, "tdfv_src"."motorcycle_emotion_desc" AS "motorcycle_emotion_desc"
			, "tdfv_src"."motorcycle_comment" AS "motorcycle_comment"
			, "tdfv_src"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."motorcycle_class_desc" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_motorcycle_class_desc"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."motorcycle_subclass_desc" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_motorcycle_subclass_desc"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."motorcycle_emotion_desc" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_motorcycle_emotion_desc"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."motorcycle_comment" = 'unchanged'::text THEN 0 ELSE 1 END AS "ch_motorcycle_comment"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."update_timestamp" = TO_TIMESTAMP('01/01/1970 00:00:00',
				 'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN 0 ELSE 1 END AS "ch_update_timestamp"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."motorcycle_class_desc" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_motorcycle_class_desc"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."motorcycle_subclass_desc" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_motorcycle_subclass_desc"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."motorcycle_emotion_desc" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_motorcycle_emotion_desc"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."motorcycle_comment" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_motorcycle_comment"
			, CASE WHEN "tdfv_src"."operation" = 'U' AND "tdfv_src"."update_timestamp" = TO_TIMESTAMP('01/01/1970 00:00:00',
				 'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN 1 ELSE 0 END AS "nc_update_timestamp"
		FROM "moto_mktg_scn01_dfv"."vw_campaign_motorcycles" "tdfv_src"
		INNER JOIN "moto_mktg_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	, "change_index" AS 
	( 
		SELECT 
			  "calculate_variables"."load_cycle_id" AS "load_cycle_id"
			, "calculate_variables"."load_date" AS "load_date"
			, "calculate_variables"."trans_timestamp" AS "trans_timestamp"
			, "calculate_variables"."operation" AS "operation"
			, "calculate_variables"."record_type" AS "record_type"
			, "calculate_variables"."campaign_code" AS "campaign_code"
			, "calculate_variables"."campaign_start_date" AS "campaign_start_date"
			, "calculate_variables"."motorcycle_id" AS "motorcycle_id"
			, "calculate_variables"."motorcycle_class_desc" AS "motorcycle_class_desc"
			, "calculate_variables"."motorcycle_subclass_desc" AS "motorcycle_subclass_desc"
			, "calculate_variables"."motorcycle_emotion_desc" AS "motorcycle_emotion_desc"
			, "calculate_variables"."motorcycle_comment" AS "motorcycle_comment"
			, "calculate_variables"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_motorcycle_class_desc")
				OVER(PARTITION BY "calculate_variables"."campaign_code","calculate_variables"."campaign_start_date","calculate_variables"."motorcycle_id" ORDER BY "calculate_variables"."load_date")END AS "ch_index_motorcycle_class_desc"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_motorcycle_subclass_desc")
				OVER(PARTITION BY "calculate_variables"."campaign_code","calculate_variables"."campaign_start_date","calculate_variables"."motorcycle_id" ORDER BY "calculate_variables"."load_date")END AS "ch_index_motorcycle_subclass_desc"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_motorcycle_emotion_desc")
				OVER(PARTITION BY "calculate_variables"."campaign_code","calculate_variables"."campaign_start_date","calculate_variables"."motorcycle_id" ORDER BY "calculate_variables"."load_date")END AS "ch_index_motorcycle_emotion_desc"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_motorcycle_comment")
				OVER(PARTITION BY "calculate_variables"."campaign_code","calculate_variables"."campaign_start_date","calculate_variables"."motorcycle_id" ORDER BY "calculate_variables"."load_date")END AS "ch_index_motorcycle_comment"
			, CASE WHEN "calculate_variables"."operation" != 'U' THEN 0 ELSE SUM("calculate_variables"."ch_update_timestamp")
				OVER(PARTITION BY "calculate_variables"."campaign_code","calculate_variables"."campaign_start_date","calculate_variables"."motorcycle_id" ORDER BY "calculate_variables"."load_date")END AS "ch_index_update_timestamp"
			, "calculate_variables"."nc_motorcycle_class_desc" AS "nc_motorcycle_class_desc"
			, "calculate_variables"."nc_motorcycle_subclass_desc" AS "nc_motorcycle_subclass_desc"
			, "calculate_variables"."nc_motorcycle_emotion_desc" AS "nc_motorcycle_emotion_desc"
			, "calculate_variables"."nc_motorcycle_comment" AS "nc_motorcycle_comment"
			, "calculate_variables"."nc_update_timestamp" AS "nc_update_timestamp"
		FROM "calculate_variables" "calculate_variables"
	)
	, "all_time_slices" AS 
	( 
		SELECT 
			  "sat_src1"."load_date" AS "load_date"
			, "sat_src1"."campaign_code" AS "campaign_code"
			, "sat_src1"."campaign_start_date" AS "campaign_start_date"
			, "sat_src1"."motorcycle_id" AS "motorcycle_id"
			, 'T' AS "record_type"
		FROM "change_index" "change_index"
		INNER JOIN "moto_scn01_fl"."lds_mm_camp_prod_class" "sat_src1" ON  "change_index"."campaign_code" = "sat_src1"."campaign_code" AND  "change_index"."campaign_start_date" = 
			"sat_src1"."campaign_start_date" AND  "change_index"."motorcycle_id" = "sat_src1"."motorcycle_id"
		UNION 
		SELECT 
			  "sat_src2"."load_date" AS "load_date"
			, "sat_src2"."campaign_code" AS "campaign_code"
			, "sat_src2"."campaign_start_date" AS "campaign_start_date"
			, "sat_src2"."motorcycle_id" AS "motorcycle_id"
			, 'T' AS "record_type"
		FROM "change_index" "change_index"
		INNER JOIN "moto_scn01_fl"."lds_mm_camp_prod_emo" "sat_src2" ON  "change_index"."campaign_code" = "sat_src2"."campaign_code" AND  "change_index"."campaign_start_date" = 
			"sat_src2"."campaign_start_date" AND  "change_index"."motorcycle_id" = "sat_src2"."motorcycle_id"
	)
	, "previous_version" AS 
	( 
		SELECT 
			  "all_time_slices"."load_date" AS "load_date"
			, "all_time_slices"."campaign_code" AS "campaign_code"
			, "all_time_slices"."campaign_start_date" AS "campaign_start_date"
			, "all_time_slices"."motorcycle_id" AS "motorcycle_id"
			, ROW_NUMBER()OVER(PARTITION BY "all_time_slices"."campaign_code" ,  "all_time_slices"."campaign_start_date" ,
				  "all_time_slices"."motorcycle_id" ORDER BY "all_time_slices"."load_date" DESC) AS "dummy"
		FROM "all_time_slices" "all_time_slices"
	)
	, "stitch_att_sats" AS 
	( 
		SELECT 
			  COALESCE("stitch_sat1"."load_cycle_id","stitch_sat2"."load_cycle_id", CAST("mex_ats_src"."key_attribute_integer" AS INTEGER)
				) AS "load_cycle_id"
			, COALESCE("stitch_sat1"."load_date","stitch_sat2"."load_date", TO_TIMESTAMP("mex_ats_src"."key_attribute_timestamp",
				 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_date"
			, COALESCE("stitch_sat1"."trans_timestamp","stitch_sat2"."trans_timestamp", TO_TIMESTAMP("mex_ats_src"."key_attribute_timestamp",
				 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "trans_timestamp"
			, COALESCE("stitch_sat1"."campaign_code","stitch_sat2"."campaign_code", "mex_ats_src"."key_attribute_varchar"::text) AS "campaign_code"
			, COALESCE("stitch_sat1"."campaign_start_date","stitch_sat2"."campaign_start_date", TO_DATE("mex_ats_src"."key_attribute_date",
				 'DD/MM/YYYY'::varchar)) AS "campaign_start_date"
			, COALESCE("stitch_sat1"."motorcycle_id","stitch_sat2"."motorcycle_id", TO_NUMBER("mex_ats_src"."key_attribute_numeric",
				 '999999999999D999999999'::varchar)) AS "motorcycle_id"
			, COALESCE("stitch_sat1"."motorcycle_class_desc", 'unchanged'::text) AS "motorcycle_class_desc"
			, COALESCE("stitch_sat1"."motorcycle_subclass_desc", 'unchanged'::text) AS "motorcycle_subclass_desc"
			, COALESCE("stitch_sat1"."update_timestamp", TO_TIMESTAMP('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
				) AS "update_timestamp"
			, COALESCE("stitch_sat2"."motorcycle_emotion_desc", 'unchanged'::text) AS "motorcycle_emotion_desc"
			, COALESCE("stitch_sat2"."motorcycle_comment", 'unchanged'::text) AS "motorcycle_comment"
		FROM "all_time_slices" "all_time_slices"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_ats_src" ON  "mex_ats_src"."record_type" = 'U'
		LEFT OUTER JOIN "moto_scn01_fl"."lds_mm_camp_prod_class" "stitch_sat1" ON  "all_time_slices"."campaign_code" = "stitch_sat1"."campaign_code" AND  "all_time_slices"."campaign_start_date" =
			 "stitch_sat1"."campaign_start_date" AND  "all_time_slices"."motorcycle_id" = "stitch_sat1"."motorcycle_id" AND "all_time_slices"."load_date" = "stitch_sat1"."load_date"
		LEFT OUTER JOIN "moto_scn01_fl"."lds_mm_camp_prod_emo" "stitch_sat2" ON  "all_time_slices"."campaign_code" = "stitch_sat2"."campaign_code" AND  "all_time_slices"."campaign_start_date" =
			 "stitch_sat2"."campaign_start_date" AND  "all_time_slices"."motorcycle_id" = "stitch_sat2"."motorcycle_id" AND "all_time_slices"."load_date" = "stitch_sat2"."load_date"
	)
	, "sat_calculate_variables" AS 
	( 
		SELECT 
			  "stitch_att_sats"."load_cycle_id" AS "load_cycle_id"
			, "stitch_att_sats"."load_date" AS "load_date"
			, "stitch_att_sats"."trans_timestamp" AS "trans_timestamp"
			, "stitch_att_sats"."campaign_code" AS "campaign_code"
			, "stitch_att_sats"."campaign_start_date" AS "campaign_start_date"
			, "stitch_att_sats"."motorcycle_id" AS "motorcycle_id"
			, "stitch_att_sats"."motorcycle_class_desc" AS "motorcycle_class_desc"
			, "stitch_att_sats"."motorcycle_subclass_desc" AS "motorcycle_subclass_desc"
			, "stitch_att_sats"."motorcycle_emotion_desc" AS "motorcycle_emotion_desc"
			, "stitch_att_sats"."motorcycle_comment" AS "motorcycle_comment"
			, "stitch_att_sats"."update_timestamp" AS "update_timestamp"
			, CASE WHEN LAG("stitch_att_sats"."motorcycle_class_desc",1)OVER(PARTITION BY "stitch_att_sats"."campaign_code",
				"stitch_att_sats"."campaign_start_date","stitch_att_sats"."motorcycle_id" ORDER BY "stitch_att_sats"."load_date")!= "stitch_att_sats"."motorcycle_class_desc" THEN 1 ELSE 0 END AS "ch_motorcycle_class_desc"
			, CASE WHEN LAG("stitch_att_sats"."motorcycle_subclass_desc",1)OVER(PARTITION BY "stitch_att_sats"."campaign_code",
				"stitch_att_sats"."campaign_start_date","stitch_att_sats"."motorcycle_id" ORDER BY "stitch_att_sats"."load_date")!= "stitch_att_sats"."motorcycle_subclass_desc" THEN 1 ELSE 0 END AS "ch_motorcycle_subclass_desc"
			, CASE WHEN LAG("stitch_att_sats"."motorcycle_emotion_desc",1)OVER(PARTITION BY "stitch_att_sats"."campaign_code",
				"stitch_att_sats"."campaign_start_date","stitch_att_sats"."motorcycle_id" ORDER BY "stitch_att_sats"."load_date")!= "stitch_att_sats"."motorcycle_emotion_desc" THEN 1 ELSE 0 END AS "ch_motorcycle_emotion_desc"
			, CASE WHEN LAG("stitch_att_sats"."motorcycle_comment",1)OVER(PARTITION BY "stitch_att_sats"."campaign_code",
				"stitch_att_sats"."campaign_start_date","stitch_att_sats"."motorcycle_id" ORDER BY "stitch_att_sats"."load_date")!= "stitch_att_sats"."motorcycle_comment" THEN 1 ELSE 0 END AS "ch_motorcycle_comment"
			, CASE WHEN LAG("stitch_att_sats"."update_timestamp",1)OVER(PARTITION BY "stitch_att_sats"."campaign_code",
				"stitch_att_sats"."campaign_start_date","stitch_att_sats"."motorcycle_id" ORDER BY "stitch_att_sats"."load_date")!= "stitch_att_sats"."update_timestamp" THEN 1 ELSE 0 END AS "ch_update_timestamp"
			, CASE WHEN "stitch_att_sats"."motorcycle_class_desc" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_motorcycle_class_desc"
			, CASE WHEN "stitch_att_sats"."motorcycle_subclass_desc" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_motorcycle_subclass_desc"
			, CASE WHEN "stitch_att_sats"."motorcycle_emotion_desc" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_motorcycle_emotion_desc"
			, CASE WHEN "stitch_att_sats"."motorcycle_comment" = 'unchanged'::text THEN 1 ELSE 0 END AS "nc_motorcycle_comment"
			, CASE WHEN "stitch_att_sats"."update_timestamp" = TO_TIMESTAMP('01/01/1970 00:00:00', 
				'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN 1 ELSE 0 END AS "nc_update_timestamp"
		FROM "stitch_att_sats" "stitch_att_sats"
	)
	, "sat_change_index" AS 
	( 
		SELECT 
			  "sat_calculate_variables"."load_cycle_id" AS "load_cycle_id"
			, "sat_calculate_variables"."load_date" AS "load_date"
			, "sat_calculate_variables"."trans_timestamp" AS "trans_timestamp"
			, "sat_calculate_variables"."campaign_code" AS "campaign_code"
			, "sat_calculate_variables"."campaign_start_date" AS "campaign_start_date"
			, "sat_calculate_variables"."motorcycle_id" AS "motorcycle_id"
			, "sat_calculate_variables"."motorcycle_class_desc" AS "motorcycle_class_desc"
			, "sat_calculate_variables"."motorcycle_subclass_desc" AS "motorcycle_subclass_desc"
			, "sat_calculate_variables"."motorcycle_emotion_desc" AS "motorcycle_emotion_desc"
			, "sat_calculate_variables"."motorcycle_comment" AS "motorcycle_comment"
			, "sat_calculate_variables"."update_timestamp" AS "update_timestamp"
			, SUM("sat_calculate_variables"."ch_motorcycle_class_desc")OVER(PARTITION BY "sat_calculate_variables"."campaign_code",
				"sat_calculate_variables"."campaign_start_date","sat_calculate_variables"."motorcycle_id" ORDER BY "sat_calculate_variables"."load_date") AS "ch_index_motorcycle_class_desc"
			, SUM("sat_calculate_variables"."ch_motorcycle_subclass_desc")OVER(PARTITION BY "sat_calculate_variables"."campaign_code",
				"sat_calculate_variables"."campaign_start_date","sat_calculate_variables"."motorcycle_id" ORDER BY "sat_calculate_variables"."load_date") AS "ch_index_motorcycle_subclass_desc"
			, SUM("sat_calculate_variables"."ch_motorcycle_emotion_desc")OVER(PARTITION BY "sat_calculate_variables"."campaign_code",
				"sat_calculate_variables"."campaign_start_date","sat_calculate_variables"."motorcycle_id" ORDER BY "sat_calculate_variables"."load_date") AS "ch_index_motorcycle_emotion_desc"
			, SUM("sat_calculate_variables"."ch_motorcycle_comment")OVER(PARTITION BY "sat_calculate_variables"."campaign_code",
				"sat_calculate_variables"."campaign_start_date","sat_calculate_variables"."motorcycle_id" ORDER BY "sat_calculate_variables"."load_date") AS "ch_index_motorcycle_comment"
			, SUM("sat_calculate_variables"."ch_update_timestamp")OVER(PARTITION BY "sat_calculate_variables"."campaign_code",
				"sat_calculate_variables"."campaign_start_date","sat_calculate_variables"."motorcycle_id" ORDER BY "sat_calculate_variables"."load_date") AS "ch_index_update_timestamp"
			, "sat_calculate_variables"."nc_motorcycle_class_desc" AS "nc_motorcycle_class_desc"
			, "sat_calculate_variables"."nc_motorcycle_subclass_desc" AS "nc_motorcycle_subclass_desc"
			, "sat_calculate_variables"."nc_motorcycle_emotion_desc" AS "nc_motorcycle_emotion_desc"
			, "sat_calculate_variables"."nc_motorcycle_comment" AS "nc_motorcycle_comment"
			, "sat_calculate_variables"."nc_update_timestamp" AS "nc_update_timestamp"
		FROM "sat_calculate_variables" "sat_calculate_variables"
	)
	, "sat_determine_lag_index" AS 
	( 
		SELECT 
			  "sat_change_index"."load_cycle_id" AS "load_cycle_id"
			, "sat_change_index"."load_date" AS "load_date"
			, "sat_change_index"."trans_timestamp" AS "trans_timestamp"
			, "sat_change_index"."campaign_code" AS "campaign_code"
			, "sat_change_index"."campaign_start_date" AS "campaign_start_date"
			, "sat_change_index"."motorcycle_id" AS "motorcycle_id"
			, "sat_change_index"."motorcycle_class_desc" AS "motorcycle_class_desc"
			, "sat_change_index"."motorcycle_subclass_desc" AS "motorcycle_subclass_desc"
			, "sat_change_index"."motorcycle_emotion_desc" AS "motorcycle_emotion_desc"
			, "sat_change_index"."motorcycle_comment" AS "motorcycle_comment"
			, "sat_change_index"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "sat_change_index"."nc_motorcycle_class_desc" = 0 THEN 0 ELSE SUM("sat_change_index"."nc_motorcycle_class_desc")
				OVER(PARTITION BY "sat_change_index"."campaign_code","sat_change_index"."campaign_start_date","sat_change_index"."motorcycle_id","sat_change_index"."ch_index_motorcycle_class_desc" ORDER BY "sat_change_index"."load_date")END AS "lag_index_motorcycle_class_desc"
			, CASE WHEN "sat_change_index"."nc_motorcycle_subclass_desc" = 0 THEN 0 ELSE SUM("sat_change_index"."nc_motorcycle_subclass_desc")
				OVER(PARTITION BY "sat_change_index"."campaign_code","sat_change_index"."campaign_start_date","sat_change_index"."motorcycle_id","sat_change_index"."ch_index_motorcycle_subclass_desc" ORDER BY "sat_change_index"."load_date")END AS "lag_index_motorcycle_subclass_desc"
			, CASE WHEN "sat_change_index"."nc_motorcycle_emotion_desc" = 0 THEN 0 ELSE SUM("sat_change_index"."nc_motorcycle_emotion_desc")
				OVER(PARTITION BY "sat_change_index"."campaign_code","sat_change_index"."campaign_start_date","sat_change_index"."motorcycle_id","sat_change_index"."ch_index_motorcycle_emotion_desc" ORDER BY "sat_change_index"."load_date")END AS "lag_index_motorcycle_emotion_desc"
			, CASE WHEN "sat_change_index"."nc_motorcycle_comment" = 0 THEN 0 ELSE SUM("sat_change_index"."nc_motorcycle_comment")
				OVER(PARTITION BY "sat_change_index"."campaign_code","sat_change_index"."campaign_start_date","sat_change_index"."motorcycle_id","sat_change_index"."ch_index_motorcycle_comment" ORDER BY "sat_change_index"."load_date")END AS "lag_index_motorcycle_comment"
			, CASE WHEN "sat_change_index"."nc_update_timestamp" = 0 THEN 0 ELSE SUM("sat_change_index"."nc_update_timestamp")
				OVER(PARTITION BY "sat_change_index"."campaign_code","sat_change_index"."campaign_start_date","sat_change_index"."motorcycle_id","sat_change_index"."ch_index_update_timestamp" ORDER BY "sat_change_index"."load_date")END AS "lag_index_update_timestamp"
		FROM "sat_change_index" "sat_change_index"
	)
	, "result_sat_values" AS 
	( 
		SELECT 
			  "sat_determine_lag_index"."load_cycle_id" AS "load_cycle_id"
			, "sat_determine_lag_index"."load_date" AS "load_date"
			, "sat_determine_lag_index"."trans_timestamp" AS "trans_timestamp"
			, "sat_determine_lag_index"."campaign_code" AS "campaign_code"
			, "sat_determine_lag_index"."campaign_start_date" AS "campaign_start_date"
			, "sat_determine_lag_index"."motorcycle_id" AS "motorcycle_id"
			, LAG("sat_determine_lag_index"."motorcycle_class_desc", "sat_determine_lag_index"."lag_index_motorcycle_class_desc"::int)
				OVER(PARTITION BY "sat_determine_lag_index"."campaign_code","sat_determine_lag_index"."campaign_start_date","sat_determine_lag_index"."motorcycle_id" ORDER BY "sat_determine_lag_index"."load_date") AS "motorcycle_class_desc"
			, LAG("sat_determine_lag_index"."motorcycle_subclass_desc", "sat_determine_lag_index"."lag_index_motorcycle_subclass_desc"::int)
				OVER(PARTITION BY "sat_determine_lag_index"."campaign_code","sat_determine_lag_index"."campaign_start_date","sat_determine_lag_index"."motorcycle_id" ORDER BY "sat_determine_lag_index"."load_date") AS "motorcycle_subclass_desc"
			, LAG("sat_determine_lag_index"."motorcycle_emotion_desc", "sat_determine_lag_index"."lag_index_motorcycle_emotion_desc"::int)
				OVER(PARTITION BY "sat_determine_lag_index"."campaign_code","sat_determine_lag_index"."campaign_start_date","sat_determine_lag_index"."motorcycle_id" ORDER BY "sat_determine_lag_index"."load_date") AS "motorcycle_emotion_desc"
			, LAG("sat_determine_lag_index"."motorcycle_comment", "sat_determine_lag_index"."lag_index_motorcycle_comment"::int)
				OVER(PARTITION BY "sat_determine_lag_index"."campaign_code","sat_determine_lag_index"."campaign_start_date","sat_determine_lag_index"."motorcycle_id" ORDER BY "sat_determine_lag_index"."load_date") AS "motorcycle_comment"
			, LAG("sat_determine_lag_index"."update_timestamp", "sat_determine_lag_index"."lag_index_update_timestamp"::int)
				OVER(PARTITION BY "sat_determine_lag_index"."campaign_code","sat_determine_lag_index"."campaign_start_date","sat_determine_lag_index"."motorcycle_id" ORDER BY "sat_determine_lag_index"."load_date") AS "update_timestamp"
		FROM "sat_determine_lag_index" "sat_determine_lag_index"
	)
	, "create_set_nc_values" AS 
	( 
		SELECT 
			  "change_index"."load_date" AS "load_date"
			, "change_index"."load_cycle_id" AS "load_cycle_id"
			, "change_index"."trans_timestamp" AS "trans_timestamp"
			, "change_index"."operation" AS "operation"
			, CASE WHEN "change_index"."operation" = 'I' THEN 1 WHEN "change_index"."operation" = 'U' THEN 2 WHEN "change_index"."operation" =
				 'D' THEN 3 ELSE 9999 END AS "order_operation"
			, "change_index"."record_type" AS "record_type"
			, "change_index"."campaign_code" AS "campaign_code"
			, "change_index"."campaign_start_date" AS "campaign_start_date"
			, "change_index"."motorcycle_id" AS "motorcycle_id"
			, "change_index"."motorcycle_class_desc" AS "motorcycle_class_desc"
			, "change_index"."motorcycle_subclass_desc" AS "motorcycle_subclass_desc"
			, "change_index"."motorcycle_emotion_desc" AS "motorcycle_emotion_desc"
			, "change_index"."motorcycle_comment" AS "motorcycle_comment"
			, "change_index"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "change_index"."nc_motorcycle_class_desc" = 0 THEN 0 ELSE SUM("change_index"."nc_motorcycle_class_desc")
				OVER(PARTITION BY "change_index"."campaign_code","change_index"."campaign_start_date","change_index"."motorcycle_id","change_index"."ch_index_motorcycle_class_desc" ORDER BY "change_index"."load_date")END AS "lag_index_motorcycle_class_desc"
			, CASE WHEN "change_index"."nc_motorcycle_subclass_desc" = 0 THEN 0 ELSE SUM("change_index"."nc_motorcycle_subclass_desc")
				OVER(PARTITION BY "change_index"."campaign_code","change_index"."campaign_start_date","change_index"."motorcycle_id","change_index"."ch_index_motorcycle_subclass_desc" ORDER BY "change_index"."load_date")END AS "lag_index_motorcycle_subclass_desc"
			, CASE WHEN "change_index"."nc_motorcycle_emotion_desc" = 0 THEN 0 ELSE SUM("change_index"."nc_motorcycle_emotion_desc")
				OVER(PARTITION BY "change_index"."campaign_code","change_index"."campaign_start_date","change_index"."motorcycle_id","change_index"."ch_index_motorcycle_emotion_desc" ORDER BY "change_index"."load_date")END AS "lag_index_motorcycle_emotion_desc"
			, CASE WHEN "change_index"."nc_motorcycle_comment" = 0 THEN 0 ELSE SUM("change_index"."nc_motorcycle_comment")
				OVER(PARTITION BY "change_index"."campaign_code","change_index"."campaign_start_date","change_index"."motorcycle_id","change_index"."ch_index_motorcycle_comment" ORDER BY "change_index"."load_date")END AS "lag_index_motorcycle_comment"
			, CASE WHEN "change_index"."nc_update_timestamp" = 0 THEN 0 ELSE SUM("change_index"."nc_update_timestamp")
				OVER(PARTITION BY "change_index"."campaign_code","change_index"."campaign_start_date","change_index"."motorcycle_id","change_index"."ch_index_update_timestamp" ORDER BY "change_index"."load_date")END AS "lag_index_update_timestamp"
			, 1 AS "error_code_camp_prod"
		FROM "change_index" "change_index"
		UNION ALL 
		SELECT 
			  "result_sat_values"."load_date" AS "load_date"
			, "result_sat_values"."load_cycle_id" AS "load_cycle_id"
			, "result_sat_values"."trans_timestamp" AS "trans_timestamp"
			, 'SAT' AS "operation"
			, 0 AS "order_operation"
			, 'T' AS "record_type"
			, "result_sat_values"."campaign_code" AS "campaign_code"
			, "result_sat_values"."campaign_start_date" AS "campaign_start_date"
			, "result_sat_values"."motorcycle_id" AS "motorcycle_id"
			, "result_sat_values"."motorcycle_class_desc" AS "motorcycle_class_desc"
			, "result_sat_values"."motorcycle_subclass_desc" AS "motorcycle_subclass_desc"
			, "result_sat_values"."motorcycle_emotion_desc" AS "motorcycle_emotion_desc"
			, "result_sat_values"."motorcycle_comment" AS "motorcycle_comment"
			, "result_sat_values"."update_timestamp" AS "update_timestamp"
			, 0 AS "lag_index_motorcycle_class_desc"
			, 0 AS "lag_index_motorcycle_subclass_desc"
			, 0 AS "lag_index_motorcycle_emotion_desc"
			, 0 AS "lag_index_motorcycle_comment"
			, 0 AS "lag_index_update_timestamp"
			, 1 AS "error_code_camp_prod"
		FROM "previous_version" "previous_version"
		INNER JOIN "result_sat_values" "result_sat_values" ON  "previous_version"."campaign_code" = "result_sat_values"."campaign_code" AND  "previous_version"."campaign_start_date" =
			 "result_sat_values"."campaign_start_date" AND  "previous_version"."motorcycle_id" = "result_sat_values"."motorcycle_id" AND "previous_version"."load_date" = "result_sat_values"."load_date" AND "previous_version"."dummy" = 1
		UNION ALL 
		SELECT 
			  "ext_err_src"."load_date" AS "load_date"
			, "err_lci_src"."load_cycle_id" AS "load_cycle_id"
			, "ext_err_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("ext_err_src"."operation",'U') AS "operation"
			, 0 AS "order_operation"
			, 'E' AS "record_type"
			, "ext_err_src"."campaign_code" AS "campaign_code"
			, "ext_err_src"."campaign_start_date" AS "campaign_start_date"
			, "ext_err_src"."motorcycle_id" AS "motorcycle_id"
			, "ext_err_src"."motorcycle_class_desc" AS "motorcycle_class_desc"
			, "ext_err_src"."motorcycle_subclass_desc" AS "motorcycle_subclass_desc"
			, "ext_err_src"."motorcycle_emotion_desc" AS "motorcycle_emotion_desc"
			, "ext_err_src"."motorcycle_comment" AS "motorcycle_comment"
			, "ext_err_src"."update_timestamp" AS "update_timestamp"
			, 0 AS "lag_index_motorcycle_class_desc"
			, 0 AS "lag_index_motorcycle_subclass_desc"
			, 0 AS "lag_index_motorcycle_emotion_desc"
			, 0 AS "lag_index_motorcycle_comment"
			, 0 AS "lag_index_update_timestamp"
			, CASE WHEN "ext_err_src"."error_code_camp_prod" = 0 THEN 2 ELSE "ext_err_src"."error_code_camp_prod" END AS "error_code_camp_prod"
		FROM "moto_mktg_scn01_ext"."campaign_motorcycles_err" "ext_err_src"
		INNER JOIN "moto_mktg_scn01_mtd"."load_cycle_info" "err_lci_src" ON  1 = 1
		WHERE  "ext_err_src"."error_code_camp_prod" < 4
	)
	, "result_set_nc_values" AS 
	( 
		SELECT 
			  "create_set_nc_values"."load_cycle_id" AS "load_cycle_id"
			, "create_set_nc_values"."load_date" AS "load_date"
			, "create_set_nc_values"."trans_timestamp" AS "trans_timestamp"
			, "create_set_nc_values"."operation" AS "operation"
			, "create_set_nc_values"."record_type" AS "record_type"
			, "create_set_nc_values"."campaign_code" AS "campaign_code"
			, "create_set_nc_values"."campaign_start_date" AS "campaign_start_date"
			, "create_set_nc_values"."motorcycle_id" AS "motorcycle_id"
			, LAG("create_set_nc_values"."motorcycle_class_desc", "create_set_nc_values"."lag_index_motorcycle_class_desc"::int)
				OVER(PARTITION BY "create_set_nc_values"."campaign_code","create_set_nc_values"."campaign_start_date","create_set_nc_values"."motorcycle_id" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "motorcycle_class_desc"
			, LAG("create_set_nc_values"."motorcycle_subclass_desc", "create_set_nc_values"."lag_index_motorcycle_subclass_desc"::int)
				OVER(PARTITION BY "create_set_nc_values"."campaign_code","create_set_nc_values"."campaign_start_date","create_set_nc_values"."motorcycle_id" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "motorcycle_subclass_desc"
			, LAG("create_set_nc_values"."motorcycle_emotion_desc", "create_set_nc_values"."lag_index_motorcycle_emotion_desc"::int)
				OVER(PARTITION BY "create_set_nc_values"."campaign_code","create_set_nc_values"."campaign_start_date","create_set_nc_values"."motorcycle_id" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "motorcycle_emotion_desc"
			, LAG("create_set_nc_values"."motorcycle_comment", "create_set_nc_values"."lag_index_motorcycle_comment"::int)
				OVER(PARTITION BY "create_set_nc_values"."campaign_code","create_set_nc_values"."campaign_start_date","create_set_nc_values"."motorcycle_id" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "motorcycle_comment"
			, LAG("create_set_nc_values"."update_timestamp", "create_set_nc_values"."lag_index_update_timestamp"::int)
				OVER(PARTITION BY "create_set_nc_values"."campaign_code","create_set_nc_values"."campaign_start_date","create_set_nc_values"."motorcycle_id" ORDER BY "create_set_nc_values"."load_date","create_set_nc_values"."order_operation") AS "update_timestamp"
			, "create_set_nc_values"."error_code_camp_prod" AS "error_code_camp_prod"
		FROM "create_set_nc_values" "create_set_nc_values"
	)
	, "calculate_bk" AS 
	( 
		SELECT 
			  "result_set_nc_values"."load_cycle_id" AS "load_cycle_id"
			, "result_set_nc_values"."load_date" AS "load_date"
			, "result_set_nc_values"."trans_timestamp" AS "trans_timestamp"
			, "result_set_nc_values"."operation" AS "operation"
			, "result_set_nc_values"."record_type" AS "record_type"
			, "result_set_nc_values"."campaign_code" AS "campaign_code"
			, "result_set_nc_values"."campaign_start_date" AS "campaign_start_date"
			, "result_set_nc_values"."motorcycle_id" AS "motorcycle_id"
			, COALESCE(UPPER(REPLACE(TRIM("result_set_nc_values"."campaign_code"),'#','\' || '#')),"mex_src_bk"."key_attribute_varchar") AS "campaign_code_fk_campaignstartdate_bk"
			, COALESCE(UPPER( TO_CHAR("result_set_nc_values"."campaign_start_date", 'DD/MM/YYYY'::varchar)),"mex_src_bk"."key_attribute_date") AS "campaign_start_date_fk_campaignstartdate_bk"
			, "result_set_nc_values"."motorcycle_class_desc" AS "motorcycle_class_desc"
			, "result_set_nc_values"."motorcycle_subclass_desc" AS "motorcycle_subclass_desc"
			, "result_set_nc_values"."motorcycle_emotion_desc" AS "motorcycle_emotion_desc"
			, "result_set_nc_values"."motorcycle_comment" AS "motorcycle_comment"
			, "result_set_nc_values"."update_timestamp" AS "update_timestamp"
			, "result_set_nc_values"."error_code_camp_prod" AS "error_code_camp_prod"
		FROM "result_set_nc_values" "result_set_nc_values"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src_bk" ON  "mex_src_bk"."record_type" = 'N'
		WHERE  "result_set_nc_values"."record_type" != 'T'
	)
	SELECT 
		  "calculate_bk"."load_cycle_id" AS "load_cycle_id"
		, CASE WHEN "calculate_bk"."record_type" != 'E' THEN TO_TIMESTAMP(NULL , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
			ELSE "calculate_bk"."load_date" END AS "load_date"
		, "calculate_bk"."trans_timestamp" AS "trans_timestamp"
		, "calculate_bk"."operation" AS "operation"
		, "calculate_bk"."record_type" AS "record_type"
		, "calculate_bk"."campaign_code" AS "campaign_code"
		, "calculate_bk"."campaign_start_date" AS "campaign_start_date"
		, "calculate_bk"."motorcycle_id" AS "motorcycle_id"
		, "calculate_bk"."campaign_code_fk_campaignstartdate_bk" AS "campaign_code_fk_campaignstartdate_bk"
		, "calculate_bk"."campaign_start_date_fk_campaignstartdate_bk" AS "campaign_start_date_fk_campaignstartdate_bk"
		, "calculate_bk"."motorcycle_class_desc" AS "motorcycle_class_desc"
		, "calculate_bk"."motorcycle_subclass_desc" AS "motorcycle_subclass_desc"
		, "calculate_bk"."motorcycle_emotion_desc" AS "motorcycle_emotion_desc"
		, "calculate_bk"."motorcycle_comment" AS "motorcycle_comment"
		, "calculate_bk"."update_timestamp" AS "update_timestamp"
		, "calculate_bk"."error_code_camp_prod" AS "error_code_camp_prod"
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 
