CREATE OR REPLACE FUNCTION "project1_proc"."sat_s1_table3_incr"() 
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

Vaultspeed version: 5.7.2.13, generation date: 2025/01/14 14:14:32
DV_NAME: dv_0114 - Release: 1(1) - Comment: 1 - Release date: 2025/01/14 14:13:57, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/14 14:14:05, 
SRC_NAME: source1 - Release: source1(2) - Comment: 2 - Release date: 2025/01/14 14:13:16
 */


BEGIN 

BEGIN -- sat_temp_tgt

	TRUNCATE TABLE "dv_0114_fl"."sat_s1_table3_tmp"  CASCADE;

	INSERT INTO "dv_0114_fl"."sat_s1_table3_tmp"(
		 "table3_hkey"
		,"load_date"
		,"load_end_date"
		,"load_cycle_id"
		,"record_type"
		,"source"
		,"equal"
		,"hash_diff"
		,"delete_flag"
		,"table3_id"
		,"some_attr"
		,"other_attr"
	)
	WITH "dist_stg" AS 
	( 
		SELECT DISTINCT 
 			  "stg_dis_src"."table3_hkey" AS "table3_hkey"
			, "stg_dis_src"."load_cycle_id" AS "load_cycle_id"
		FROM "source1_stg"."table3" "stg_dis_src"
		WHERE  "stg_dis_src"."record_type" = 'S'
	)
	, "temp_table_set" AS 
	( 
		SELECT 
			  "stg_temp_src"."table3_hkey" AS "table3_hkey"
			, "stg_temp_src"."load_date" AS "load_date"
			, "stg_temp_src"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS'::varchar) AS "load_end_date"
			, "stg_temp_src"."record_type" AS "record_type"
			, 'STG' AS "source"
			, 1 AS "origin_id"
			, UPPER(ENCODE(DIGEST(COALESCE(RTRIM( REPLACE(COALESCE(TRIM( "stg_temp_src"."some_attr"::text),'~'),'#','\' || 
				'#')|| '#' ||  REPLACE(COALESCE(TRIM( "stg_temp_src"."other_attr"),'~'),'#','\' || '#')|| '#','#' || '~'),'~') ,'MD5'),'HEX')) AS "hash_diff"
			, CASE WHEN "stg_temp_src"."jrn_flag" = 'D' THEN 'Y'::text ELSE 'N'::text END AS "delete_flag"
			, "stg_temp_src"."table3_id" AS "table3_id"
			, "stg_temp_src"."some_attr" AS "some_attr"
			, "stg_temp_src"."other_attr" AS "other_attr"
		FROM "source1_stg"."table3" "stg_temp_src"
		WHERE  "stg_temp_src"."record_type" = 'S'
		UNION ALL 
		SELECT 
			  "sat_src"."table3_hkey" AS "table3_hkey"
			, "sat_src"."load_date" AS "load_date"
			, "sat_src"."load_cycle_id" AS "load_cycle_id"
			, "sat_src"."load_end_date" AS "load_end_date"
			, 'SAT' AS "record_type"
			, 'SAT' AS "source"
			, 0 AS "origin_id"
			, "sat_src"."hash_diff" AS "hash_diff"
			, "sat_src"."delete_flag" AS "delete_flag"
			, "sat_src"."table3_id" AS "table3_id"
			, "sat_src"."some_attr" AS "some_attr"
			, "sat_src"."other_attr" AS "other_attr"
		FROM "dv_0114_fl"."sat_s1_table3" "sat_src"
		INNER JOIN "dist_stg" "dist_stg" ON  "sat_src"."table3_hkey" = "dist_stg"."table3_hkey"
		WHERE  "sat_src"."load_end_date" = TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS'::varchar)
	)
	SELECT 
		  "temp_table_set"."table3_hkey" AS "table3_hkey"
		, "temp_table_set"."load_date" AS "load_date"
		, "temp_table_set"."load_end_date" AS "load_end_date"
		, "temp_table_set"."load_cycle_id" AS "load_cycle_id"
		, "temp_table_set"."record_type" AS "record_type"
		, "temp_table_set"."source" AS "source"
		, CASE WHEN "temp_table_set"."source" = 'STG' AND "temp_table_set"."delete_flag"::text || "temp_table_set"."hash_diff"::text =
			LAG( "temp_table_set"."delete_flag"::text || "temp_table_set"."hash_diff"::text,1)OVER(PARTITION BY "temp_table_set"."table3_hkey" ORDER BY "temp_table_set"."load_date","temp_table_set"."origin_id")THEN 1 ELSE 0 END AS "equal"
		, "temp_table_set"."hash_diff" AS "hash_diff"
		, "temp_table_set"."delete_flag" AS "delete_flag"
		, "temp_table_set"."table3_id" AS "table3_id"
		, "temp_table_set"."some_attr" AS "some_attr"
		, "temp_table_set"."other_attr" AS "other_attr"
	FROM "temp_table_set" "temp_table_set"
	;
END;


BEGIN -- sat_inur_tgt

	INSERT INTO "dv_0114_fl"."sat_s1_table3"(
		 "table3_hkey"
		,"load_date"
		,"load_end_date"
		,"load_cycle_id"
		,"hash_diff"
		,"delete_flag"
		,"table3_id"
		,"some_attr"
		,"other_attr"
	)
	SELECT 
		  "sat_temp_src_inur"."table3_hkey" AS "table3_hkey"
		, "sat_temp_src_inur"."load_date" AS "load_date"
		, "sat_temp_src_inur"."load_end_date" AS "load_end_date"
		, "sat_temp_src_inur"."load_cycle_id" AS "load_cycle_id"
		, "sat_temp_src_inur"."hash_diff" AS "hash_diff"
		, "sat_temp_src_inur"."delete_flag" AS "delete_flag"
		, "sat_temp_src_inur"."table3_id" AS "table3_id"
		, "sat_temp_src_inur"."some_attr" AS "some_attr"
		, "sat_temp_src_inur"."other_attr" AS "other_attr"
	FROM "dv_0114_fl"."sat_s1_table3_tmp" "sat_temp_src_inur"
	WHERE  "sat_temp_src_inur"."source" = 'STG' AND "sat_temp_src_inur"."equal" = 0
	;
END;


BEGIN -- sat_ed_tgt

	WITH "calc_load_end_date" AS 
	( 
		SELECT 
			  "sat_temp_src_us"."table3_hkey" AS "table3_hkey"
			, "sat_temp_src_us"."load_date" AS "load_date"
			, COALESCE(LEAD("sat_temp_src_us"."load_date",1)OVER(PARTITION BY "sat_temp_src_us"."table3_hkey" ORDER BY "sat_temp_src_us"."load_date")
				, TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS'::varchar)) AS "load_end_date"
		FROM "dv_0114_fl"."sat_s1_table3_tmp" "sat_temp_src_us"
		WHERE  "sat_temp_src_us"."equal" = 0
	)
	, "filter_load_end_date" AS 
	( 
		SELECT 
			  "calc_load_end_date"."table3_hkey" AS "table3_hkey"
			, "calc_load_end_date"."load_date" AS "load_date"
			, "calc_load_end_date"."load_end_date" AS "load_end_date"
		FROM "calc_load_end_date" "calc_load_end_date"
		WHERE  "calc_load_end_date"."load_end_date" != TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS'::varchar)
	)
	UPDATE "dv_0114_fl"."sat_s1_table3" "sat_ed_tgt"
	SET 
		 "load_end_date" =  "filter_load_end_date"."load_end_date"
	FROM  "filter_load_end_date"
	WHERE "sat_ed_tgt"."table3_hkey" =  "filter_load_end_date"."table3_hkey"
	  AND "sat_ed_tgt"."load_date" =  "filter_load_end_date"."load_date"
	;
END;



END;
$function$;
 
 
