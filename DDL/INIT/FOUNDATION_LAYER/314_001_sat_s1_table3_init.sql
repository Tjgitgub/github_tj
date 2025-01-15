CREATE OR REPLACE FUNCTION "project1_proc"."sat_s1_table3_init"() 
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

BEGIN -- sat_tgt

	TRUNCATE TABLE "dv_0114_fl"."sat_s1_table3"  CASCADE;

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
	WITH "stg_src" AS 
	( 
		SELECT 
			  "stg_inr_src"."table3_hkey" AS "table3_hkey"
			, "stg_inr_src"."load_date" AS "load_date"
			, TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS'::varchar) AS "load_end_date"
			, "stg_inr_src"."load_cycle_id" AS "load_cycle_id"
			, UPPER(ENCODE(DIGEST(COALESCE(RTRIM( REPLACE(COALESCE(TRIM( "stg_inr_src"."some_attr"::text),'~'),'#','\' || 
				'#')|| '#' ||  REPLACE(COALESCE(TRIM( "stg_inr_src"."other_attr"),'~'),'#','\' || '#')|| '#','#' || '~'),'~') ,'MD5'),'HEX')) AS "hash_diff"
			, 'N'::text AS "delete_flag"
			, "stg_inr_src"."table3_id" AS "table3_id"
			, "stg_inr_src"."some_attr" AS "some_attr"
			, "stg_inr_src"."other_attr" AS "other_attr"
			, ROW_NUMBER()OVER(PARTITION BY "stg_inr_src"."table3_hkey" ORDER BY "stg_inr_src"."load_date") AS "dummy"
		FROM "source1_stg"."table3" "stg_inr_src"
	)
	SELECT 
		  "stg_src"."table3_hkey" AS "table3_hkey"
		, "stg_src"."load_date" AS "load_date"
		, "stg_src"."load_end_date" AS "load_end_date"
		, "stg_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_src"."hash_diff" AS "hash_diff"
		, "stg_src"."delete_flag" AS "delete_flag"
		, "stg_src"."table3_id" AS "table3_id"
		, "stg_src"."some_attr" AS "some_attr"
		, "stg_src"."other_attr" AS "other_attr"
	FROM "stg_src" "stg_src"
	WHERE  "stg_src"."dummy" = 1
	;
END;



END;
$function$;
 
 
