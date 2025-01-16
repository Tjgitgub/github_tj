CREATE OR REPLACE FUNCTION "project1_proc"."ext_s1_table1_init"() 
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

BEGIN -- ext_tgt

	TRUNCATE TABLE "source1_ext"."table1"  CASCADE;

	INSERT INTO "source1_ext"."table1"(
		 "load_cycle_id"
		,"load_date"
		,"jrn_flag"
		,"record_type"
		,"other_attr"
		,"other_attr_bk"
		,"table1_id"
		,"some_attr"
	)
	WITH "load_init_data" AS 
	( 
		SELECT 
			  'I' ::text AS "jrn_flag"
			, 'S'::text AS "record_type"
			, COALESCE("ini_src"."other_attr", "mex_inr_src"."key_attribute_varchar"::text) AS "other_attr"
			, "ini_src"."table1_id" AS "table1_id"
			, "ini_src"."some_attr" AS "some_attr"
		FROM "source1_ini"."table1" "ini_src"
		INNER JOIN "source1_mtd"."mtd_exception_records" "mex_inr_src" ON  "mex_inr_src"."record_type" = 'N'
	)
	, "prep_excep" AS 
	( 
		SELECT 
			  "load_init_data"."jrn_flag" AS "jrn_flag"
			, "load_init_data"."record_type" AS "record_type"
			, NULL ::int AS "load_cycle_id"
			, "load_init_data"."other_attr" AS "other_attr"
			, "load_init_data"."table1_id" AS "table1_id"
			, "load_init_data"."some_attr" AS "some_attr"
		FROM "load_init_data" "load_init_data"
		UNION ALL 
		SELECT 
			  'I' ::text AS "jrn_flag"
			, "mex_ext_src"."record_type" AS "record_type"
			, "mex_ext_src"."load_cycle_id" ::int AS "load_cycle_id"
			, "mex_ext_src"."key_attribute_varchar"::text AS "other_attr"
			, CAST("mex_ext_src"."attribute_integer" AS INTEGER) AS "table1_id"
			, CAST("mex_ext_src"."attribute_integer" AS INTEGER) AS "some_attr"
		FROM "source1_mtd"."mtd_exception_records" "mex_ext_src"
	)
	, "calculate_bk" AS 
	( 
		SELECT 
			  COALESCE("prep_excep"."load_cycle_id","lci_src"."load_cycle_id") AS "load_cycle_id"
			, "lci_src"."load_date" AS "load_date"
			, "prep_excep"."jrn_flag" AS "jrn_flag"
			, "prep_excep"."record_type" AS "record_type"
			, "prep_excep"."other_attr" AS "other_attr"
			, COALESCE(UPPER(REPLACE(TRIM("prep_excep"."other_attr"),'#','\' || '#')),"mex_src"."key_attribute_varchar") AS "other_attr_bk"
			, "prep_excep"."table1_id" AS "table1_id"
			, "prep_excep"."some_attr" AS "some_attr"
		FROM "prep_excep" "prep_excep"
		INNER JOIN "source1_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		INNER JOIN "source1_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	SELECT 
		  "calculate_bk"."load_cycle_id" AS "load_cycle_id"
		, "calculate_bk"."load_date" AS "load_date"
		, "calculate_bk"."jrn_flag" AS "jrn_flag"
		, "calculate_bk"."record_type" AS "record_type"
		, "calculate_bk"."other_attr" AS "other_attr"
		, "calculate_bk"."other_attr_bk" AS "other_attr_bk"
		, "calculate_bk"."table1_id" AS "table1_id"
		, "calculate_bk"."some_attr" AS "some_attr"
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 