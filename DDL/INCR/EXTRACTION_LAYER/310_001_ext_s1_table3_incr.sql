CREATE OR REPLACE FUNCTION "project1_proc"."ext_s1_table3_incr"() 
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

	TRUNCATE TABLE "source1_ext"."table3"  CASCADE;

	INSERT INTO "source1_ext"."table3"(
		 "load_cycle_id"
		,"load_date"
		,"jrn_flag"
		,"record_type"
		,"table3_id"
		,"table3_id_bk"
		,"some_attr"
		,"other_attr"
	)
	WITH "calculate_bk" AS 
	( 
		SELECT 
			  "mex_src"."attribute_varchar" AS "jrn_flag"
			, "tdfv_src"."record_type" AS "record_type"
			, "tdfv_src"."table3_id" AS "table3_id"
			, COALESCE(UPPER( "tdfv_src"."table3_id"::text),"mex_src"."key_attribute_integer") AS "table3_id_bk"
			, "tdfv_src"."some_attr" AS "some_attr"
			, "tdfv_src"."other_attr" AS "other_attr"
		FROM "source1_dfv"."vw_table3" "tdfv_src"
		INNER JOIN "source1_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	, "ext_union" AS 
	( 
		SELECT 
			  "lci_src"."load_cycle_id" AS "load_cycle_id"
			, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "calculate_bk"."table3_id_bk"  ORDER BY  "lci_src"."load_date")
				* interval'2 microsecond'   AS "load_date"
			, "calculate_bk"."jrn_flag" AS "jrn_flag"
			, "calculate_bk"."record_type" AS "record_type"
			, "calculate_bk"."table3_id" AS "table3_id"
			, "calculate_bk"."table3_id_bk" AS "table3_id_bk"
			, "calculate_bk"."some_attr" AS "some_attr"
			, "calculate_bk"."other_attr" AS "other_attr"
		FROM "calculate_bk" "calculate_bk"
		INNER JOIN "source1_mtd"."load_cycle_info" "lci_src" ON  1 = 1
	)
	SELECT 
		  "ext_union"."load_cycle_id" AS "load_cycle_id"
		, "ext_union"."load_date" AS "load_date"
		, "ext_union"."jrn_flag" AS "jrn_flag"
		, "ext_union"."record_type" AS "record_type"
		, "ext_union"."table3_id" AS "table3_id"
		, "ext_union"."table3_id_bk" AS "table3_id_bk"
		, "ext_union"."some_attr" AS "some_attr"
		, "ext_union"."other_attr" AS "other_attr"
	FROM "ext_union" "ext_union"
	;
END;



END;
$function$;
 
 
