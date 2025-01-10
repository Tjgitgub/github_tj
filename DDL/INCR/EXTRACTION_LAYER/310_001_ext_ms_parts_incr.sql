CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_ms_parts_incr"() 
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

BEGIN -- ext_tgt

	TRUNCATE TABLE "moto_sales_scn01_ext"."parts"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."parts"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"part_id"
		,"ref_part_number_fk"
		,"ref_part_language_code_fk"
		,"part_number_bk"
		,"part_langua_code_bk"
		,"part_number"
		,"part_language_code"
		,"update_timestamp"
	)
	WITH "calculate_bk" AS 
	( 
		SELECT 
			  "tdfv_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("tdfv_src"."operation","mex_src"."attribute_varchar") AS "operation"
			, "tdfv_src"."record_type" AS "record_type"
			, "tdfv_src"."part_id" AS "part_id"
			, "tdfv_src"."ref_part_number_fk" AS "ref_part_number_fk"
			, "tdfv_src"."ref_part_language_code_fk" AS "ref_part_language_code_fk"
			, CASE WHEN TRIM("tdfv_src"."part_number")= '' OR "tdfv_src"."part_number" IS NULL THEN "mex_src"."key_attribute_varchar" ELSE UPPER(REPLACE(TRIM( "tdfv_src"."part_number")
				,'#','\' || '#'))END AS "part_number_bk"
			, CASE WHEN TRIM("tdfv_src"."part_language_code")= '' OR "tdfv_src"."part_language_code" IS NULL THEN "mex_src"."key_attribute_varchar" ELSE UPPER(REPLACE(TRIM( "tdfv_src"."part_language_code")
				,'#','\' || '#'))END AS "part_langua_code_bk"
			, "tdfv_src"."part_number" AS "part_number"
			, "tdfv_src"."part_language_code" AS "part_language_code"
			, "tdfv_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01_dfv"."vw_parts" "tdfv_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	, "ext_union" AS 
	( 
		SELECT 
			  "lci_src"."load_cycle_id" AS "load_cycle_id"
			, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "calculate_bk"."part_number_bk" ,  "calculate_bk"."part_langua_code_bk"  ORDER BY  "calculate_bk"."trans_timestamp")
				* interval'2 microsecond'   AS "load_date"
			, "calculate_bk"."trans_timestamp" AS "trans_timestamp"
			, "calculate_bk"."operation" AS "operation"
			, "calculate_bk"."record_type" AS "record_type"
			, "calculate_bk"."part_id" AS "part_id"
			, "calculate_bk"."ref_part_number_fk" AS "ref_part_number_fk"
			, "calculate_bk"."ref_part_language_code_fk" AS "ref_part_language_code_fk"
			, "calculate_bk"."part_number_bk" AS "part_number_bk"
			, "calculate_bk"."part_langua_code_bk" AS "part_langua_code_bk"
			, "calculate_bk"."part_number" AS "part_number"
			, "calculate_bk"."part_language_code" AS "part_language_code"
			, "calculate_bk"."update_timestamp" AS "update_timestamp"
		FROM "calculate_bk" "calculate_bk"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
	)
	SELECT 
		  "ext_union"."load_cycle_id" AS "load_cycle_id"
		, "ext_union"."load_date" AS "load_date"
		, "ext_union"."trans_timestamp" AS "trans_timestamp"
		, "ext_union"."operation" AS "operation"
		, "ext_union"."record_type" AS "record_type"
		, "ext_union"."part_id" AS "part_id"
		, "ext_union"."ref_part_number_fk" AS "ref_part_number_fk"
		, "ext_union"."ref_part_language_code_fk" AS "ref_part_language_code_fk"
		, "ext_union"."part_number_bk" AS "part_number_bk"
		, "ext_union"."part_langua_code_bk" AS "part_langua_code_bk"
		, "ext_union"."part_number" AS "part_number"
		, "ext_union"."part_language_code" AS "part_language_code"
		, "ext_union"."update_timestamp" AS "update_timestamp"
	FROM "ext_union" "ext_union"
	;
END;



END;
$function$;
 
 
