CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_ms_parts_incr"() 
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

BEGIN -- stg_tgt

	TRUNCATE TABLE "moto_sales_scn01_stg"."parts"  CASCADE;

	INSERT INTO "moto_sales_scn01_stg"."parts"(
		 "parts_hkey"
		,"load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"part_id"
		,"ref_part_number_fk"
		,"ref_part_language_code_fk"
		,"part_number"
		,"part_language_code"
		,"part_number_bk"
		,"part_langua_code_bk"
		,"update_timestamp"
	)
	WITH "calc_hash_keys" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST( "ext_src"."part_number_bk" || '#' ||  "ext_src"."part_langua_code_bk" || '#' ,'MD5'),
				'HEX')) AS "parts_hkey"
			, "ext_src"."load_date" AS "load_date"
			, "ext_src"."load_cycle_id" AS "load_cycle_id"
			, "ext_src"."trans_timestamp" AS "trans_timestamp"
			, "ext_src"."operation" AS "operation"
			, "ext_src"."record_type" AS "record_type"
			, "ext_src"."part_id" AS "part_id"
			, "ext_src"."ref_part_number_fk" AS "ref_part_number_fk"
			, "ext_src"."ref_part_language_code_fk" AS "ref_part_language_code_fk"
			, "ext_src"."part_number" AS "part_number"
			, "ext_src"."part_language_code" AS "part_language_code"
			, "ext_src"."part_number_bk" AS "part_number_bk"
			, "ext_src"."part_langua_code_bk" AS "part_langua_code_bk"
			, "ext_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01_ext"."parts" "ext_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
	)
	SELECT 
		  "calc_hash_keys"."parts_hkey" AS "parts_hkey"
		, "calc_hash_keys"."load_date" AS "load_date"
		, "calc_hash_keys"."load_cycle_id" AS "load_cycle_id"
		, "calc_hash_keys"."trans_timestamp" AS "trans_timestamp"
		, "calc_hash_keys"."operation" AS "operation"
		, "calc_hash_keys"."record_type" AS "record_type"
		, "calc_hash_keys"."part_id" AS "part_id"
		, "calc_hash_keys"."ref_part_number_fk" AS "ref_part_number_fk"
		, "calc_hash_keys"."ref_part_language_code_fk" AS "ref_part_language_code_fk"
		, "calc_hash_keys"."part_number" AS "part_number"
		, "calc_hash_keys"."part_language_code" AS "part_language_code"
		, "calc_hash_keys"."part_number_bk" AS "part_number_bk"
		, "calc_hash_keys"."part_langua_code_bk" AS "part_langua_code_bk"
		, "calc_hash_keys"."update_timestamp" AS "update_timestamp"
	FROM "calc_hash_keys" "calc_hash_keys"
	;
END;



END;
$function$;
 
 
