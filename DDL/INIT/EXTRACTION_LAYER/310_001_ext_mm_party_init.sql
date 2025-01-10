CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_mm_party_init"() 
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

	TRUNCATE TABLE "moto_mktg_scn01_ext"."party"  CASCADE;

	INSERT INTO "moto_mktg_scn01_ext"."party"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"party_number"
		,"parent_party_number"
		,"address_number"
		,"name_bk"
		,"birthdate_bk"
		,"gender_bk"
		,"party_type_code_bk"
		,"name"
		,"birthdate"
		,"gender"
		,"party_type_code"
		,"comments"
		,"update_timestamp"
	)
	WITH "load_init_data" AS 
	( 
		SELECT 
			  'I' ::text AS "operation"
			, 'S'::text AS "record_type"
			, COALESCE("ini_src"."party_number", TO_NUMBER("mex_inr_src"."key_attribute_numeric", '999999999999D999999999'::varchar)
				) AS "party_number"
			, COALESCE("ini_src"."parent_party_number", TO_NUMBER("mex_inr_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "parent_party_number"
			, COALESCE("ini_src"."address_number", TO_NUMBER("mex_inr_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "address_number"
			, "ini_src"."name" AS "name"
			, "ini_src"."birthdate" AS "birthdate"
			, "ini_src"."gender" AS "gender"
			, "ini_src"."party_type_code" AS "party_type_code"
			, "ini_src"."comments" AS "comments"
			, "ini_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_mktg_scn01"."party" "ini_src"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_inr_src" ON  "mex_inr_src"."record_type" = 'N'
	)
	, "prep_excep" AS 
	( 
		SELECT 
			  "load_init_data"."operation" AS "operation"
			, "load_init_data"."record_type" AS "record_type"
			, NULL ::int AS "load_cycle_id"
			, "load_init_data"."party_number" AS "party_number"
			, "load_init_data"."parent_party_number" AS "parent_party_number"
			, "load_init_data"."address_number" AS "address_number"
			, "load_init_data"."name" AS "name"
			, "load_init_data"."birthdate" AS "birthdate"
			, "load_init_data"."gender" AS "gender"
			, "load_init_data"."party_type_code" AS "party_type_code"
			, "load_init_data"."comments" AS "comments"
			, "load_init_data"."update_timestamp" AS "update_timestamp"
		FROM "load_init_data" "load_init_data"
		UNION ALL 
		SELECT 
			  'I' ::text AS "operation"
			, "mex_ext_src"."record_type" AS "record_type"
			, "mex_ext_src"."load_cycle_id" ::int AS "load_cycle_id"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "party_number"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "parent_party_number"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "address_number"
			, "mex_ext_src"."key_attribute_varchar"::text AS "name"
			, TO_DATE("mex_ext_src"."key_attribute_date", 'DD/MM/YYYY'::varchar) AS "birthdate"
			, "mex_ext_src"."key_attribute_character"::text AS "gender"
			, "mex_ext_src"."key_attribute_character"::text AS "party_type_code"
			, "mex_ext_src"."attribute_varchar"::text AS "comments"
			, TO_TIMESTAMP("mex_ext_src"."attribute_timestamp", 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "update_timestamp"
		FROM "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_ext_src"
	)
	, "calculate_bk" AS 
	( 
		SELECT 
			  COALESCE("prep_excep"."load_cycle_id","lci_src"."load_cycle_id") AS "load_cycle_id"
			, "lci_src"."load_date" AS "load_date"
			, "lci_src"."load_date" AS "trans_timestamp"
			, "prep_excep"."operation" AS "operation"
			, "prep_excep"."record_type" AS "record_type"
			, "prep_excep"."party_number" AS "party_number"
			, "prep_excep"."parent_party_number" AS "parent_party_number"
			, "prep_excep"."address_number" AS "address_number"
			, COALESCE(UPPER(REPLACE(TRIM("prep_excep"."name"),'#','\' || '#')),"mex_src"."key_attribute_varchar") AS "name_bk"
			, COALESCE(UPPER( TO_CHAR("prep_excep"."birthdate", 'DD/MM/YYYY'::varchar)),"mex_src"."key_attribute_date") AS "birthdate_bk"
			, COALESCE(UPPER(REPLACE(TRIM("prep_excep"."gender"),'#','\' || '#')),"mex_src"."key_attribute_character") AS "gender_bk"
			, COALESCE(UPPER(REPLACE(TRIM("prep_excep"."party_type_code"),'#','\' || '#')),"mex_src"."key_attribute_character") AS "party_type_code_bk"
			, "prep_excep"."name" AS "name"
			, "prep_excep"."birthdate" AS "birthdate"
			, "prep_excep"."gender" AS "gender"
			, "prep_excep"."party_type_code" AS "party_type_code"
			, "prep_excep"."comments" AS "comments"
			, "prep_excep"."update_timestamp" AS "update_timestamp"
		FROM "prep_excep" "prep_excep"
		INNER JOIN "moto_mktg_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	SELECT 
		  "calculate_bk"."load_cycle_id" AS "load_cycle_id"
		, "calculate_bk"."load_date" AS "load_date"
		, "calculate_bk"."trans_timestamp" AS "trans_timestamp"
		, "calculate_bk"."operation" AS "operation"
		, "calculate_bk"."record_type" AS "record_type"
		, "calculate_bk"."party_number" AS "party_number"
		, "calculate_bk"."parent_party_number" AS "parent_party_number"
		, "calculate_bk"."address_number" AS "address_number"
		, "calculate_bk"."name_bk" AS "name_bk"
		, "calculate_bk"."birthdate_bk" AS "birthdate_bk"
		, "calculate_bk"."gender_bk" AS "gender_bk"
		, "calculate_bk"."party_type_code_bk" AS "party_type_code_bk"
		, "calculate_bk"."name" AS "name"
		, "calculate_bk"."birthdate" AS "birthdate"
		, "calculate_bk"."gender" AS "gender"
		, "calculate_bk"."party_type_code" AS "party_type_code"
		, "calculate_bk"."comments" AS "comments"
		, "calculate_bk"."update_timestamp" AS "update_timestamp"
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 
