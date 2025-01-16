CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_mm_addresses_init"() 
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

Vaultspeed version: 5.7.2.16, generation date: 2025/01/16 15:01:59
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/16 14:56:23, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46
 */


BEGIN 

BEGIN -- ext_tgt

	TRUNCATE TABLE "moto_mktg_scn01_ext"."addresses"  CASCADE;

	INSERT INTO "moto_mktg_scn01_ext"."addresses"(
		 "load_cycle_id"
		,"load_date"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"address_number"
		,"street_name_bk"
		,"street_number_bk"
		,"postal_code_bk"
		,"city_bk"
		,"street_name"
		,"street_number"
		,"postal_code"
		,"city"
		,"province"
		,"update_timestamp"
	)
	WITH "load_init_data" AS 
	( 
		SELECT 
			  'I' ::text AS "operation"
			, 'S'::text AS "record_type"
			, COALESCE("ini_src"."address_number", TO_NUMBER("mex_inr_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "address_number"
			, "ini_src"."street_name" AS "street_name"
			, "ini_src"."street_number" AS "street_number"
			, "ini_src"."postal_code" AS "postal_code"
			, "ini_src"."city" AS "city"
			, "ini_src"."province" AS "province"
			, "ini_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_mktg_scn01"."addresses" "ini_src"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_inr_src" ON  "mex_inr_src"."record_type" = 'N'
	)
	, "prep_excep" AS 
	( 
		SELECT 
			  "load_init_data"."operation" AS "operation"
			, "load_init_data"."record_type" AS "record_type"
			, NULL ::int AS "load_cycle_id"
			, "load_init_data"."address_number" AS "address_number"
			, "load_init_data"."street_name" AS "street_name"
			, "load_init_data"."street_number" AS "street_number"
			, "load_init_data"."postal_code" AS "postal_code"
			, "load_init_data"."city" AS "city"
			, "load_init_data"."province" AS "province"
			, "load_init_data"."update_timestamp" AS "update_timestamp"
		FROM "load_init_data" "load_init_data"
		UNION ALL 
		SELECT 
			  'I' ::text AS "operation"
			, "mex_ext_src"."record_type" AS "record_type"
			, "mex_ext_src"."load_cycle_id" ::int AS "load_cycle_id"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "address_number"
			, "mex_ext_src"."key_attribute_varchar"::text AS "street_name"
			, TO_NUMBER("mex_ext_src"."key_attribute_numeric", '999999999999D999999999'::varchar) AS "street_number"
			, "mex_ext_src"."key_attribute_varchar"::text AS "postal_code"
			, "mex_ext_src"."key_attribute_varchar"::text AS "city"
			, "mex_ext_src"."attribute_varchar"::text AS "province"
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
			, "prep_excep"."address_number" AS "address_number"
			, COALESCE(UPPER(REPLACE(TRIM("prep_excep"."street_name"),'#','\' || '#')),"mex_src"."key_attribute_varchar") AS "street_name_bk"
			, COALESCE(UPPER( "prep_excep"."street_number"::text),"mex_src"."key_attribute_numeric") AS "street_number_bk"
			, COALESCE(UPPER(REPLACE(TRIM("prep_excep"."postal_code"),'#','\' || '#')),"mex_src"."key_attribute_varchar") AS "postal_code_bk"
			, COALESCE(UPPER(REPLACE(TRIM("prep_excep"."city"),'#','\' || '#')),"mex_src"."key_attribute_varchar") AS "city_bk"
			, "prep_excep"."street_name" AS "street_name"
			, "prep_excep"."street_number" AS "street_number"
			, "prep_excep"."postal_code" AS "postal_code"
			, "prep_excep"."city" AS "city"
			, "prep_excep"."province" AS "province"
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
		, "calculate_bk"."address_number" AS "address_number"
		, "calculate_bk"."street_name_bk" AS "street_name_bk"
		, "calculate_bk"."street_number_bk" AS "street_number_bk"
		, "calculate_bk"."postal_code_bk" AS "postal_code_bk"
		, "calculate_bk"."city_bk" AS "city_bk"
		, "calculate_bk"."street_name" AS "street_name"
		, "calculate_bk"."street_number" AS "street_number"
		, "calculate_bk"."postal_code" AS "postal_code"
		, "calculate_bk"."city" AS "city"
		, "calculate_bk"."province" AS "province"
		, "calculate_bk"."update_timestamp" AS "update_timestamp"
	FROM "calculate_bk" "calculate_bk"
	;
END;



END;
$function$;
 
 
