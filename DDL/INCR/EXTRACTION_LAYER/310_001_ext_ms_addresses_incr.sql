CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ext_ms_addresses_incr"() 
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

	TRUNCATE TABLE "moto_sales_scn01_ext"."addresses"  CASCADE;

	INSERT INTO "moto_sales_scn01_ext"."addresses"(
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
		,"coordinates"
		,"update_timestamp"
	)
	WITH "calculate_bk" AS 
	( 
		SELECT 
			  "tdfv_src"."trans_timestamp" AS "trans_timestamp"
			, COALESCE("tdfv_src"."operation","mex_src"."attribute_varchar") AS "operation"
			, "tdfv_src"."record_type" AS "record_type"
			, "tdfv_src"."address_number" AS "address_number"
			, CASE WHEN TRIM("tdfv_src"."street_name")= '' OR "tdfv_src"."street_name" IS NULL THEN "mex_src"."key_attribute_varchar" ELSE UPPER(REPLACE(TRIM( "tdfv_src"."street_name")
				,'#','\' || '#'))END AS "street_name_bk"
			, COALESCE(UPPER( "tdfv_src"."street_number"::text),"mex_src"."key_attribute_numeric") AS "street_number_bk"
			, CASE WHEN TRIM("tdfv_src"."postal_code")= '' OR "tdfv_src"."postal_code" IS NULL THEN "mex_src"."key_attribute_varchar" ELSE UPPER(REPLACE(TRIM( "tdfv_src"."postal_code")
				,'#','\' || '#'))END AS "postal_code_bk"
			, CASE WHEN TRIM("tdfv_src"."city")= '' OR "tdfv_src"."city" IS NULL THEN "mex_src"."key_attribute_varchar" ELSE UPPER(REPLACE(TRIM( "tdfv_src"."city")
				,'#','\' || '#'))END AS "city_bk"
			, "tdfv_src"."street_name" AS "street_name"
			, "tdfv_src"."street_number" AS "street_number"
			, "tdfv_src"."postal_code" AS "postal_code"
			, "tdfv_src"."city" AS "city"
			, "tdfv_src"."coordinates" AS "coordinates"
			, "tdfv_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01_dfv"."vw_addresses" "tdfv_src"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_src" ON  1 = 1
		WHERE  "mex_src"."record_type" = 'N'
	)
	, "ext_union" AS 
	( 
		SELECT 
			  "lci_src"."load_cycle_id" AS "load_cycle_id"
			, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "calculate_bk"."street_name_bk" ,  "calculate_bk"."street_number_bk" ,
				  "calculate_bk"."postal_code_bk" ,  "calculate_bk"."city_bk"  ORDER BY  "calculate_bk"."trans_timestamp") * interval'2 microsecond'   AS "load_date"
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
			, "calculate_bk"."coordinates" AS "coordinates"
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
		, "ext_union"."address_number" AS "address_number"
		, "ext_union"."street_name_bk" AS "street_name_bk"
		, "ext_union"."street_number_bk" AS "street_number_bk"
		, "ext_union"."postal_code_bk" AS "postal_code_bk"
		, "ext_union"."city_bk" AS "city_bk"
		, "ext_union"."street_name" AS "street_name"
		, "ext_union"."street_number" AS "street_number"
		, "ext_union"."postal_code" AS "postal_code"
		, "ext_union"."city" AS "city"
		, "ext_union"."coordinates" AS "coordinates"
		, "ext_union"."update_timestamp" AS "update_timestamp"
	FROM "ext_union" "ext_union"
	;
END;



END;
$function$;
 
 
