CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_mm_party_incr"() 
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

BEGIN -- stg_tgt

	TRUNCATE TABLE "moto_mktg_scn01_stg"."party"  CASCADE;

	INSERT INTO "moto_mktg_scn01_stg"."party"(
		 "customers_hkey"
		,"customers_parentpartynumber_hkey"
		,"addresses_hkey"
		,"lnk_cust_cust_parentpartynumber_hkey"
		,"lnk_cust_addr_hkey"
		,"load_date"
		,"load_cycle_id"
		,"src_bk"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"party_number"
		,"parent_party_number"
		,"address_number"
		,"customers_bk"
		,"name"
		,"birthdate"
		,"gender"
		,"party_type_code"
		,"name_fk_parentpartynumber_bk"
		,"birthdate_fk_parentpartynumber_bk"
		,"gender_fk_parentpartynumber_bk"
		,"party_type_code_fk_parentpartynumber_bk"
		,"street_name_fk_addressnumber_bk"
		,"street_number_fk_addressnumber_bk"
		,"postal_code_fk_addressnumber_bk"
		,"city_fk_addressnumber_bk"
		,"comments"
		,"update_timestamp"
		,"error_code_cust_cust_parentpartynumber"
		,"error_code_cust_addr"
	)
	WITH "dist_fk1" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src1"."parent_party_number" AS "parent_party_number"
		FROM "moto_mktg_scn01_ext"."party" "ext_dis_src1"
	)
	, "dist_fk2" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src2"."address_number" AS "address_number"
		FROM "moto_mktg_scn01_ext"."party" "ext_dis_src2"
	)
	, "prep_find_bk_fk1" AS 
	( 
		SELECT 
			  UPPER(REPLACE(TRIM( "sat_src1"."name"),'#','\' || '#')) AS "name_bk"
			, UPPER( TO_CHAR("sat_src1"."birthdate", 'DD/MM/YYYY'::varchar)) AS "birthdate_bk"
			, UPPER(REPLACE(TRIM( "sat_src1"."gender"),'#','\' || '#')) AS "gender_bk"
			, UPPER(REPLACE(TRIM( "sat_src1"."party_type_code"),'#','\' || '#')) AS "party_type_code_bk"
			, "dist_fk1"."parent_party_number" AS "parent_party_number"
			, "sat_src1"."load_date" AS "load_date"
			, 1 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_scn01_fl"."sat_mm_customers" "sat_src1" ON  "dist_fk1"."parent_party_number" = "sat_src1"."party_number"
		INNER JOIN "moto_scn01_fl"."hub_customers" "hub_src1" ON  "hub_src1"."customers_hkey" = "sat_src1"."customers_hkey"
		WHERE  "sat_src1"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
		UNION ALL 
		SELECT 
			  "ext_fkbk_src1"."name_bk" AS "name_bk"
			, "ext_fkbk_src1"."birthdate_bk" AS "birthdate_bk"
			, "ext_fkbk_src1"."gender_bk" AS "gender_bk"
			, "ext_fkbk_src1"."party_type_code_bk" AS "party_type_code_bk"
			, "dist_fk1"."parent_party_number" AS "parent_party_number"
			, "ext_fkbk_src1"."load_date" AS "load_date"
			, 0 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_mktg_scn01_ext"."party" "ext_fkbk_src1" ON  "dist_fk1"."parent_party_number" = "ext_fkbk_src1"."party_number"
	)
	, "prep_find_bk_fk2" AS 
	( 
		SELECT 
			  "hub_src2"."street_name_bk" AS "street_name_bk"
			, "hub_src2"."street_number_bk" AS "street_number_bk"
			, "hub_src2"."postal_code_bk" AS "postal_code_bk"
			, "hub_src2"."city_bk" AS "city_bk"
			, "dist_fk2"."address_number" AS "address_number"
			, "sat_src2"."load_date" AS "load_date"
			, 1 AS "general_order"
		FROM "dist_fk2" "dist_fk2"
		INNER JOIN "moto_scn01_fl"."sat_mm_addresses" "sat_src2" ON  "dist_fk2"."address_number" = "sat_src2"."address_number"
		INNER JOIN "moto_scn01_fl"."hub_addresses" "hub_src2" ON  "hub_src2"."addresses_hkey" = "sat_src2"."addresses_hkey"
		WHERE  "sat_src2"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
		UNION ALL 
		SELECT 
			  "ext_fkbk_src2"."street_name_bk" AS "street_name_bk"
			, "ext_fkbk_src2"."street_number_bk" AS "street_number_bk"
			, "ext_fkbk_src2"."postal_code_bk" AS "postal_code_bk"
			, "ext_fkbk_src2"."city_bk" AS "city_bk"
			, "dist_fk2"."address_number" AS "address_number"
			, "ext_fkbk_src2"."load_date" AS "load_date"
			, 0 AS "general_order"
		FROM "dist_fk2" "dist_fk2"
		INNER JOIN "moto_mktg_scn01_ext"."addresses" "ext_fkbk_src2" ON  "dist_fk2"."address_number" = "ext_fkbk_src2"."address_number"
	)
	, "order_bk_fk1" AS 
	( 
		SELECT 
			  "prep_find_bk_fk1"."name_bk" AS "name_bk"
			, "prep_find_bk_fk1"."birthdate_bk" AS "birthdate_bk"
			, "prep_find_bk_fk1"."gender_bk" AS "gender_bk"
			, "prep_find_bk_fk1"."party_type_code_bk" AS "party_type_code_bk"
			, "prep_find_bk_fk1"."parent_party_number" AS "parent_party_number"
			, ROW_NUMBER()OVER(PARTITION BY "prep_find_bk_fk1"."parent_party_number" ORDER BY "prep_find_bk_fk1"."general_order",
				"prep_find_bk_fk1"."load_date" DESC) AS "dummy"
		FROM "prep_find_bk_fk1" "prep_find_bk_fk1"
	)
	, "order_bk_fk2" AS 
	( 
		SELECT 
			  "prep_find_bk_fk2"."street_name_bk" AS "street_name_bk"
			, "prep_find_bk_fk2"."street_number_bk" AS "street_number_bk"
			, "prep_find_bk_fk2"."postal_code_bk" AS "postal_code_bk"
			, "prep_find_bk_fk2"."city_bk" AS "city_bk"
			, "prep_find_bk_fk2"."address_number" AS "address_number"
			, ROW_NUMBER()OVER(PARTITION BY "prep_find_bk_fk2"."address_number" ORDER BY "prep_find_bk_fk2"."general_order",
				"prep_find_bk_fk2"."load_date" DESC) AS "dummy"
		FROM "prep_find_bk_fk2" "prep_find_bk_fk2"
	)
	, "find_bk_fk1" AS 
	( 
		SELECT 
			  "order_bk_fk1"."name_bk" AS "name_bk"
			, "order_bk_fk1"."birthdate_bk" AS "birthdate_bk"
			, "order_bk_fk1"."gender_bk" AS "gender_bk"
			, "order_bk_fk1"."party_type_code_bk" AS "party_type_code_bk"
			, "order_bk_fk1"."parent_party_number" AS "parent_party_number"
		FROM "order_bk_fk1" "order_bk_fk1"
		WHERE  "order_bk_fk1"."dummy" = 1
	)
	, "find_bk_fk2" AS 
	( 
		SELECT 
			  "order_bk_fk2"."street_name_bk" AS "street_name_bk"
			, "order_bk_fk2"."street_number_bk" AS "street_number_bk"
			, "order_bk_fk2"."postal_code_bk" AS "postal_code_bk"
			, "order_bk_fk2"."city_bk" AS "city_bk"
			, "order_bk_fk2"."address_number" AS "address_number"
		FROM "order_bk_fk2" "order_bk_fk2"
		WHERE  "order_bk_fk2"."dummy" = 1
	)
	, "calc_hash_keys" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST( 'moto_mktg_scn01' || '#' || "ext_src"."name_bk" || '#' ||  "ext_src"."birthdate_bk" || 
				'#' ||  "ext_src"."gender_bk" || '#' ||  "ext_src"."party_type_code_bk" || '#' ,'MD5'),'HEX')) AS "customers_hkey"
			, UPPER(ENCODE(DIGEST( 'moto_mktg_scn01' || '#' || COALESCE("find_bk_fk1"."name_bk","mex_src"."key_attribute_varchar")
				|| '#' ||  COALESCE("find_bk_fk1"."birthdate_bk","mex_src"."key_attribute_date")|| '#' ||  COALESCE("find_bk_fk1"."gender_bk","mex_src"."key_attribute_character")|| '#' ||  COALESCE("find_bk_fk1"."party_type_code_bk","mex_src"."key_attribute_character")|| '#' ,'MD5'),'HEX')) AS "customers_parentpartynumber_hkey"
			, UPPER(ENCODE(DIGEST( COALESCE("find_bk_fk2"."street_name_bk","mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk2"."street_number_bk",
				"mex_src"."key_attribute_numeric")|| '#' ||  COALESCE("find_bk_fk2"."postal_code_bk","mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk2"."city_bk","mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "addresses_hkey"
			, UPPER(ENCODE(DIGEST( 'moto_mktg_scn01' || '#' || "ext_src"."name_bk" || '#' ||  "ext_src"."birthdate_bk" || 
				'#' ||  "ext_src"."gender_bk" || '#' ||  "ext_src"."party_type_code_bk" || '#' || 'moto_mktg_scn01' || '#' || COALESCE("find_bk_fk1"."name_bk","mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk1"."birthdate_bk","mex_src"."key_attribute_date")|| '#' ||  COALESCE("find_bk_fk1"."gender_bk","mex_src"."key_attribute_character")|| '#' ||  COALESCE("find_bk_fk1"."party_type_code_bk","mex_src"."key_attribute_character")|| '#' ,'MD5'),'HEX')) AS "lnk_cust_cust_parentpartynumber_hkey"
			, UPPER(ENCODE(DIGEST( 'moto_mktg_scn01' || '#' || "ext_src"."name_bk" || '#' ||  "ext_src"."birthdate_bk" || 
				'#' ||  "ext_src"."gender_bk" || '#' ||  "ext_src"."party_type_code_bk" || '#' || COALESCE("find_bk_fk2"."street_name_bk","mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk2"."street_number_bk","mex_src"."key_attribute_numeric")|| '#' ||  COALESCE("find_bk_fk2"."postal_code_bk","mex_src"."key_attribute_varchar")|| '#' ||  COALESCE("find_bk_fk2"."city_bk","mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "lnk_cust_addr_hkey"
			, "ext_src"."load_date" AS "load_date"
			, "ext_src"."load_cycle_id" AS "load_cycle_id"
			, 'moto_mktg_scn01' AS "src_bk"
			, "ext_src"."trans_timestamp" AS "trans_timestamp"
			, "ext_src"."operation" AS "operation"
			, "ext_src"."record_type" AS "record_type"
			, "ext_src"."party_number" AS "party_number"
			, "ext_src"."parent_party_number" AS "parent_party_number"
			, "ext_src"."address_number" AS "address_number"
			, "ext_src"."name_bk" || '#' ||  "ext_src"."birthdate_bk" || '#' ||  "ext_src"."gender_bk" || '#' ||  
				"ext_src"."party_type_code_bk" AS "customers_bk"
			, "ext_src"."name" AS "name"
			, "ext_src"."birthdate" AS "birthdate"
			, "ext_src"."gender" AS "gender"
			, "ext_src"."party_type_code" AS "party_type_code"
			, NULL::text AS "name_fk_parentpartynumber_bk"
			, NULL::text AS "birthdate_fk_parentpartynumber_bk"
			, NULL::text AS "gender_fk_parentpartynumber_bk"
			, NULL::text AS "party_type_code_fk_parentpartynumber_bk"
			, NULL::text AS "street_name_fk_addressnumber_bk"
			, NULL::text AS "street_number_fk_addressnumber_bk"
			, NULL::text AS "postal_code_fk_addressnumber_bk"
			, NULL::text AS "city_fk_addressnumber_bk"
			, "ext_src"."comments" AS "comments"
			, "ext_src"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "ext_src"."error_code_cust_cust_parentpartynumber" = 2 THEN 2 WHEN "ext_src"."error_code_cust_cust_parentpartynumber" =
				- 1 THEN 0 WHEN "find_bk_fk1"."parent_party_number" IS NULL THEN "ext_src"."error_code_cust_cust_parentpartynumber" ELSE 0 END AS "error_code_cust_cust_parentpartynumber"
			, CASE WHEN "ext_src"."error_code_cust_addr" = 2 THEN 2 WHEN "ext_src"."error_code_cust_addr" =
				- 1 THEN 0 WHEN "find_bk_fk2"."address_number" IS NULL THEN "ext_src"."error_code_cust_addr" ELSE 0 END AS "error_code_cust_addr"
		FROM "moto_mktg_scn01_ext"."party" "ext_src"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
		LEFT OUTER JOIN "find_bk_fk1" "find_bk_fk1" ON  "ext_src"."parent_party_number" = "find_bk_fk1"."parent_party_number"
		LEFT OUTER JOIN "find_bk_fk2" "find_bk_fk2" ON  "ext_src"."address_number" = "find_bk_fk2"."address_number"
	)
	SELECT 
		  "calc_hash_keys"."customers_hkey" AS "customers_hkey"
		, "calc_hash_keys"."customers_parentpartynumber_hkey" AS "customers_parentpartynumber_hkey"
		, "calc_hash_keys"."addresses_hkey" AS "addresses_hkey"
		, "calc_hash_keys"."lnk_cust_cust_parentpartynumber_hkey" AS "lnk_cust_cust_parentpartynumber_hkey"
		, "calc_hash_keys"."lnk_cust_addr_hkey" AS "lnk_cust_addr_hkey"
		, "calc_hash_keys"."load_date" AS "load_date"
		, "calc_hash_keys"."load_cycle_id" AS "load_cycle_id"
		, "calc_hash_keys"."src_bk" AS "src_bk"
		, "calc_hash_keys"."trans_timestamp" AS "trans_timestamp"
		, "calc_hash_keys"."operation" AS "operation"
		, "calc_hash_keys"."record_type" AS "record_type"
		, "calc_hash_keys"."party_number" AS "party_number"
		, "calc_hash_keys"."parent_party_number" AS "parent_party_number"
		, "calc_hash_keys"."address_number" AS "address_number"
		, "calc_hash_keys"."customers_bk" AS "customers_bk"
		, "calc_hash_keys"."name" AS "name"
		, "calc_hash_keys"."birthdate" AS "birthdate"
		, "calc_hash_keys"."gender" AS "gender"
		, "calc_hash_keys"."party_type_code" AS "party_type_code"
		, "calc_hash_keys"."name_fk_parentpartynumber_bk" AS "name_fk_parentpartynumber_bk"
		, "calc_hash_keys"."birthdate_fk_parentpartynumber_bk" AS "birthdate_fk_parentpartynumber_bk"
		, "calc_hash_keys"."gender_fk_parentpartynumber_bk" AS "gender_fk_parentpartynumber_bk"
		, "calc_hash_keys"."party_type_code_fk_parentpartynumber_bk" AS "party_type_code_fk_parentpartynumber_bk"
		, "calc_hash_keys"."street_name_fk_addressnumber_bk" AS "street_name_fk_addressnumber_bk"
		, "calc_hash_keys"."street_number_fk_addressnumber_bk" AS "street_number_fk_addressnumber_bk"
		, "calc_hash_keys"."postal_code_fk_addressnumber_bk" AS "postal_code_fk_addressnumber_bk"
		, "calc_hash_keys"."city_fk_addressnumber_bk" AS "city_fk_addressnumber_bk"
		, "calc_hash_keys"."comments" AS "comments"
		, "calc_hash_keys"."update_timestamp" AS "update_timestamp"
		, "calc_hash_keys"."error_code_cust_cust_parentpartynumber" AS "error_code_cust_cust_parentpartynumber"
		, "calc_hash_keys"."error_code_cust_addr" AS "error_code_cust_addr"
	FROM "calc_hash_keys" "calc_hash_keys"
	;
END;


BEGIN -- ext_err_tgt

	TRUNCATE TABLE "moto_mktg_scn01_ext"."party_err"  CASCADE;

	INSERT INTO "moto_mktg_scn01_ext"."party_err"(
		 "load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"party_number"
		,"parent_party_number"
		,"address_number"
		,"name"
		,"birthdate"
		,"gender"
		,"party_type_code"
		,"name_bk"
		,"birthdate_bk"
		,"gender_bk"
		,"party_type_code_bk"
		,"comments"
		,"update_timestamp"
		,"error_code_cust_cust_parentpartynumber"
		,"error_code_cust_addr"
	)
	SELECT 
		  "stg_err_src"."load_date" AS "load_date"
		, "stg_err_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_err_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_err_src"."operation" AS "operation"
		, "stg_err_src"."record_type" AS "record_type"
		, "stg_err_src"."party_number" AS "party_number"
		, "stg_err_src"."parent_party_number" AS "parent_party_number"
		, "stg_err_src"."address_number" AS "address_number"
		, "stg_err_src"."name" AS "name"
		, "stg_err_src"."birthdate" AS "birthdate"
		, "stg_err_src"."gender" AS "gender"
		, "stg_err_src"."party_type_code" AS "party_type_code"
		, COALESCE(UPPER(REPLACE(TRIM( "stg_err_src"."name"),'#','\' || '#')),"err_mex_src"."key_attribute_varchar") AS "name_bk"
		, COALESCE(UPPER( TO_CHAR("stg_err_src"."birthdate", 'DD/MM/YYYY'::varchar)),"err_mex_src"."key_attribute_date") AS "birthdate_bk"
		, COALESCE(UPPER(REPLACE(TRIM( "stg_err_src"."gender"),'#','\' || '#')),"err_mex_src"."key_attribute_character") AS "gender_bk"
		, COALESCE(UPPER(REPLACE(TRIM( "stg_err_src"."party_type_code"),'#','\' || '#')),"err_mex_src"."key_attribute_character") AS "party_type_code_bk"
		, "stg_err_src"."comments" AS "comments"
		, "stg_err_src"."update_timestamp" AS "update_timestamp"
		, "stg_err_src"."error_code_cust_cust_parentpartynumber" AS "error_code_cust_cust_parentpartynumber"
		, "stg_err_src"."error_code_cust_addr" AS "error_code_cust_addr"
	FROM "moto_mktg_scn01_stg"."party" "stg_err_src"
	INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "err_mex_src" ON  1 = 1
	WHERE ( "stg_err_src"."error_code_cust_cust_parentpartynumber" IN(1,3) OR  "stg_err_src"."error_code_cust_addr" IN(1,3))AND "stg_err_src"."load_cycle_id" >= 0 AND "err_mex_src"."record_type" = 'N'
	;
END;



END;
$function$;
 
 
