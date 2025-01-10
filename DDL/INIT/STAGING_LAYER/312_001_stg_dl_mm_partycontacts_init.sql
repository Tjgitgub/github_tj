CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_dl_mm_partycontacts_init"() 
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

BEGIN -- stg_dl_tgt

	TRUNCATE TABLE "moto_mktg_scn01_stg"."party_contacts"  CASCADE;

	INSERT INTO "moto_mktg_scn01_stg"."party_contacts"(
		 "lnd_cust_cont_hkey"
		,"customers_hkey"
		,"contacts_hkey"
		,"load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"party_number"
		,"contact_id"
		,"name_fk_partynumber_bk"
		,"birthdate_fk_partynumber_bk"
		,"gender_fk_partynumber_bk"
		,"party_type_code_fk_partynumber_bk"
		,"contact_id_fk_contactid_bk"
		,"update_timestamp"
		,"error_code_cust_cont"
	)
	WITH "dist_fk1" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src1"."party_number" AS "party_number"
		FROM "moto_mktg_scn01_ext"."party_contacts" "ext_dis_src1"
	)
	, "prep_find_bk_fk1" AS 
	( 
		SELECT 
			  UPPER(REPLACE(TRIM( "sat_src1"."name"),'#','\' || '#')) AS "name_bk"
			, UPPER( TO_CHAR("sat_src1"."birthdate", 'DD/MM/YYYY'::varchar)) AS "birthdate_bk"
			, UPPER(REPLACE(TRIM( "sat_src1"."gender"),'#','\' || '#')) AS "gender_bk"
			, UPPER(REPLACE(TRIM( "sat_src1"."party_type_code"),'#','\' || '#')) AS "party_type_code_bk"
			, "dist_fk1"."party_number" AS "party_number"
			, "sat_src1"."load_date" AS "load_date"
			, 1 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_scn01_fl"."sat_mm_customers" "sat_src1" ON  "dist_fk1"."party_number" = "sat_src1"."party_number"
		INNER JOIN "moto_scn01_fl"."hub_customers" "hub_src1" ON  "hub_src1"."customers_hkey" = "sat_src1"."customers_hkey"
		WHERE  "sat_src1"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
		UNION ALL 
		SELECT 
			  "ext_fkbk_src1"."name_bk" AS "name_bk"
			, "ext_fkbk_src1"."birthdate_bk" AS "birthdate_bk"
			, "ext_fkbk_src1"."gender_bk" AS "gender_bk"
			, "ext_fkbk_src1"."party_type_code_bk" AS "party_type_code_bk"
			, "dist_fk1"."party_number" AS "party_number"
			, "ext_fkbk_src1"."load_date" AS "load_date"
			, 0 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_mktg_scn01_ext"."party" "ext_fkbk_src1" ON  "dist_fk1"."party_number" = "ext_fkbk_src1"."party_number"
	)
	, "order_bk_fk1" AS 
	( 
		SELECT 
			  "prep_find_bk_fk1"."name_bk" AS "name_bk"
			, "prep_find_bk_fk1"."birthdate_bk" AS "birthdate_bk"
			, "prep_find_bk_fk1"."gender_bk" AS "gender_bk"
			, "prep_find_bk_fk1"."party_type_code_bk" AS "party_type_code_bk"
			, "prep_find_bk_fk1"."party_number" AS "party_number"
			, ROW_NUMBER()OVER(PARTITION BY "prep_find_bk_fk1"."party_number" ORDER BY "prep_find_bk_fk1"."general_order",
				"prep_find_bk_fk1"."load_date" DESC) AS "dummy"
		FROM "prep_find_bk_fk1" "prep_find_bk_fk1"
	)
	, "find_bk_fk1" AS 
	( 
		SELECT 
			  "order_bk_fk1"."name_bk" AS "name_bk"
			, "order_bk_fk1"."birthdate_bk" AS "birthdate_bk"
			, "order_bk_fk1"."gender_bk" AS "gender_bk"
			, "order_bk_fk1"."party_type_code_bk" AS "party_type_code_bk"
			, "order_bk_fk1"."party_number" AS "party_number"
		FROM "order_bk_fk1" "order_bk_fk1"
		WHERE  "order_bk_fk1"."dummy" = 1
	)
	SELECT 
		  UPPER(ENCODE(DIGEST(  'moto_mktg_scn01' || '#' || COALESCE("find_bk_fk1"."name_bk","mex_src"."key_attribute_varchar")
			|| '#' ||  COALESCE("find_bk_fk1"."birthdate_bk","mex_src"."key_attribute_date")|| '#' ||  COALESCE("find_bk_fk1"."gender_bk","mex_src"."key_attribute_character")|| '#' ||  COALESCE("find_bk_fk1"."party_type_code_bk","mex_src"."key_attribute_character")|| '#' || "ext_src"."contact_id_fk_contactid_bk" || '#'  ,'MD5'),'HEX')) AS "lnd_cust_cont_hkey"
		, UPPER(ENCODE(DIGEST( 'moto_mktg_scn01' || '#' || COALESCE("find_bk_fk1"."name_bk","mex_src"."key_attribute_varchar")
			|| '#' ||  COALESCE("find_bk_fk1"."birthdate_bk","mex_src"."key_attribute_date")|| '#' ||  COALESCE("find_bk_fk1"."gender_bk","mex_src"."key_attribute_character")|| '#' ||  COALESCE("find_bk_fk1"."party_type_code_bk","mex_src"."key_attribute_character")|| '#' ,'MD5'),'HEX')) AS "customers_hkey"
		, UPPER(ENCODE(DIGEST( "ext_src"."contact_id_fk_contactid_bk" || '#' ,'MD5'),'HEX')) AS "contacts_hkey"
		, "ext_src"."load_date" AS "load_date"
		, "ext_src"."load_cycle_id" AS "load_cycle_id"
		, "ext_src"."trans_timestamp" AS "trans_timestamp"
		, "ext_src"."operation" AS "operation"
		, "ext_src"."record_type" AS "record_type"
		, "ext_src"."party_number" AS "party_number"
		, "ext_src"."contact_id" AS "contact_id"
		, NULL::text AS "name_fk_partynumber_bk"
		, NULL::text AS "birthdate_fk_partynumber_bk"
		, NULL::text AS "gender_fk_partynumber_bk"
		, NULL::text AS "party_type_code_fk_partynumber_bk"
		, "ext_src"."contact_id_fk_contactid_bk" AS "contact_id_fk_contactid_bk"
		, "ext_src"."update_timestamp" AS "update_timestamp"
		, CASE WHEN  "find_bk_fk1"."party_number" IS NULL  THEN 1 ELSE 0 END AS "error_code_cust_cont"
	FROM "moto_mktg_scn01_ext"."party_contacts" "ext_src"
	INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
	LEFT OUTER JOIN "find_bk_fk1" "find_bk_fk1" ON  "ext_src"."party_number" = "find_bk_fk1"."party_number"
	;
END;


BEGIN -- ext_err_tgt

	TRUNCATE TABLE "moto_mktg_scn01_ext"."party_contacts_err"  CASCADE;

	INSERT INTO "moto_mktg_scn01_ext"."party_contacts_err"(
		 "load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"party_number"
		,"contact_id"
		,"contact_id_fk_contactid_bk"
		,"update_timestamp"
		,"error_code_cust_cont"
	)
	SELECT 
		  "stg_err_src"."load_date" AS "load_date"
		, "stg_err_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_err_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_err_src"."operation" AS "operation"
		, "stg_err_src"."record_type" AS "record_type"
		, "stg_err_src"."party_number" AS "party_number"
		, "stg_err_src"."contact_id" AS "contact_id"
		, "stg_err_src"."contact_id_fk_contactid_bk" AS "contact_id_fk_contactid_bk"
		, "stg_err_src"."update_timestamp" AS "update_timestamp"
		, "stg_err_src"."error_code_cust_cont" AS "error_code_cust_cont"
	FROM "moto_mktg_scn01_stg"."party_contacts" "stg_err_src"
	WHERE ( "stg_err_src"."error_code_cust_cont" > 0)AND "stg_err_src"."load_cycle_id" >= 0
	;
END;



END;
$function$;
 
 
