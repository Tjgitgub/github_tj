CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_dl_mm_campaignmotorcycles_incr"() 
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

BEGIN -- stg_dl_tgt

	TRUNCATE TABLE "moto_mktg_scn01_stg"."campaign_motorcycles"  CASCADE;

	INSERT INTO "moto_mktg_scn01_stg"."campaign_motorcycles"(
		 "lnd_camp_prod_hkey"
		,"products_hkey"
		,"campaigns_hkey"
		,"load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"campaign_code"
		,"campaign_start_date"
		,"motorcycle_id"
		,"product_cc_fk_motorcycleid_bk"
		,"product_et_code_fk_motorcycleid_bk"
		,"product_part_code_fk_motorcycleid_bk"
		,"campaign_code_fk_campaignstartdate_bk"
		,"campaign_start_date_fk_campaignstartdate_bk"
		,"motorcycle_class_desc"
		,"motorcycle_subclass_desc"
		,"motorcycle_emotion_desc"
		,"motorcycle_comment"
		,"update_timestamp"
		,"error_code_camp_prod"
	)
	WITH "dist_fk1" AS 
	( 
		SELECT DISTINCT 
 			  "ext_dis_src1"."motorcycle_id" AS "motorcycle_id"
		FROM "moto_mktg_scn01_ext"."campaign_motorcycles" "ext_dis_src1"
	)
	, "prep_find_bk_fk1" AS 
	( 
		SELECT 
			  "hub_src1"."product_cc_bk" AS "product_cc_bk"
			, "hub_src1"."product_et_code_bk" AS "product_et_code_bk"
			, "hub_src1"."product_part_code_bk" AS "product_part_code_bk"
			, "dist_fk1"."motorcycle_id" AS "motorcycle_id"
			, "sat_src1"."load_date" AS "load_date"
			, 1 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_scn01_fl"."sat_mm_products" "sat_src1" ON  "dist_fk1"."motorcycle_id" = "sat_src1"."motorcycle_id"
		INNER JOIN "moto_scn01_fl"."hub_products" "hub_src1" ON  "hub_src1"."products_hkey" = "sat_src1"."products_hkey"
		WHERE  "sat_src1"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
		UNION ALL 
		SELECT 
			  "ext_fkbk_src1"."product_cc_bk" AS "product_cc_bk"
			, "ext_fkbk_src1"."product_et_code_bk" AS "product_et_code_bk"
			, "ext_fkbk_src1"."product_part_code_bk" AS "product_part_code_bk"
			, "dist_fk1"."motorcycle_id" AS "motorcycle_id"
			, "ext_fkbk_src1"."load_date" AS "load_date"
			, 0 AS "general_order"
		FROM "dist_fk1" "dist_fk1"
		INNER JOIN "moto_mktg_scn01_ext"."motorcycles" "ext_fkbk_src1" ON  "dist_fk1"."motorcycle_id" = "ext_fkbk_src1"."motorcycle_id"
	)
	, "order_bk_fk1" AS 
	( 
		SELECT 
			  "prep_find_bk_fk1"."product_cc_bk" AS "product_cc_bk"
			, "prep_find_bk_fk1"."product_et_code_bk" AS "product_et_code_bk"
			, "prep_find_bk_fk1"."product_part_code_bk" AS "product_part_code_bk"
			, "prep_find_bk_fk1"."motorcycle_id" AS "motorcycle_id"
			, ROW_NUMBER()OVER(PARTITION BY "prep_find_bk_fk1"."motorcycle_id" ORDER BY "prep_find_bk_fk1"."general_order",
				"prep_find_bk_fk1"."load_date" DESC) AS "dummy"
		FROM "prep_find_bk_fk1" "prep_find_bk_fk1"
	)
	, "find_bk_fk1" AS 
	( 
		SELECT 
			  "order_bk_fk1"."product_cc_bk" AS "product_cc_bk"
			, "order_bk_fk1"."product_et_code_bk" AS "product_et_code_bk"
			, "order_bk_fk1"."product_part_code_bk" AS "product_part_code_bk"
			, "order_bk_fk1"."motorcycle_id" AS "motorcycle_id"
		FROM "order_bk_fk1" "order_bk_fk1"
		WHERE  "order_bk_fk1"."dummy" = 1
	)
	, "calc_hash_keys" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST(  'moto_mktg_scn01' || '#' || COALESCE("find_bk_fk1"."product_cc_bk","mex_src"."key_attribute_numeric")
				||'#' ||  COALESCE("find_bk_fk1"."product_et_code_bk","mex_src"."key_attribute_character")|| '#' ||  COALESCE("find_bk_fk1"."product_part_code_bk","mex_src"."key_attribute_varchar")|| '#' || "ext_src"."campaign_code_fk_campaignstartdate_bk" || '#' ||  "ext_src"."campaign_start_date_fk_campaignstartdate_bk" || '#'  ,'MD5'),'HEX')) AS "lnd_camp_prod_hkey"
			, UPPER(ENCODE(DIGEST( 'moto_mktg_scn01' || '#' || COALESCE("find_bk_fk1"."product_cc_bk","mex_src"."key_attribute_numeric")
				||'#' ||  COALESCE("find_bk_fk1"."product_et_code_bk","mex_src"."key_attribute_character")|| '#' ||  COALESCE("find_bk_fk1"."product_part_code_bk","mex_src"."key_attribute_varchar")|| '#' ,'MD5'),'HEX')) AS "products_hkey"
			, UPPER(ENCODE(DIGEST( "ext_src"."campaign_code_fk_campaignstartdate_bk" || '#' ||  "ext_src"."campaign_start_date_fk_campaignstartdate_bk" || 
				'#' ,'MD5'),'HEX')) AS "campaigns_hkey"
			, "ext_src"."load_date" AS "load_date"
			, "ext_src"."load_cycle_id" AS "load_cycle_id"
			, "ext_src"."trans_timestamp" AS "trans_timestamp"
			, "ext_src"."operation" AS "operation"
			, "ext_src"."record_type" AS "record_type"
			, "ext_src"."campaign_code" AS "campaign_code"
			, "ext_src"."campaign_start_date" AS "campaign_start_date"
			, "ext_src"."motorcycle_id" AS "motorcycle_id"
			, NULL::text AS "product_cc_fk_motorcycleid_bk"
			, NULL::text AS "product_et_code_fk_motorcycleid_bk"
			, NULL::text AS "product_part_code_fk_motorcycleid_bk"
			, "ext_src"."campaign_code_fk_campaignstartdate_bk" AS "campaign_code_fk_campaignstartdate_bk"
			, "ext_src"."campaign_start_date_fk_campaignstartdate_bk" AS "campaign_start_date_fk_campaignstartdate_bk"
			, "ext_src"."motorcycle_class_desc" AS "motorcycle_class_desc"
			, "ext_src"."motorcycle_subclass_desc" AS "motorcycle_subclass_desc"
			, "ext_src"."motorcycle_emotion_desc" AS "motorcycle_emotion_desc"
			, "ext_src"."motorcycle_comment" AS "motorcycle_comment"
			, "ext_src"."update_timestamp" AS "update_timestamp"
			, CASE WHEN "ext_src"."error_code_camp_prod" = 2 THEN 2 WHEN "ext_src"."error_code_camp_prod" =
				- 1 THEN 0 WHEN"find_bk_fk1"."motorcycle_id" IS NULL  THEN "ext_src"."error_code_camp_prod" ELSE 0 END AS "error_code_camp_prod"
		FROM "moto_mktg_scn01_ext"."campaign_motorcycles" "ext_src"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
		LEFT OUTER JOIN "find_bk_fk1" "find_bk_fk1" ON  "ext_src"."motorcycle_id" = "find_bk_fk1"."motorcycle_id"
	)
	SELECT 
		  "calc_hash_keys"."lnd_camp_prod_hkey" AS "lnd_camp_prod_hkey"
		, "calc_hash_keys"."products_hkey" AS "products_hkey"
		, "calc_hash_keys"."campaigns_hkey" AS "campaigns_hkey"
		, CASE WHEN "calc_hash_keys"."record_type" = 'E' THEN "calc_hash_keys"."load_date" ELSE CURRENT_TIMESTAMP +
			row_number() over (PARTITION BY  "calc_hash_keys"."products_hkey" ,  "calc_hash_keys"."campaigns_hkey"  ORDER BY  "calc_hash_keys"."trans_timestamp") * interval'2 microsecond'   END AS "load_date"
		, "calc_hash_keys"."load_cycle_id" AS "load_cycle_id"
		, "calc_hash_keys"."trans_timestamp" AS "trans_timestamp"
		, "calc_hash_keys"."operation" AS "operation"
		, "calc_hash_keys"."record_type" AS "record_type"
		, "calc_hash_keys"."campaign_code" AS "campaign_code"
		, "calc_hash_keys"."campaign_start_date" AS "campaign_start_date"
		, "calc_hash_keys"."motorcycle_id" AS "motorcycle_id"
		, "calc_hash_keys"."product_cc_fk_motorcycleid_bk" AS "product_cc_fk_motorcycleid_bk"
		, "calc_hash_keys"."product_et_code_fk_motorcycleid_bk" AS "product_et_code_fk_motorcycleid_bk"
		, "calc_hash_keys"."product_part_code_fk_motorcycleid_bk" AS "product_part_code_fk_motorcycleid_bk"
		, "calc_hash_keys"."campaign_code_fk_campaignstartdate_bk" AS "campaign_code_fk_campaignstartdate_bk"
		, "calc_hash_keys"."campaign_start_date_fk_campaignstartdate_bk" AS "campaign_start_date_fk_campaignstartdate_bk"
		, "calc_hash_keys"."motorcycle_class_desc" AS "motorcycle_class_desc"
		, "calc_hash_keys"."motorcycle_subclass_desc" AS "motorcycle_subclass_desc"
		, "calc_hash_keys"."motorcycle_emotion_desc" AS "motorcycle_emotion_desc"
		, "calc_hash_keys"."motorcycle_comment" AS "motorcycle_comment"
		, "calc_hash_keys"."update_timestamp" AS "update_timestamp"
		, "calc_hash_keys"."error_code_camp_prod" AS "error_code_camp_prod"
	FROM "calc_hash_keys" "calc_hash_keys"
	;
END;


BEGIN -- ext_err_tgt

	TRUNCATE TABLE "moto_mktg_scn01_ext"."campaign_motorcycles_err"  CASCADE;

	INSERT INTO "moto_mktg_scn01_ext"."campaign_motorcycles_err"(
		 "load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"campaign_code"
		,"campaign_start_date"
		,"motorcycle_id"
		,"campaign_code_fk_campaignstartdate_bk"
		,"campaign_start_date_fk_campaignstartdate_bk"
		,"motorcycle_class_desc"
		,"motorcycle_subclass_desc"
		,"motorcycle_emotion_desc"
		,"motorcycle_comment"
		,"update_timestamp"
		,"error_code_camp_prod"
	)
	SELECT 
		  "stg_err_src"."load_date" AS "load_date"
		, "stg_err_src"."load_cycle_id" AS "load_cycle_id"
		, "stg_err_src"."trans_timestamp" AS "trans_timestamp"
		, "stg_err_src"."operation" AS "operation"
		, "stg_err_src"."record_type" AS "record_type"
		, "stg_err_src"."campaign_code" AS "campaign_code"
		, "stg_err_src"."campaign_start_date" AS "campaign_start_date"
		, "stg_err_src"."motorcycle_id" AS "motorcycle_id"
		, "stg_err_src"."campaign_code_fk_campaignstartdate_bk" AS "campaign_code_fk_campaignstartdate_bk"
		, "stg_err_src"."campaign_start_date_fk_campaignstartdate_bk" AS "campaign_start_date_fk_campaignstartdate_bk"
		, "stg_err_src"."motorcycle_class_desc" AS "motorcycle_class_desc"
		, "stg_err_src"."motorcycle_subclass_desc" AS "motorcycle_subclass_desc"
		, "stg_err_src"."motorcycle_emotion_desc" AS "motorcycle_emotion_desc"
		, "stg_err_src"."motorcycle_comment" AS "motorcycle_comment"
		, "stg_err_src"."update_timestamp" AS "update_timestamp"
		, "stg_err_src"."error_code_camp_prod" AS "error_code_camp_prod"
	FROM "moto_mktg_scn01_stg"."campaign_motorcycles" "stg_err_src"
	WHERE  "stg_err_src"."error_code_camp_prod" IN(1,3)AND "stg_err_src"."load_cycle_id" >= 0
	;
END;



END;
$function$;
 
 
