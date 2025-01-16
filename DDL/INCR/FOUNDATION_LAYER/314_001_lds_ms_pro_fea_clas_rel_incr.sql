CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lds_ms_pro_fea_clas_rel_incr"() 
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

Vaultspeed version: 5.7.2.16, generation date: 2025/01/16 15:00:22
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/16 14:56:23, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08
 */


BEGIN 

BEGIN -- lds_temp_tgt

	TRUNCATE TABLE "moto_sales_scn01_stg"."lds_ms_pro_fea_clas_rel_tmp"  CASCADE;

	INSERT INTO "moto_sales_scn01_stg"."lds_ms_pro_fea_clas_rel_tmp"(
		 "lnd_pro_fea_clas_rel_hkey"
		,"products_hkey"
		,"prod_featu_class_hkey"
		,"product_features_hkey"
		,"product_id"
		,"product_feature_class_id"
		,"product_feature_id"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"record_type"
		,"source"
		,"equal"
		,"delete_flag"
		,"trans_timestamp"
		,"update_timestamp"
	)
	WITH "dist_stg" AS 
	( 
		SELECT 
			  "stg_dis_src"."products_hkey" AS "products_hkey"
			, "stg_dis_src"."product_features_hkey" AS "product_features_hkey"
			, "stg_dis_src"."load_cycle_id" AS "load_cycle_id"
			, MIN("stg_dis_src"."load_date") AS "min_load_timestamp"
			, MIN("stg_dis_src"."error_code_pro_fea_clas_rel") AS "error_code_pro_fea_clas_rel"
		FROM "moto_sales_scn01_stg"."product_feat_class_rel" "stg_dis_src"
		GROUP BY  "stg_dis_src"."products_hkey",  "stg_dis_src"."product_features_hkey",  "stg_dis_src"."load_cycle_id"
	)
	, "last_lds" AS 
	( 
		SELECT 
			  "last_lnd_src"."products_hkey" AS "products_hkey"
			, "last_lnd_src"."product_features_hkey" AS "product_features_hkey"
			, "dist_stg"."load_cycle_id" AS "load_cycle_id"
			, MAX("last_lds_src"."load_date") AS "max_load_timestamp"
			, MIN("dist_stg"."error_code_pro_fea_clas_rel") AS "error_code_pro_fea_clas_rel"
		FROM "moto_scn01_fl"."lds_ms_pro_fea_clas_rel" "last_lds_src"
		INNER JOIN "moto_scn01_fl"."lnd_pro_fea_clas_rel" "last_lnd_src" ON  "last_lds_src"."lnd_pro_fea_clas_rel_hkey" = "last_lnd_src"."lnd_pro_fea_clas_rel_hkey"
		INNER JOIN "dist_stg" "dist_stg" ON  "last_lnd_src"."products_hkey" = "dist_stg"."products_hkey" AND  "last_lnd_src"."product_features_hkey" = 
			"dist_stg"."product_features_hkey"
		WHERE  "last_lds_src"."load_date" <= "dist_stg"."min_load_timestamp"
		GROUP BY  "last_lnd_src"."products_hkey",  "last_lnd_src"."product_features_hkey",  "dist_stg"."load_cycle_id"
	)
	, "temp_table_set" AS 
	( 
		SELECT 
			  "stg_temp_src"."lnd_pro_fea_clas_rel_hkey" AS "lnd_pro_fea_clas_rel_hkey"
			, "stg_temp_src"."products_hkey" AS "products_hkey"
			, "stg_temp_src"."prod_featu_class_hkey" AS "prod_featu_class_hkey"
			, "stg_temp_src"."product_features_hkey" AS "product_features_hkey"
			, "stg_temp_src"."product_id" AS "product_id"
			, "stg_temp_src"."product_feature_class_id" AS "product_feature_class_id"
			, "stg_temp_src"."product_feature_id" AS "product_feature_id"
			, "stg_temp_src"."load_date" AS "load_date"
			, "stg_temp_src"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, "stg_temp_src"."record_type" AS "record_type"
			, 'STG' AS "source"
			, 1 AS "origin_id"
			, CASE WHEN "stg_temp_src"."operation" = 'D' THEN 'Y'::text ELSE 'N'::text END AS "delete_flag"
			, "stg_temp_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_temp_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_sales_scn01_stg"."product_feat_class_rel" "stg_temp_src"
		WHERE  "stg_temp_src"."error_code_pro_fea_clas_rel" in(0,1)
		UNION ALL 
		SELECT 
			  "lds_src"."lnd_pro_fea_clas_rel_hkey" AS "lnd_pro_fea_clas_rel_hkey"
			, "lnd_src"."products_hkey" AS "products_hkey"
			, "lnd_src"."prod_featu_class_hkey" AS "prod_featu_class_hkey"
			, "lnd_src"."product_features_hkey" AS "product_features_hkey"
			, "lds_src"."product_id" AS "product_id"
			, "lds_src"."product_feature_class_id" AS "product_feature_class_id"
			, "lds_src"."product_feature_id" AS "product_feature_id"
			, "lds_src"."load_date" AS "load_date"
			, "lds_src"."load_cycle_id" AS "load_cycle_id"
			, "lds_src"."load_end_date" AS "load_end_date"
			, 'SAT' AS "record_type"
			, 'LDS' AS "source"
			, 0 AS "origin_id"
			, "lds_src"."delete_flag" AS "delete_flag"
			, "lds_src"."trans_timestamp" AS "trans_timestamp"
			, "lds_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_scn01_fl"."lds_ms_pro_fea_clas_rel" "lds_src"
		INNER JOIN "moto_scn01_fl"."lnd_pro_fea_clas_rel" "lnd_src" ON  "lds_src"."lnd_pro_fea_clas_rel_hkey" = "lnd_src"."lnd_pro_fea_clas_rel_hkey"
		INNER JOIN "last_lds" "last_lds" ON  "lnd_src"."products_hkey" = "last_lds"."products_hkey" AND  "lnd_src"."product_features_hkey" = "last_lds"."product_features_hkey"
		WHERE  "lds_src"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) OR("last_lds"."error_code_pro_fea_clas_rel" = 0 AND "lds_src"."load_date" >= "last_lds"."max_load_timestamp")
	)
	SELECT 
		  "temp_table_set"."lnd_pro_fea_clas_rel_hkey" AS "lnd_pro_fea_clas_rel_hkey"
		, "temp_table_set"."products_hkey" AS "products_hkey"
		, "temp_table_set"."prod_featu_class_hkey" AS "prod_featu_class_hkey"
		, "temp_table_set"."product_features_hkey" AS "product_features_hkey"
		, "temp_table_set"."product_id" AS "product_id"
		, "temp_table_set"."product_feature_class_id" AS "product_feature_class_id"
		, "temp_table_set"."product_feature_id" AS "product_feature_id"
		, "temp_table_set"."load_date" AS "load_date"
		, "temp_table_set"."load_cycle_id" AS "load_cycle_id"
		, "temp_table_set"."load_end_date" AS "load_end_date"
		, "temp_table_set"."record_type" AS "record_type"
		, "temp_table_set"."source" AS "source"
		, CASE WHEN "temp_table_set"."source" = 'STG' AND "temp_table_set"."delete_flag"::text || "temp_table_set"."prod_featu_class_hkey"::text =
			LAG("temp_table_set"."delete_flag"::text || "temp_table_set"."prod_featu_class_hkey"::text,1)OVER(PARTITION BY "temp_table_set"."products_hkey" ,  "temp_table_set"."product_features_hkey" ORDER BY "temp_table_set"."load_date","temp_table_set"."origin_id", "temp_table_set"."prod_featu_class_hkey")THEN 1 ELSE 0 END AS "equal"
		, "temp_table_set"."delete_flag" AS "delete_flag"
		, "temp_table_set"."trans_timestamp" AS "trans_timestamp"
		, "temp_table_set"."update_timestamp" AS "update_timestamp"
	FROM "temp_table_set" "temp_table_set"
	;
END;


BEGIN -- lds_inur_tgt

	INSERT INTO "moto_scn01_fl"."lds_ms_pro_fea_clas_rel"(
		 "lnd_pro_fea_clas_rel_hkey"
		,"products_hkey"
		,"prod_featu_class_hkey"
		,"product_features_hkey"
		,"product_id"
		,"product_feature_class_id"
		,"product_feature_id"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"delete_flag"
		,"trans_timestamp"
		,"update_timestamp"
	)
	SELECT 
		  "lds_temp_src_inur"."lnd_pro_fea_clas_rel_hkey" AS "lnd_pro_fea_clas_rel_hkey"
		, "lds_temp_src_inur"."products_hkey" AS "products_hkey"
		, "lds_temp_src_inur"."prod_featu_class_hkey" AS "prod_featu_class_hkey"
		, "lds_temp_src_inur"."product_features_hkey" AS "product_features_hkey"
		, "lds_temp_src_inur"."product_id" AS "product_id"
		, "lds_temp_src_inur"."product_feature_class_id" AS "product_feature_class_id"
		, "lds_temp_src_inur"."product_feature_id" AS "product_feature_id"
		, "lds_temp_src_inur"."load_date" AS "load_date"
		, "lds_temp_src_inur"."load_cycle_id" AS "load_cycle_id"
		, "lds_temp_src_inur"."load_end_date" AS "load_end_date"
		, "lds_temp_src_inur"."delete_flag" AS "delete_flag"
		, "lds_temp_src_inur"."trans_timestamp" AS "trans_timestamp"
		, "lds_temp_src_inur"."update_timestamp" AS "update_timestamp"
	FROM "moto_sales_scn01_stg"."lds_ms_pro_fea_clas_rel_tmp" "lds_temp_src_inur"
	WHERE  "lds_temp_src_inur"."source" = 'STG' AND "lds_temp_src_inur"."equal" = 0
	;
END;


BEGIN -- lds_ed_tgt

	WITH "calc_load_end_date" AS 
	( 
		SELECT 
			  "lds_temp_src_us"."lnd_pro_fea_clas_rel_hkey" AS "lnd_pro_fea_clas_rel_hkey"
			, "lds_temp_src_us"."load_date" AS "load_date"
			, CASE WHEN "lds_temp_src_us"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 
				'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN "lci_src"."load_cycle_id" ELSE "lds_temp_src_us"."load_cycle_id" END AS "load_cycle_id"
			, COALESCE(LEAD("lds_temp_src_us"."load_date",1)OVER(PARTITION BY "lds_temp_src_us"."products_hkey" ,  
				"lds_temp_src_us"."product_features_hkey" ORDER BY "lds_temp_src_us"."load_date","lds_temp_src_us"."load_cycle_id"), TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
		FROM "moto_sales_scn01_stg"."lds_ms_pro_fea_clas_rel_tmp" "lds_temp_src_us"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		WHERE  "lds_temp_src_us"."equal" = 0
	)
	, "filter_load_end_date" AS 
	( 
		SELECT 
			  "calc_load_end_date"."lnd_pro_fea_clas_rel_hkey" AS "lnd_pro_fea_clas_rel_hkey"
			, "calc_load_end_date"."load_date" AS "load_date"
			, "calc_load_end_date"."load_cycle_id" AS "load_cycle_id"
			, "calc_load_end_date"."load_end_date" AS "load_end_date"
		FROM "calc_load_end_date" "calc_load_end_date"
		WHERE  "calc_load_end_date"."load_end_date" != TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
	)
	UPDATE "moto_scn01_fl"."lds_ms_pro_fea_clas_rel" "lds_ed_tgt"
	SET 
		 "load_cycle_id" =  "filter_load_end_date"."load_cycle_id"
		,"load_end_date" =  "filter_load_end_date"."load_end_date"
	FROM  "filter_load_end_date"
	WHERE "lds_ed_tgt"."lnd_pro_fea_clas_rel_hkey" =  "filter_load_end_date"."lnd_pro_fea_clas_rel_hkey"
	  AND "lds_ed_tgt"."load_date" =  "filter_load_end_date"."load_date"
	;
END;



END;
$function$;
 
 
