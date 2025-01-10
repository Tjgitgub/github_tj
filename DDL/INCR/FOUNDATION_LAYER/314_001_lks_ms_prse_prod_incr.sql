CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lks_ms_prse_prod_incr"() 
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

BEGIN -- lks_temp_tgt

	TRUNCATE TABLE "moto_sales_scn01_stg"."lks_ms_prse_prod_tmp"  CASCADE;

	INSERT INTO "moto_sales_scn01_stg"."lks_ms_prse_prod_tmp"(
		 "lnk_prse_prod_hkey"
		,"products_hkey"
		,"product_sensors_hkey"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"record_type"
		,"source"
		,"equal"
		,"delete_flag"
		,"vehicle_number"
		,"product_number"
	)
	WITH "dist_stg" AS 
	( 
		SELECT 
			  "stg_dis_src"."product_sensors_hkey" AS "product_sensors_hkey"
			, "stg_dis_src"."load_cycle_id" AS "load_cycle_id"
			, MIN("stg_dis_src"."load_date") AS "min_load_timestamp"
		FROM "moto_sales_scn01_stg"."product_sensors" "stg_dis_src"
		GROUP BY  "stg_dis_src"."product_sensors_hkey",  "stg_dis_src"."load_cycle_id"
	)
	, "dist_del" AS 
	( 
		SELECT DISTINCT 
 			  "stg_del_src"."product_sensors_hkey" AS "product_sensors_hkey"
		FROM "moto_sales_scn01_stg"."product_sensors" "stg_del_src"
		WHERE  "stg_del_src"."operation" = 'D'
	)
	, "all_rel_del" AS 
	( 
		SELECT 
			  "stg_rd_src"."product_sensors_hkey" AS "product_sensors_hkey"
			, "stg_rd_src"."subsequence_seq" AS "subsequence_seq"
			, "stg_rd_src"."load_date" AS "load_date"
			, CASE WHEN "stg_rd_src"."operation" = 'D' THEN 'Y'::text ELSE 'N'::text END AS "delete_flag"
		FROM "moto_sales_scn01_stg"."product_sensors" "stg_rd_src"
		INNER JOIN "dist_del" "dist_del" ON  "stg_rd_src"."product_sensors_hkey" = "dist_del"."product_sensors_hkey"
		UNION 
		SELECT 
			  "del_sat"."product_sensors_hkey" AS "product_sensors_hkey"
			, "del_sat"."subsequence_seq" AS "subsequence_seq"
			, "del_sat"."load_date" AS "load_date"
			, "del_sat"."delete_flag" AS "delete_flag"
		FROM "moto_scn01_fl"."sat_ms_product_sensors" "del_sat"
		INNER JOIN "dist_del" "dist_del" ON  "del_sat"."product_sensors_hkey" = "dist_del"."product_sensors_hkey"
		WHERE  "del_sat"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AND "del_sat"."delete_flag" = 'N'::text
	)
	, "calc_interval" AS 
	( 
		SELECT 
			  "all_rel_del"."product_sensors_hkey" AS "product_sensors_hkey"
			, "all_rel_del"."load_date" AS "load_date"
			, "all_rel_del"."delete_flag" AS "delete_flag"
			, COALESCE(LEAD("all_rel_del"."load_date")OVER(PARTITION BY "all_rel_del"."product_sensors_hkey", 
				"all_rel_del"."subsequence_seq" ORDER BY "all_rel_del"."load_date"), TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "end_interval"
		FROM "all_rel_del" "all_rel_del"
	)
	, "del_keys" AS 
	( 
		SELECT 
			  "all_rel_del"."product_sensors_hkey" AS "product_sensors_hkey"
			, "all_rel_del"."load_date" AS "load_date"
		FROM "calc_interval" "calc_interval"
		RIGHT OUTER JOIN "all_rel_del" "all_rel_del" ON  "calc_interval"."product_sensors_hkey" = "all_rel_del"."product_sensors_hkey" AND "all_rel_del"."load_date" >=
			 "calc_interval"."load_date" AND "all_rel_del"."load_date" < "calc_interval"."end_interval" AND "calc_interval"."delete_flag" = 'N'::text
		WHERE  "calc_interval"."product_sensors_hkey" IS NULL AND "all_rel_del"."delete_flag" = 'Y'::text
	)
	, "temp_table_set" AS 
	( 
		SELECT 
			  "stg_temp_src"."lnk_prse_prod_hkey" AS "lnk_prse_prod_hkey"
			, "stg_temp_src"."products_hkey" AS "products_hkey"
			, "stg_temp_src"."product_sensors_hkey" AS "product_sensors_hkey"
			, "stg_temp_src"."load_date" AS "load_date"
			, "stg_temp_src"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, "stg_temp_src"."record_type" AS "record_type"
			, 'STG' AS "source"
			, 1 AS "origin_id"
			, CASE WHEN "stg_temp_src"."operation" = 'D' AND "del_keys"."product_sensors_hkey" IS NOT NULL THEN 'Y'::text ELSE 'N'::text END AS "delete_flag"
			, "stg_temp_src"."vehicle_number" AS "vehicle_number"
			, "stg_temp_src"."product_number" AS "product_number"
		FROM "moto_sales_scn01_stg"."product_sensors" "stg_temp_src"
		LEFT OUTER JOIN "del_keys" "del_keys" ON  "stg_temp_src"."product_sensors_hkey" = "del_keys"."product_sensors_hkey" AND "stg_temp_src"."load_date" = 
			"del_keys"."load_date"
		UNION 
		SELECT 
			  "lks_src"."lnk_prse_prod_hkey" AS "lnk_prse_prod_hkey"
			, "lnk_src"."products_hkey" AS "products_hkey"
			, "lnk_src"."product_sensors_hkey" AS "product_sensors_hkey"
			, "lks_src"."load_date" AS "load_date"
			, "lks_src"."load_cycle_id" AS "load_cycle_id"
			, "lks_src"."load_end_date" AS "load_end_date"
			, 'SAT' AS "record_type"
			, 'LKS' AS "source"
			, 0 AS "origin_id"
			, "lks_src"."delete_flag" AS "delete_flag"
			, "lks_src"."vehicle_number" AS "vehicle_number"
			, "lks_src"."product_number" AS "product_number"
		FROM "moto_scn01_fl"."lks_ms_prse_prod" "lks_src"
		INNER JOIN "moto_scn01_fl"."lnk_prse_prod" "lnk_src" ON  "lks_src"."lnk_prse_prod_hkey" = "lnk_src"."lnk_prse_prod_hkey"
		INNER JOIN "dist_stg" "dist_stg" ON  "lnk_src"."product_sensors_hkey" = "dist_stg"."product_sensors_hkey"
		WHERE  "lks_src"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
	)
	SELECT 
		  "temp_table_set"."lnk_prse_prod_hkey" AS "lnk_prse_prod_hkey"
		, "temp_table_set"."products_hkey" AS "products_hkey"
		, "temp_table_set"."product_sensors_hkey" AS "product_sensors_hkey"
		, "temp_table_set"."load_date" AS "load_date"
		, "temp_table_set"."load_cycle_id" AS "load_cycle_id"
		, "temp_table_set"."load_end_date" AS "load_end_date"
		, "temp_table_set"."record_type" AS "record_type"
		, "temp_table_set"."source" AS "source"
		, CASE WHEN "temp_table_set"."source" = 'STG' AND "temp_table_set"."delete_flag"::text || "temp_table_set"."products_hkey"::text =
			LAG( "temp_table_set"."delete_flag"::text || "temp_table_set"."products_hkey"::text,1)OVER(PARTITION BY "temp_table_set"."product_sensors_hkey" ORDER BY "temp_table_set"."load_date","temp_table_set"."origin_id")THEN 1 ELSE 0 END AS "equal"
		, "temp_table_set"."delete_flag" AS "delete_flag"
		, "temp_table_set"."vehicle_number" AS "vehicle_number"
		, "temp_table_set"."product_number" AS "product_number"
	FROM "temp_table_set" "temp_table_set"
	;
END;


BEGIN -- lks_inur_tgt

	INSERT INTO "moto_scn01_fl"."lks_ms_prse_prod"(
		 "lnk_prse_prod_hkey"
		,"products_hkey"
		,"product_sensors_hkey"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"delete_flag"
		,"vehicle_number"
		,"product_number"
	)
	SELECT 
		  "lks_temp_src_inur"."lnk_prse_prod_hkey" AS "lnk_prse_prod_hkey"
		, "lks_temp_src_inur"."products_hkey" AS "products_hkey"
		, "lks_temp_src_inur"."product_sensors_hkey" AS "product_sensors_hkey"
		, "lks_temp_src_inur"."load_date" AS "load_date"
		, "lks_temp_src_inur"."load_cycle_id" AS "load_cycle_id"
		, "lks_temp_src_inur"."load_end_date" AS "load_end_date"
		, "lks_temp_src_inur"."delete_flag" AS "delete_flag"
		, "lks_temp_src_inur"."vehicle_number" AS "vehicle_number"
		, "lks_temp_src_inur"."product_number" AS "product_number"
	FROM "moto_sales_scn01_stg"."lks_ms_prse_prod_tmp" "lks_temp_src_inur"
	WHERE  "lks_temp_src_inur"."source" = 'STG' AND "lks_temp_src_inur"."equal" = 0
	;
END;


BEGIN -- lks_ed_tgt

	WITH "calc_load_end_date" AS 
	( 
		SELECT 
			  "lks_temp_src_us"."lnk_prse_prod_hkey" AS "lnk_prse_prod_hkey"
			, "lks_temp_src_us"."load_date" AS "load_date"
			, CASE WHEN "lks_temp_src_us"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 
				'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN "lci_src"."load_cycle_id" ELSE "lks_temp_src_us"."load_cycle_id" END AS "load_cycle_id"
			, COALESCE(LEAD("lks_temp_src_us"."load_date",1)OVER(PARTITION BY "lks_temp_src_us"."product_sensors_hkey" ORDER BY "lks_temp_src_us"."load_date",
				"lks_temp_src_us"."load_cycle_id"), TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
		FROM "moto_sales_scn01_stg"."lks_ms_prse_prod_tmp" "lks_temp_src_us"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		WHERE  "lks_temp_src_us"."equal" = 0
	)
	, "filter_load_end_date" AS 
	( 
		SELECT 
			  "calc_load_end_date"."lnk_prse_prod_hkey" AS "lnk_prse_prod_hkey"
			, "calc_load_end_date"."load_date" AS "load_date"
			, "calc_load_end_date"."load_cycle_id" AS "load_cycle_id"
			, "calc_load_end_date"."load_end_date" AS "load_end_date"
		FROM "calc_load_end_date" "calc_load_end_date"
		WHERE  "calc_load_end_date"."load_end_date" != TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
	)
	UPDATE "moto_scn01_fl"."lks_ms_prse_prod" "lks_ed_tgt"
	SET 
		 "load_end_date" =  "filter_load_end_date"."load_end_date"
		,"load_cycle_id" =  "filter_load_end_date"."load_cycle_id"
	FROM  "filter_load_end_date"
	WHERE "lks_ed_tgt"."lnk_prse_prod_hkey" =  "filter_load_end_date"."lnk_prse_prod_hkey"
	  AND "lks_ed_tgt"."load_date" =  "filter_load_end_date"."load_date"
	;
END;



END;
$function$;
 
 
