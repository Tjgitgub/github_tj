CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lks_ms_prod_prod_rpid_incr"() 
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

	TRUNCATE TABLE "moto_sales_scn01_stg"."lks_ms_prod_prod_rpid_tmp"  CASCADE;

	INSERT INTO "moto_sales_scn01_stg"."lks_ms_prod_prod_rpid_tmp"(
		 "lnk_prod_prod_rpid_hkey"
		,"products_rpid_hkey"
		,"products_hkey"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"record_type"
		,"source"
		,"equal"
		,"delete_flag"
		,"trans_timestamp"
		,"product_id"
		,"replacement_product_id"
	)
	WITH "dist_stg" AS 
	( 
		SELECT 
			  "stg_dis_src"."products_hkey" AS "products_hkey"
			, "stg_dis_src"."load_cycle_id" AS "load_cycle_id"
			, MIN("stg_dis_src"."load_date") AS "min_load_timestamp"
			, MIN("stg_dis_src"."error_code_prod_prod_rpid") AS "error_code_prod_prod_rpid"
		FROM "moto_sales_scn01_stg"."products" "stg_dis_src"
		WHERE  "stg_dis_src"."error_code_prod_prod_rpid" in(0,1)
		GROUP BY  "stg_dis_src"."products_hkey",  "stg_dis_src"."load_cycle_id"
	)
	, "last_lks" AS 
	( 
		SELECT 
			  "last_lnk_src"."products_hkey" AS "products_hkey"
			, "dist_stg"."load_cycle_id" AS "load_cycle_id"
			, MAX("last_lks_src"."load_date") AS "max_load_timestamp"
			, MIN("dist_stg"."error_code_prod_prod_rpid") AS "error_code_prod_prod_rpid"
		FROM "moto_scn01_fl"."lks_ms_prod_prod_rpid" "last_lks_src"
		INNER JOIN "moto_scn01_fl"."lnk_prod_prod_rpid" "last_lnk_src" ON  "last_lks_src"."lnk_prod_prod_rpid_hkey" = "last_lnk_src"."lnk_prod_prod_rpid_hkey"
		INNER JOIN "dist_stg" "dist_stg" ON  "last_lnk_src"."products_hkey" = "dist_stg"."products_hkey"
		WHERE  "last_lks_src"."load_date" <= "dist_stg"."min_load_timestamp"
		GROUP BY  "last_lnk_src"."products_hkey",  "dist_stg"."load_cycle_id"
	)
	, "temp_table_set" AS 
	( 
		SELECT 
			  "stg_temp_src"."lnk_prod_prod_rpid_hkey" AS "lnk_prod_prod_rpid_hkey"
			, "stg_temp_src"."products_rpid_hkey" AS "products_rpid_hkey"
			, "stg_temp_src"."products_hkey" AS "products_hkey"
			, "stg_temp_src"."load_date" AS "load_date"
			, "stg_temp_src"."load_cycle_id" AS "load_cycle_id"
			, TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar) AS "load_end_date"
			, "stg_temp_src"."record_type" AS "record_type"
			, 'STG' AS "source"
			, 1 AS "origin_id"
			, CASE WHEN "stg_temp_src"."operation" = 'D' THEN 'Y'::text ELSE 'N'::text END AS "delete_flag"
			, "stg_temp_src"."trans_timestamp" AS "trans_timestamp"
			, "stg_temp_src"."product_id" AS "product_id"
			, "stg_temp_src"."replacement_product_id" AS "replacement_product_id"
		FROM "moto_sales_scn01_stg"."products" "stg_temp_src"
		WHERE  "stg_temp_src"."error_code_prod_prod_rpid" in(0,1)
		UNION 
		SELECT 
			  "lks_src"."lnk_prod_prod_rpid_hkey" AS "lnk_prod_prod_rpid_hkey"
			, "lnk_src"."products_rpid_hkey" AS "products_rpid_hkey"
			, "lnk_src"."products_hkey" AS "products_hkey"
			, "lks_src"."load_date" AS "load_date"
			, "lks_src"."load_cycle_id" AS "load_cycle_id"
			, "lks_src"."load_end_date" AS "load_end_date"
			, 'SAT' AS "record_type"
			, 'LKS' AS "source"
			, 0 AS "origin_id"
			, "lks_src"."delete_flag" AS "delete_flag"
			, "lks_src"."trans_timestamp" AS "trans_timestamp"
			, "lks_src"."product_id" AS "product_id"
			, "lks_src"."replacement_product_id" AS "replacement_product_id"
		FROM "moto_scn01_fl"."lks_ms_prod_prod_rpid" "lks_src"
		INNER JOIN "moto_scn01_fl"."lnk_prod_prod_rpid" "lnk_src" ON  "lks_src"."lnk_prod_prod_rpid_hkey" = "lnk_src"."lnk_prod_prod_rpid_hkey"
		INNER JOIN "last_lks" "last_lks" ON  "lnk_src"."products_hkey" = "last_lks"."products_hkey"
		WHERE  "lks_src"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000' , 'DD/MM/YYYY HH24:MI:SS.US'::varchar) OR("last_lks"."error_code_prod_prod_rpid" = 0 AND "lks_src"."load_date" >= "last_lks"."max_load_timestamp")
	)
	SELECT 
		  "temp_table_set"."lnk_prod_prod_rpid_hkey" AS "lnk_prod_prod_rpid_hkey"
		, "temp_table_set"."products_rpid_hkey" AS "products_rpid_hkey"
		, "temp_table_set"."products_hkey" AS "products_hkey"
		, "temp_table_set"."load_date" AS "load_date"
		, "temp_table_set"."load_cycle_id" AS "load_cycle_id"
		, "temp_table_set"."load_end_date" AS "load_end_date"
		, "temp_table_set"."record_type" AS "record_type"
		, "temp_table_set"."source" AS "source"
		, CASE WHEN "temp_table_set"."source" = 'STG' AND "temp_table_set"."delete_flag"::text || "temp_table_set"."products_rpid_hkey"::text =
			LAG( "temp_table_set"."delete_flag"::text || "temp_table_set"."products_rpid_hkey"::text,1)OVER(PARTITION BY "temp_table_set"."products_hkey" ORDER BY "temp_table_set"."load_date","temp_table_set"."origin_id")THEN 1 ELSE 0 END AS "equal"
		, "temp_table_set"."delete_flag" AS "delete_flag"
		, "temp_table_set"."trans_timestamp" AS "trans_timestamp"
		, "temp_table_set"."product_id" AS "product_id"
		, "temp_table_set"."replacement_product_id" AS "replacement_product_id"
	FROM "temp_table_set" "temp_table_set"
	;
END;


BEGIN -- lks_inur_tgt

	INSERT INTO "moto_scn01_fl"."lks_ms_prod_prod_rpid"(
		 "lnk_prod_prod_rpid_hkey"
		,"products_rpid_hkey"
		,"products_hkey"
		,"load_date"
		,"load_cycle_id"
		,"load_end_date"
		,"delete_flag"
		,"trans_timestamp"
		,"product_id"
		,"replacement_product_id"
	)
	SELECT 
		  "lks_temp_src_inur"."lnk_prod_prod_rpid_hkey" AS "lnk_prod_prod_rpid_hkey"
		, "lks_temp_src_inur"."products_rpid_hkey" AS "products_rpid_hkey"
		, "lks_temp_src_inur"."products_hkey" AS "products_hkey"
		, "lks_temp_src_inur"."load_date" AS "load_date"
		, "lks_temp_src_inur"."load_cycle_id" AS "load_cycle_id"
		, "lks_temp_src_inur"."load_end_date" AS "load_end_date"
		, "lks_temp_src_inur"."delete_flag" AS "delete_flag"
		, "lks_temp_src_inur"."trans_timestamp" AS "trans_timestamp"
		, "lks_temp_src_inur"."product_id" AS "product_id"
		, "lks_temp_src_inur"."replacement_product_id" AS "replacement_product_id"
	FROM "moto_sales_scn01_stg"."lks_ms_prod_prod_rpid_tmp" "lks_temp_src_inur"
	WHERE  "lks_temp_src_inur"."source" = 'STG' AND "lks_temp_src_inur"."equal" = 0
	;
END;


BEGIN -- lks_ed_tgt

	WITH "calc_load_end_date" AS 
	( 
		SELECT 
			  "lks_temp_src_us"."lnk_prod_prod_rpid_hkey" AS "lnk_prod_prod_rpid_hkey"
			, "lks_temp_src_us"."load_date" AS "load_date"
			, CASE WHEN "lks_temp_src_us"."load_end_date" = TO_TIMESTAMP('31/12/2399 23:59:59.000000', 
				'DD/MM/YYYY HH24:MI:SS.US'::varchar) THEN "lci_src"."load_cycle_id" ELSE "lks_temp_src_us"."load_cycle_id" END AS "load_cycle_id"
			, COALESCE(LEAD("lks_temp_src_us"."load_date",1)OVER(PARTITION BY "lks_temp_src_us"."products_hkey" ORDER BY "lks_temp_src_us"."load_date",
				"lks_temp_src_us"."load_cycle_id"), TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)) AS "load_end_date"
		FROM "moto_sales_scn01_stg"."lks_ms_prod_prod_rpid_tmp" "lks_temp_src_us"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
		WHERE  "lks_temp_src_us"."equal" = 0
	)
	, "filter_load_end_date" AS 
	( 
		SELECT 
			  "calc_load_end_date"."lnk_prod_prod_rpid_hkey" AS "lnk_prod_prod_rpid_hkey"
			, "calc_load_end_date"."load_date" AS "load_date"
			, "calc_load_end_date"."load_cycle_id" AS "load_cycle_id"
			, "calc_load_end_date"."load_end_date" AS "load_end_date"
		FROM "calc_load_end_date" "calc_load_end_date"
		WHERE  "calc_load_end_date"."load_end_date" != TO_TIMESTAMP('31/12/2399 23:59:59.000000', 'DD/MM/YYYY HH24:MI:SS.US'::varchar)
	)
	UPDATE "moto_scn01_fl"."lks_ms_prod_prod_rpid" "lks_ed_tgt"
	SET 
		 "load_end_date" =  "filter_load_end_date"."load_end_date"
		,"load_cycle_id" =  "filter_load_end_date"."load_cycle_id"
	FROM  "filter_load_end_date"
	WHERE "lks_ed_tgt"."lnk_prod_prod_rpid_hkey" =  "filter_load_end_date"."lnk_prod_prod_rpid_hkey"
	  AND "lks_ed_tgt"."load_date" =  "filter_load_end_date"."load_date"
	;
END;



END;
$function$;
 
 
