CREATE OR REPLACE FUNCTION "moto_scn01_proc"."hub_ms_products_incr"() 
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

BEGIN -- hub_tgt

	INSERT INTO "moto_scn01_fl"."hub_products"(
		 "products_hkey"
		,"load_date"
		,"load_cycle_id"
		,"product_cc_bk"
		,"product_et_code_bk"
		,"product_part_code_bk"
		,"src_bk"
	)
	WITH "change_set" AS 
	( 
		SELECT 
			  "stg_src1"."products_hkey" AS "products_hkey"
			, "stg_src1"."load_date" AS "load_date"
			, "stg_src1"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "logposition"
			, "stg_src1"."product_cc_bk" AS "product_cc_bk"
			, "stg_src1"."product_et_code_bk" AS "product_et_code_bk"
			, "stg_src1"."product_part_code_bk" AS "product_part_code_bk"
			, "stg_src1"."src_bk" AS "src_bk"
			, 0 AS "general_order"
		FROM "moto_sales_scn01_stg"."products" "stg_src1"
		WHERE  "stg_src1"."record_type" = 'S'
	)
	, "min_load_time" AS 
	( 
		SELECT 
			  "change_set"."products_hkey" AS "products_hkey"
			, "change_set"."load_date" AS "load_date"
			, "change_set"."load_cycle_id" AS "load_cycle_id"
			, "change_set"."product_cc_bk" AS "product_cc_bk"
			, "change_set"."product_et_code_bk" AS "product_et_code_bk"
			, "change_set"."product_part_code_bk" AS "product_part_code_bk"
			, "change_set"."src_bk" AS "src_bk"
			, ROW_NUMBER()OVER(PARTITION BY "change_set"."products_hkey" ORDER BY "change_set"."general_order",
				"change_set"."load_date","change_set"."logposition") AS "dummy"
		FROM "change_set" "change_set"
	)
	SELECT 
		  "min_load_time"."products_hkey" AS "products_hkey"
		, "min_load_time"."load_date" AS "load_date"
		, "min_load_time"."load_cycle_id" AS "load_cycle_id"
		, "min_load_time"."product_cc_bk" AS "product_cc_bk"
		, "min_load_time"."product_et_code_bk" AS "product_et_code_bk"
		, "min_load_time"."product_part_code_bk" AS "product_part_code_bk"
		, "min_load_time"."src_bk" AS "src_bk"
	FROM "min_load_time" "min_load_time"
	LEFT OUTER JOIN "moto_scn01_fl"."hub_products" "hub_src" ON  "min_load_time"."products_hkey" = "hub_src"."products_hkey"
	WHERE  "min_load_time"."dummy" = 1 AND "hub_src"."products_hkey" is NULL
	;
END;



END;
$function$;
 
 
