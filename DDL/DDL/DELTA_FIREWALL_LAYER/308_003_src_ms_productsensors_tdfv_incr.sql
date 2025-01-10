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


DROP VIEW IF EXISTS "moto_sales_scn01_dfv"."vw_product_sensors";
CREATE  VIEW "moto_sales_scn01_dfv"."vw_product_sensors"  AS 
	WITH "delta_view_filter" AS 
	( 
		SELECT 
			  'S' ::text AS "record_type"
			, "cdc_src"."vehicle_number" AS "vehicle_number"
			, "cdc_src"."product_number" AS "product_number"
			, "cdc_src"."sensor" AS "sensor"
			, "cdc_src"."sensor_value" AS "sensor_value"
			, "cdc_src"."unit_of_measurement" AS "unit_of_measurement"
		FROM "moto_sales_scn01"."jrn_product_sensors" "cdc_src"
	)
	, "delta_view" AS 
	( 
		SELECT 
			  "delta_view_filter"."record_type" AS "record_type"
			, "delta_view_filter"."vehicle_number" AS "vehicle_number"
			, "delta_view_filter"."product_number" AS "product_number"
			, "delta_view_filter"."sensor" AS "sensor"
			, "delta_view_filter"."sensor_value" AS "sensor_value"
			, "delta_view_filter"."unit_of_measurement" AS "unit_of_measurement"
		FROM "delta_view_filter" "delta_view_filter"
	)
	, "prepjoinbk" AS 
	( 
		SELECT 
			  "delta_view"."record_type" AS "record_type"
			, CASE WHEN "delta_view"."vehicle_number" = '' THEN "mex_bk_src"."key_attribute_varchar" ELSE COALESCE("delta_view"."vehicle_number",
				"mex_bk_src"."key_attribute_varchar")END AS "vehicle_number"
			, COALESCE("delta_view"."product_number", TO_NUMBER("mex_bk_src"."key_attribute_numeric", 
				'999999999999D999999999'::varchar)) AS "product_number"
			, "delta_view"."sensor" AS "sensor"
			, "delta_view"."sensor_value" AS "sensor_value"
			, "delta_view"."unit_of_measurement" AS "unit_of_measurement"
		FROM "delta_view" "delta_view"
		INNER JOIN "moto_sales_scn01_mtd"."mtd_exception_records" "mex_bk_src" ON  1 = 1
		WHERE  "mex_bk_src"."record_type" = 'N'
	)
	SELECT 
		  "prepjoinbk"."record_type" AS "record_type"
		, "prepjoinbk"."vehicle_number" AS "vehicle_number"
		, "prepjoinbk"."product_number" AS "product_number"
		, "prepjoinbk"."sensor" AS "sensor"
		, "prepjoinbk"."sensor_value" AS "sensor_value"
		, "prepjoinbk"."unit_of_measurement" AS "unit_of_measurement"
	FROM "prepjoinbk" "prepjoinbk"
	;

 
 
