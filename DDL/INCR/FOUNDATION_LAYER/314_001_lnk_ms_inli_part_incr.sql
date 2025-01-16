CREATE OR REPLACE FUNCTION "moto_scn01_proc"."lnk_ms_inli_part_incr"() 
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

BEGIN -- lnk_tgt

	INSERT INTO "moto_scn01_fl"."lnk_inli_part"(
		 "lnk_inli_part_hkey"
		,"load_date"
		,"load_cycle_id"
		,"parts_hkey"
		,"invoice_lines_hkey"
	)
	WITH "change_set" AS 
	( 
		SELECT 
			  "stg_src1"."lnk_inli_part_hkey" AS "lnk_inli_part_hkey"
			, "stg_src1"."load_date" AS "load_date"
			, "stg_src1"."load_cycle_id" AS "load_cycle_id"
			, "stg_src1"."parts_hkey" AS "parts_hkey"
			, "stg_src1"."invoice_lines_hkey" AS "invoice_lines_hkey"
			, 0 AS "logposition"
		FROM "moto_sales_scn01_stg"."invoice_lines" "stg_src1"
		WHERE  "stg_src1"."error_code_inli_part" in(0,1)
	)
	, "min_load_time" AS 
	( 
		SELECT 
			  "change_set"."lnk_inli_part_hkey" AS "lnk_inli_part_hkey"
			, "change_set"."load_date" AS "load_date"
			, "change_set"."load_cycle_id" AS "load_cycle_id"
			, "change_set"."parts_hkey" AS "parts_hkey"
			, "change_set"."invoice_lines_hkey" AS "invoice_lines_hkey"
			, ROW_NUMBER()OVER(PARTITION BY "change_set"."lnk_inli_part_hkey" ORDER BY "change_set"."load_date",
				"change_set"."logposition") AS "dummy"
		FROM "change_set" "change_set"
	)
	SELECT 
		  "min_load_time"."lnk_inli_part_hkey" AS "lnk_inli_part_hkey"
		, "min_load_time"."load_date" AS "load_date"
		, "min_load_time"."load_cycle_id" AS "load_cycle_id"
		, "min_load_time"."parts_hkey" AS "parts_hkey"
		, "min_load_time"."invoice_lines_hkey" AS "invoice_lines_hkey"
	FROM "min_load_time" "min_load_time"
	LEFT OUTER JOIN "moto_scn01_fl"."lnk_inli_part" "lnk_src" ON  "min_load_time"."lnk_inli_part_hkey" = "lnk_src"."lnk_inli_part_hkey"
	WHERE  "lnk_src"."lnk_inli_part_hkey" IS NULL AND "min_load_time"."dummy" = 1
	;
END;



END;
$function$;
 
 
