CREATE OR REPLACE FUNCTION "moto_scn01_proc"."ref_ms_codes_to_languag_incr"() 
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

BEGIN -- ref_upd_tgt

	WITH "change_set" AS 
	( 
		SELECT 
			  "tdfv_miv_src"."description" AS "description"
			, "tdfv_miv_src"."update_timestamp" AS "update_timestamp"
			, "tdfv_miv_src"."code" AS "code"
			, "tdfv_miv_src"."language_code" AS "language_code"
			, "tdfv_miv_src"."trans_timestamp" AS "trans_timestamp"
			, ROW_NUMBER()OVER(PARTITION BY "tdfv_miv_src"."code" ,  "tdfv_miv_src"."language_code" ORDER BY "tdfv_miv_src"."trans_timestamp" DESC) AS "dummy"
		FROM "moto_sales_scn01_dfv"."vw_codes_to_language" "tdfv_miv_src"
	)
	, "miv" AS 
	( 
		SELECT 
			  "change_set"."description" AS "description"
			, "change_set"."update_timestamp" AS "update_timestamp"
			, "change_set"."code" AS "code"
			, "change_set"."language_code" AS "language_code"
			, "lci_miv_src"."load_cycle_id" AS "load_cycle_id"
			, "change_set"."trans_timestamp" AS "load_date"
		FROM "change_set" "change_set"
		INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_miv_src" ON  1 = 1
		WHERE  "change_set"."dummy" = 1
	)
	UPDATE "moto_scn01_fl"."ref_ms_codes_to_languag" "ref_upd_tgt"
	SET 
		 "description" =  "miv"."description"
		,"update_timestamp" =  "miv"."update_timestamp"
		,"load_cycle_id" =  "miv"."load_cycle_id"
		,"load_date" =  "miv"."load_date"
	FROM  "miv"
	WHERE "ref_upd_tgt"."code" =  "miv"."code"
	  AND "ref_upd_tgt"."language_code" =  "miv"."language_code"
	;
END;


BEGIN -- ref_tgt

	INSERT INTO "moto_scn01_fl"."ref_ms_codes_to_languag"(
		 "code"
		,"language_code"
		,"load_cycle_id"
		,"load_date"
		,"description"
		,"update_timestamp"
	)
	WITH "new_set" AS 
	( 
		SELECT 
			  "tdfv_src"."description" AS "description"
			, "tdfv_src"."update_timestamp" AS "update_timestamp"
			, "tdfv_src"."code" AS "code"
			, "tdfv_src"."language_code" AS "language_code"
			, "tdfv_src"."trans_timestamp" AS "trans_timestamp"
			, ROW_NUMBER()OVER(PARTITION BY "tdfv_src"."code" ,  "tdfv_src"."language_code" ORDER BY "tdfv_src"."trans_timestamp" DESC) AS "dummy"
		FROM "moto_sales_scn01_dfv"."vw_codes_to_language" "tdfv_src"
	)
	SELECT 
		  "new_set"."code" AS "code"
		, "new_set"."language_code" AS "language_code"
		, "lci_src"."load_cycle_id" AS "load_cycle_id"
		, "new_set"."trans_timestamp" AS "load_date"
		, "new_set"."description" AS "description"
		, "new_set"."update_timestamp" AS "update_timestamp"
	FROM "new_set" "new_set"
	INNER JOIN "moto_sales_scn01_mtd"."load_cycle_info" "lci_src" ON  1 = 1
	LEFT OUTER JOIN "moto_scn01_fl"."ref_ms_codes_to_languag" "ref_src" ON  "new_set"."code" = "ref_src"."code" AND  "new_set"."language_code" = "ref_src"."language_code"
	WHERE  "new_set"."dummy" = 1 AND "ref_src"."code" IS NULL AND "ref_src"."language_code" IS NULL
	;
END;



END;
$function$;
 
 
