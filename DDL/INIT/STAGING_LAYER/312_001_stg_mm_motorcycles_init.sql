CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_mm_motorcycles_init"() 
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

Vaultspeed version: 5.7.2.16, generation date: 2025/01/17 07:04:50
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/16 14:56:23, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46
 */


BEGIN 

BEGIN -- stg_tgt

	TRUNCATE TABLE "moto_mktg_scn01_stg"."motorcycles"  CASCADE;

	INSERT INTO "moto_mktg_scn01_stg"."motorcycles"(
		 "products_hkey"
		,"load_date"
		,"load_cycle_id"
		,"src_bk"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"motorcycle_id"
		,"product_cc_bk"
		,"product_et_code_bk"
		,"product_part_code_bk"
		,"motorcycle_name"
		,"update_timestamp"
	)
	SELECT 
		  UPPER(ENCODE(DIGEST( 'moto_mktg_scn01' || '#' || "ext_src"."product_cc_bk" || '#' ||  "ext_src"."product_et_code_bk" || 
			'#' ||  "ext_src"."product_part_code_bk" || '#' ,'MD5'),'HEX')) AS "products_hkey"
		, "ext_src"."load_date" AS "load_date"
		, "ext_src"."load_cycle_id" AS "load_cycle_id"
		, 'moto_mktg_scn01' AS "src_bk"
		, "ext_src"."trans_timestamp" AS "trans_timestamp"
		, "ext_src"."operation" AS "operation"
		, "ext_src"."record_type" AS "record_type"
		, "ext_src"."motorcycle_id" AS "motorcycle_id"
		, "ext_src"."product_cc_bk" AS "product_cc_bk"
		, "ext_src"."product_et_code_bk" AS "product_et_code_bk"
		, "ext_src"."product_part_code_bk" AS "product_part_code_bk"
		, "ext_src"."motorcycle_name" AS "motorcycle_name"
		, "ext_src"."update_timestamp" AS "update_timestamp"
	FROM "moto_mktg_scn01_ext"."motorcycles" "ext_src"
	INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
	;
END;



END;
$function$;
 
 
