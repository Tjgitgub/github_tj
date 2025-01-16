CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_mm_addresses_init"() 
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

BEGIN -- stg_tgt

	TRUNCATE TABLE "moto_mktg_scn01_stg"."addresses"  CASCADE;

	INSERT INTO "moto_mktg_scn01_stg"."addresses"(
		 "addresses_hkey"
		,"load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"address_number"
		,"street_name"
		,"street_number"
		,"postal_code"
		,"city"
		,"street_name_bk"
		,"street_number_bk"
		,"postal_code_bk"
		,"city_bk"
		,"province"
		,"update_timestamp"
	)
	SELECT 
		  UPPER(ENCODE(DIGEST( "ext_src"."street_name_bk" || '#' ||  "ext_src"."street_number_bk" || '#' ||  "ext_src"."postal_code_bk" || 
			'#' ||  "ext_src"."city_bk" || '#' ,'MD5'),'HEX')) AS "addresses_hkey"
		, "ext_src"."load_date" AS "load_date"
		, "ext_src"."load_cycle_id" AS "load_cycle_id"
		, "ext_src"."trans_timestamp" AS "trans_timestamp"
		, "ext_src"."operation" AS "operation"
		, "ext_src"."record_type" AS "record_type"
		, "ext_src"."address_number" AS "address_number"
		, "ext_src"."street_name" AS "street_name"
		, "ext_src"."street_number" AS "street_number"
		, "ext_src"."postal_code" AS "postal_code"
		, "ext_src"."city" AS "city"
		, "ext_src"."street_name_bk" AS "street_name_bk"
		, "ext_src"."street_number_bk" AS "street_number_bk"
		, "ext_src"."postal_code_bk" AS "postal_code_bk"
		, "ext_src"."city_bk" AS "city_bk"
		, "ext_src"."province" AS "province"
		, "ext_src"."update_timestamp" AS "update_timestamp"
	FROM "moto_mktg_scn01_ext"."addresses" "ext_src"
	INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
	;
END;



END;
$function$;
 
 
