CREATE OR REPLACE FUNCTION "moto_scn01_proc"."stg_mm_phones_incr"() 
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

	TRUNCATE TABLE "moto_mktg_scn01_stg"."phones"  CASCADE;

	INSERT INTO "moto_mktg_scn01_stg"."phones"(
		 "contacts_hkey"
		,"load_date"
		,"load_cycle_id"
		,"trans_timestamp"
		,"operation"
		,"record_type"
		,"contact_id"
		,"contact_id_fk_contactid_bk"
		,"phone_number"
		,"update_timestamp"
	)
	WITH "calc_hash_keys" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST( "ext_src"."contact_id_fk_contactid_bk" || '#' ,'MD5'),'HEX')) AS "contacts_hkey"
			, "ext_src"."load_date" AS "load_date"
			, "ext_src"."load_cycle_id" AS "load_cycle_id"
			, "ext_src"."trans_timestamp" AS "trans_timestamp"
			, "ext_src"."operation" AS "operation"
			, "ext_src"."record_type" AS "record_type"
			, "ext_src"."contact_id" AS "contact_id"
			, "ext_src"."contact_id_fk_contactid_bk" AS "contact_id_fk_contactid_bk"
			, "ext_src"."phone_number" AS "phone_number"
			, "ext_src"."update_timestamp" AS "update_timestamp"
		FROM "moto_mktg_scn01_ext"."phones" "ext_src"
		INNER JOIN "moto_mktg_scn01_mtd"."mtd_exception_records" "mex_src" ON  "mex_src"."record_type" = 'U'
	)
	SELECT 
		  "calc_hash_keys"."contacts_hkey" AS "contacts_hkey"
		, CURRENT_TIMESTAMP + row_number() over (PARTITION BY  "calc_hash_keys"."contacts_hkey"  ORDER BY  "calc_hash_keys"."trans_timestamp")
			* interval'2 microsecond'   AS "load_date"
		, "calc_hash_keys"."load_cycle_id" AS "load_cycle_id"
		, "calc_hash_keys"."trans_timestamp" AS "trans_timestamp"
		, "calc_hash_keys"."operation" AS "operation"
		, "calc_hash_keys"."record_type" AS "record_type"
		, "calc_hash_keys"."contact_id" AS "contact_id"
		, "calc_hash_keys"."contact_id_fk_contactid_bk" AS "contact_id_fk_contactid_bk"
		, "calc_hash_keys"."phone_number" AS "phone_number"
		, "calc_hash_keys"."update_timestamp" AS "update_timestamp"
	FROM "calc_hash_keys" "calc_hash_keys"
	;
END;



END;
$function$;
 
 
