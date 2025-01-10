CREATE OR REPLACE FUNCTION "moto_scn01_proc"."hub_mm_contacts_incr"() 
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

Vaultspeed version: 5.7.2.14, generation date: 2025/01/09 12:48:54
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/09 09:40:46, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51
 */


BEGIN 

BEGIN -- hub_tgt

	INSERT INTO "moto_scn01_fl"."hub_contacts"(
		 "contacts_hkey"
		,"load_date"
		,"load_cycle_id"
		,"contact_id_bk"
	)
	WITH "change_set" AS 
	( 
		SELECT 
			  "stg_src1"."contacts_hkey" AS "contacts_hkey"
			, "stg_src1"."load_date" AS "load_date"
			, "stg_src1"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "logposition"
			, "stg_src1"."contact_id_bk" AS "contact_id_bk"
			, 0 AS "general_order"
		FROM "moto_mktg_scn01_stg"."contacts" "stg_src1"
		WHERE  "stg_src1"."record_type" = 'S'
		UNION ALL 
		SELECT 
			  "stg_fk_src1_1"."contacts_hkey" AS "contacts_hkey"
			, "stg_fk_src1_1"."load_date" AS "load_date"
			, "stg_fk_src1_1"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "logposition"
			, "stg_fk_src1_1"."contact_id_fk_contactid_bk" AS "contact_id_bk"
			, 1 AS "general_order"
		FROM "moto_mktg_scn01_stg"."camp_part_cont" "stg_fk_src1_1"
		WHERE  "stg_fk_src1_1"."record_type" = 'S'
		UNION ALL 
		SELECT 
			  "stg_fk_src1_2"."contacts_hkey" AS "contacts_hkey"
			, "stg_fk_src1_2"."load_date" AS "load_date"
			, "stg_fk_src1_2"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "logposition"
			, "stg_fk_src1_2"."contact_id_fk_contactid_bk" AS "contact_id_bk"
			, 1 AS "general_order"
		FROM "moto_mktg_scn01_stg"."party_contacts" "stg_fk_src1_2"
		WHERE  "stg_fk_src1_2"."record_type" = 'S'
		UNION ALL 
		SELECT 
			  "stg_fk_src1_3"."contacts_hkey" AS "contacts_hkey"
			, "stg_fk_src1_3"."load_date" AS "load_date"
			, "stg_fk_src1_3"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "logposition"
			, "stg_fk_src1_3"."contact_id_fk_contactid_bk" AS "contact_id_bk"
			, 1 AS "general_order"
		FROM "moto_mktg_scn01_stg"."e_mails" "stg_fk_src1_3"
		WHERE  "stg_fk_src1_3"."record_type" = 'S'
		UNION ALL 
		SELECT 
			  "stg_fk_src1_4"."contacts_hkey" AS "contacts_hkey"
			, "stg_fk_src1_4"."load_date" AS "load_date"
			, "stg_fk_src1_4"."load_cycle_id" AS "load_cycle_id"
			, 0 AS "logposition"
			, "stg_fk_src1_4"."contact_id_fk_contactid_bk" AS "contact_id_bk"
			, 1 AS "general_order"
		FROM "moto_mktg_scn01_stg"."phones" "stg_fk_src1_4"
		WHERE  "stg_fk_src1_4"."record_type" = 'S'
	)
	, "min_load_time" AS 
	( 
		SELECT 
			  "change_set"."contacts_hkey" AS "contacts_hkey"
			, "change_set"."load_date" AS "load_date"
			, "change_set"."load_cycle_id" AS "load_cycle_id"
			, "change_set"."contact_id_bk" AS "contact_id_bk"
			, ROW_NUMBER()OVER(PARTITION BY "change_set"."contacts_hkey" ORDER BY "change_set"."general_order",
				"change_set"."load_date","change_set"."logposition") AS "dummy"
		FROM "change_set" "change_set"
	)
	SELECT 
		  "min_load_time"."contacts_hkey" AS "contacts_hkey"
		, "min_load_time"."load_date" AS "load_date"
		, "min_load_time"."load_cycle_id" AS "load_cycle_id"
		, "min_load_time"."contact_id_bk" AS "contact_id_bk"
	FROM "min_load_time" "min_load_time"
	LEFT OUTER JOIN "moto_scn01_fl"."hub_contacts" "hub_src" ON  "min_load_time"."contacts_hkey" = "hub_src"."contacts_hkey"
	WHERE  "min_load_time"."dummy" = 1 AND "hub_src"."contacts_hkey" is NULL
	;
END;



END;
$function$;
 
 
