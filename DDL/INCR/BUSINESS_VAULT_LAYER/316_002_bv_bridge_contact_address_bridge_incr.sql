CREATE OR REPLACE FUNCTION "moto_scn01_proc"."bv_bridge_contact_address_bridge_incr"() 
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

Vaultspeed version: 5.7.2.14, generation date: 2025/01/09 12:50:01
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/09 09:40:46, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04
 */


BEGIN 

BEGIN -- bridge_tgt

	INSERT INTO "moto_scn01_bv"."bridge_contact_address"(
		 "load_date"
		,"load_cycle_id"
		,"addresses_hkey"
		,"customers_hkey"
		,"contacts_hkey"
		,"lnk_cust_addr_hkey"
		,"lnd_cust_cont_hkey"
		,"street_name_bk"
		,"street_number_bk"
		,"postal_code_bk"
		,"city_bk"
		,"contact_id_bk"
		,"customers_bk"
	)
	WITH "miv" AS 
	( 
		SELECT 
			  "bvlci_src"."load_date" AS "load_date"
			, "bvlci_src"."load_cycle_id" AS "load_cycle_id"
			, "dvo_src1"."addresses_hkey" AS "addresses_hkey"
			, "dvo_src3"."customers_hkey" AS "customers_hkey"
			, "dvo_src5"."contacts_hkey" AS "contacts_hkey"
			, "dvo_src2"."lnk_cust_addr_hkey" AS "lnk_cust_addr_hkey"
			, "dvo_src4"."lnd_cust_cont_hkey" AS "lnd_cust_cont_hkey"
			, "dvo_src1"."street_name_bk" AS "street_name_bk"
			, "dvo_src1"."street_number_bk" AS "street_number_bk"
			, "dvo_src1"."postal_code_bk" AS "postal_code_bk"
			, "dvo_src1"."city_bk" AS "city_bk"
			, "dvo_src5"."contact_id_bk" AS "contact_id_bk"
			, "dvo_src3"."customers_bk" AS "customers_bk"
		FROM "moto_scn01_fmc"."load_cycle_info" "bvlci_src"
		INNER JOIN "moto_scn01_fl"."hub_addresses" "dvo_src1" ON 1 = 1
		INNER JOIN "moto_scn01_fl"."lnk_cust_addr" "dvo_src2" ON "dvo_src2"."addresses_hkey" = "dvo_src1"."addresses_hkey"
		INNER JOIN "moto_scn01_fl"."hub_customers" "dvo_src3" ON "dvo_src3"."customers_hkey" = "dvo_src2"."customers_hkey"
		INNER JOIN "moto_scn01_fl"."lnd_cust_cont" "dvo_src4" ON "dvo_src4"."customers_hkey" = "dvo_src3"."customers_hkey"
		INNER JOIN "moto_scn01_fl"."hub_contacts" "dvo_src5" ON "dvo_src5"."contacts_hkey" = "dvo_src4"."contacts_hkey"
	)
	SELECT 
		  "miv"."load_date" AS "load_date"
		, "miv"."load_cycle_id" AS "load_cycle_id"
		, "miv"."addresses_hkey" AS "addresses_hkey"
		, "miv"."customers_hkey" AS "customers_hkey"
		, "miv"."contacts_hkey" AS "contacts_hkey"
		, "miv"."lnk_cust_addr_hkey" AS "lnk_cust_addr_hkey"
		, "miv"."lnd_cust_cont_hkey" AS "lnd_cust_cont_hkey"
		, "miv"."street_name_bk" AS "street_name_bk"
		, "miv"."street_number_bk" AS "street_number_bk"
		, "miv"."postal_code_bk" AS "postal_code_bk"
		, "miv"."city_bk" AS "city_bk"
		, "miv"."contact_id_bk" AS "contact_id_bk"
		, "miv"."customers_bk" AS "customers_bk"
	FROM "miv" "miv"
	WHERE  NOT EXISTS
	(
		SELECT 
			  1 AS "dummy"
		FROM "moto_scn01_bv"."bridge_contact_address" "bridge_src"
		WHERE  "bridge_src"."addresses_hkey" = "miv"."addresses_hkey" AND  "bridge_src"."customers_hkey" = "miv"."customers_hkey" AND  "bridge_src"."contacts_hkey" = "miv"."contacts_hkey"
	)
	;
END;



END;
$function$;
 
 
