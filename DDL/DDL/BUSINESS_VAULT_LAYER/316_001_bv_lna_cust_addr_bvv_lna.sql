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


DROP VIEW IF EXISTS "moto_scn01_bv"."view_lna_cust_addr";
CREATE  VIEW "moto_scn01_bv"."view_lna_cust_addr"  AS 
	SELECT 
		  "dvt_src"."lna_cust_addr_hkey" AS "lna_cust_addr_hkey"
		, "dvt_src"."customers_hkey" AS "customers_hkey"
		, "dvt_src"."addresses_hkey" AS "addresses_hkey"
		, "dvt_src"."load_cycle_id" AS "load_cycle_id"
		, "dvt_src"."load_date" AS "load_date"
	FROM "moto_scn01_bv"."lna_cust_addr" "dvt_src"
	;

 
 
