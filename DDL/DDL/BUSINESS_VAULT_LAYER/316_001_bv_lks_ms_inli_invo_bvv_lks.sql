/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.16, generation date: 2025/01/16 15:03:29
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
BV release: release1(2) - Comment: VaultSpeed Automation - Release date: 2025/01/16 14:56:23, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08
 */


DROP VIEW IF EXISTS "moto_scn01_bv"."lks_ms_inli_invo";
CREATE  VIEW "moto_scn01_bv"."lks_ms_inli_invo"  AS 
	SELECT 
		  "dvt_src"."lnk_inli_invo_hkey" AS "lnk_inli_invo_hkey"
		, "dvt_src"."invoices_hkey" AS "invoices_hkey"
		, "dvt_src"."invoice_lines_hkey" AS "invoice_lines_hkey"
		, "dvt_src"."load_date" AS "load_date"
		, "dvt_src"."load_cycle_id" AS "load_cycle_id"
		, "dvt_src"."load_end_date" AS "load_end_date"
		, "dvt_src"."delete_flag" AS "delete_flag"
		, "dvt_src"."trans_timestamp" AS "trans_timestamp"
		, "dvt_src"."invoice_line_number" AS "invoice_line_number"
		, "dvt_src"."invoice_number" AS "invoice_number"
	FROM "moto_scn01_fl"."lks_ms_inli_invo" "dvt_src"
	;

 
 
