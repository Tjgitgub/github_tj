CREATE OR REPLACE FUNCTION "moto_scn01_proc"."bv_bridge_part_customer_bridge_init"() 
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

	TRUNCATE TABLE "moto_scn01_bv"."bridge_part_customer"  CASCADE;

	INSERT INTO "moto_scn01_bv"."bridge_part_customer"(
		 "bridge_part_customer_hkey"
		,"load_date"
		,"load_cycle_id"
		,"customers_hkey"
		,"invoice_lines_hkey"
		,"invoices_hkey"
		,"parts_hkey"
		,"lnk_inli_part_hkey"
		,"lnk_inli_invo_hkey"
		,"lnk_invo_cust_hkey"
		,"inli_invoice_number_bk"
		,"invoice_line_number_bk"
		,"invo_invoice_number_bk"
		,"part_number_bk"
		,"part_langua_code_bk"
		,"customers_bk"
	)
	WITH "miv" AS 
	( 
		SELECT 
			  UPPER(ENCODE(DIGEST(  "dvo_src1"."customers_hkey"::text || '#' || "dvo_src3"."invoices_hkey"::text || '#' || 
				"dvo_src5"."invoice_lines_hkey"::text || '#' || "dvo_src7"."parts_hkey"::text || '#'  ,'MD5'),'HEX')) AS "bridge_part_customer_hkey"
			, "bvlci_src"."load_date" AS "load_date"
			, "bvlci_src"."load_cycle_id" AS "load_cycle_id"
			, "dvo_src1"."customers_hkey" AS "customers_hkey"
			, "dvo_src3"."invoices_hkey" AS "invoices_hkey"
			, "dvo_src5"."invoice_lines_hkey" AS "invoice_lines_hkey"
			, "dvo_src7"."parts_hkey" AS "parts_hkey"
			, "dvo_src2"."lnk_invo_cust_hkey" AS "lnk_invo_cust_hkey"
			, "dvo_src4"."lnk_inli_invo_hkey" AS "lnk_inli_invo_hkey"
			, "dvo_src6"."lnk_inli_part_hkey" AS "lnk_inli_part_hkey"
			, "dvo_src3"."invoice_number_bk" AS "invo_invoice_number_bk"
			, "dvo_src5"."invoice_number_bk" AS "inli_invoice_number_bk"
			, "dvo_src5"."invoice_line_number_bk" AS "invoice_line_number_bk"
			, "dvo_src7"."part_number_bk" AS "part_number_bk"
			, "dvo_src7"."part_langua_code_bk" AS "part_langua_code_bk"
			, "dvo_src1"."customers_bk" AS "customers_bk"
		FROM "moto_scn01_fmc"."load_cycle_info" "bvlci_src"
		INNER JOIN "moto_scn01_fl"."hub_customers" "dvo_src1" ON 1 = 1
		INNER JOIN "moto_scn01_fl"."lnk_invo_cust" "dvo_src2" ON "dvo_src2"."customers_hkey" = "dvo_src1"."customers_hkey"
		INNER JOIN "moto_scn01_fl"."hub_invoices" "dvo_src3" ON "dvo_src3"."invoices_hkey" = "dvo_src2"."invoices_hkey"
		INNER JOIN "moto_scn01_fl"."lnk_inli_invo" "dvo_src4" ON "dvo_src4"."invoices_hkey" = "dvo_src3"."invoices_hkey"
		INNER JOIN "moto_scn01_fl"."hub_invoice_lines" "dvo_src5" ON "dvo_src5"."invoice_lines_hkey" = "dvo_src4"."invoice_lines_hkey"
		INNER JOIN "moto_scn01_fl"."lnk_inli_part" "dvo_src6" ON "dvo_src6"."invoice_lines_hkey" = "dvo_src5"."invoice_lines_hkey"
		INNER JOIN "moto_scn01_fl"."hub_parts" "dvo_src7" ON "dvo_src7"."parts_hkey" = "dvo_src6"."parts_hkey"
	)
	SELECT 
		  "miv"."bridge_part_customer_hkey" AS "bridge_part_customer_hkey"
		, "miv"."load_date" AS "load_date"
		, "miv"."load_cycle_id" AS "load_cycle_id"
		, "miv"."customers_hkey" AS "customers_hkey"
		, "miv"."invoice_lines_hkey" AS "invoice_lines_hkey"
		, "miv"."invoices_hkey" AS "invoices_hkey"
		, "miv"."parts_hkey" AS "parts_hkey"
		, "miv"."lnk_inli_part_hkey" AS "lnk_inli_part_hkey"
		, "miv"."lnk_inli_invo_hkey" AS "lnk_inli_invo_hkey"
		, "miv"."lnk_invo_cust_hkey" AS "lnk_invo_cust_hkey"
		, "miv"."inli_invoice_number_bk" AS "inli_invoice_number_bk"
		, "miv"."invoice_line_number_bk" AS "invoice_line_number_bk"
		, "miv"."invo_invoice_number_bk" AS "invo_invoice_number_bk"
		, "miv"."part_number_bk" AS "part_number_bk"
		, "miv"."part_langua_code_bk" AS "part_langua_code_bk"
		, "miv"."customers_bk" AS "customers_bk"
	FROM "miv" "miv"
	;
END;



END;
$function$;
 
 
