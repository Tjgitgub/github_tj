/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.16, generation date: 2025/01/20 10:46:04
DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/20 10:43:29
 */

/* DROP TABLES */

-- START
DROP TABLE IF EXISTS "moto_scn01_bv"."pit_camp_prod_camp_prod" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_bv"."pit_contacts_contacts" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_bv"."pit_location_cust_addr" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_bv"."pit_prse_prse_prod" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_bv"."bridge_contact_address" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_bv"."bridge_part_customer" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fmc"."fmc_bv_loading_window_table" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fmc"."load_cycle_info" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fmc"."dv_load_cycle_info" 
CASCADE
;

-- END


/* CREATE TABLES */

-- START

CREATE   TABLE "moto_scn01_bv"."pit_camp_prod_camp_prod"
(
	 "pit_camp_prod_camp_prod_hkey" VARCHAR(32)
	,"snapshot_timestamp" TIMESTAMP
	,"load_cycle_id" INTEGER
	,"lnd_camp_prod_hkey" VARCHAR(32)
	,"lds_mm_camp_prod_class_hkey" VARCHAR(32)
	,"lds_mm_camp_prod_class_trans_timestamp" TIMESTAMP
	,"lds_mm_camp_prod_emo_hkey" VARCHAR(32)
	,"lds_mm_camp_prod_emo_trans_timestamp" TIMESTAMP
   ,CONSTRAINT "pit_camp_prod_camp_prod_pk" PRIMARY KEY ("pit_camp_prod_camp_prod_hkey")   
   ,CONSTRAINT "pit_camp_prod_camp_prod_uk" UNIQUE ("lnd_camp_prod_hkey", "snapshot_timestamp")   
)
;

COMMENT ON TABLE "moto_scn01_bv"."pit_camp_prod_camp_prod" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/20 10:43:29';


CREATE   TABLE "moto_scn01_bv"."pit_contacts_contacts"
(
	 "pit_contacts_contacts_hkey" VARCHAR(32)
	,"snapshot_timestamp" TIMESTAMP
	,"load_cycle_id" INTEGER
	,"contacts_hkey" VARCHAR(32)
	,"sat_mm_contacts_hkey" VARCHAR(32)
	,"sat_mm_contacts_trans_timestamp" TIMESTAMP
	,"sat_mm_e_mails_hkey" VARCHAR(32)
	,"sat_mm_e_mails_trans_timestamp" TIMESTAMP
	,"sat_mm_phones_hkey" VARCHAR(32)
	,"sat_mm_phones_trans_timestamp" TIMESTAMP
   ,CONSTRAINT "pit_contacts_contacts_pk" PRIMARY KEY ("pit_contacts_contacts_hkey")   
   ,CONSTRAINT "pit_contacts_contacts_uk" UNIQUE ("contacts_hkey", "snapshot_timestamp")   
)
;

COMMENT ON TABLE "moto_scn01_bv"."pit_contacts_contacts" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/20 10:43:29';


CREATE   TABLE "moto_scn01_bv"."pit_location_cust_addr"
(
	 "pit_location_cust_addr_hkey" VARCHAR(32)
	,"snapshot_timestamp" TIMESTAMP
	,"load_cycle_id" INTEGER
	,"lnk_cust_addr_hkey" VARCHAR(32)
	,"lks_mm_cust_addr_hkey" VARCHAR(32)
	,"lks_mm_cust_addr_load_date" TIMESTAMP
   ,CONSTRAINT "pit_location_cust_addr_pk" PRIMARY KEY ("pit_location_cust_addr_hkey")   
   ,CONSTRAINT "pit_location_cust_addr_uk" UNIQUE ("lnk_cust_addr_hkey", "snapshot_timestamp")   
)
;

COMMENT ON TABLE "moto_scn01_bv"."pit_location_cust_addr" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/20 10:43:29';


CREATE   TABLE "moto_scn01_bv"."pit_prse_prse_prod"
(
	 "pit_prse_prse_prod_hkey" VARCHAR(32)
	,"snapshot_timestamp" TIMESTAMP
	,"load_cycle_id" INTEGER
	,"lnk_prse_prod_hkey" VARCHAR(32)
	,"lks_ms_prse_prod_hkey" VARCHAR(32)
	,"lks_ms_prse_prod_load_date" TIMESTAMP
   ,CONSTRAINT "pit_prse_prse_prod_pk" PRIMARY KEY ("pit_prse_prse_prod_hkey")   
   ,CONSTRAINT "pit_prse_prse_prod_uk" UNIQUE ("lnk_prse_prod_hkey", "snapshot_timestamp")   
)
;

COMMENT ON TABLE "moto_scn01_bv"."pit_prse_prse_prod" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/20 10:43:29';


CREATE   TABLE "moto_scn01_bv"."bridge_contact_address"
(
	 "load_date" TIMESTAMP
	,"load_cycle_id" INTEGER
	,"addresses_hkey" VARCHAR(32)
	,"customers_hkey" VARCHAR(32)
	,"contacts_hkey" VARCHAR(32)
	,"lnk_cust_addr_hkey" VARCHAR(32)
	,"lnd_cust_cont_hkey" VARCHAR(32)
	,"street_name_bk" VARCHAR(375)
	,"street_number_bk" VARCHAR(375)
	,"postal_code_bk" VARCHAR(375)
	,"city_bk" VARCHAR(375)
	,"customers_bk" VARCHAR
	,"contact_id_bk" VARCHAR(1500)
   ,CONSTRAINT "bridge_contact_address_uk" UNIQUE ("addresses_hkey","customers_hkey","contacts_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_bv"."bridge_contact_address" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/20 10:43:29';


CREATE   TABLE "moto_scn01_bv"."bridge_part_customer"
(
	 "bridge_part_customer_hkey" VARCHAR(32)
   ,CONSTRAINT "bridge_part_customer_pk" PRIMARY KEY ("bridge_part_customer_hkey")   
	,"load_date" TIMESTAMP
	,"load_cycle_id" INTEGER
	,"customers_hkey" VARCHAR(32)
	,"invoice_lines_hkey" VARCHAR(32)
	,"invoices_hkey" VARCHAR(32)
	,"parts_hkey" VARCHAR(32)
	,"lnk_inli_part_hkey" VARCHAR(32)
	,"lnk_inli_invo_hkey" VARCHAR(32)
	,"lnk_invo_cust_hkey" VARCHAR(32)
	,"customers_bk" VARCHAR
	,"inli_invoice_number_bk" VARCHAR(750)
	,"invoice_line_number_bk" VARCHAR(750)
	,"invo_invoice_number_bk" VARCHAR(1500)
	,"part_number_bk" VARCHAR(750)
	,"part_langua_code_bk" VARCHAR(750)
   ,CONSTRAINT "bridge_part_customer_uk" UNIQUE ("customers_hkey","invoice_lines_hkey","invoices_hkey","parts_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_bv"."bridge_part_customer" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/20 10:43:29';


CREATE   TABLE "moto_scn01_fmc"."fmc_bv_loading_window_table"
(
	"fmc_begin_lw_timestamp" TIMESTAMP,
	"fmc_end_lw_timestamp" TIMESTAMP
)
;

COMMENT ON TABLE "moto_scn01_fmc"."fmc_bv_loading_window_table" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/20 10:43:29';


CREATE   TABLE "moto_scn01_fmc"."load_cycle_info"
(
	"load_cycle_id" INTEGER,
	"load_date" TIMESTAMP
)
;

COMMENT ON TABLE "moto_scn01_fmc"."load_cycle_info" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/20 10:43:29';


CREATE   TABLE "moto_scn01_fmc"."dv_load_cycle_info"
(
	"dv_load_cycle_id" INTEGER
)
;

COMMENT ON TABLE "moto_scn01_fmc"."dv_load_cycle_info" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14, 
BV release: init(1) - Comment: initial release - Release date: 2025/01/20 10:43:29';


-- END


