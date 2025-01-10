/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.14, generation date: 2025/01/09 12:47:27
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36
 */

/* DROP TABLES */

-- START
DROP TABLE IF EXISTS "moto_scn01_fl"."nhl_ms_payments" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_bv"."las_mm_cust_addr_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_bv"."las_mm_cust_addr" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_bv"."lna_cust_addr" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."ref_ms_codes_to_languag" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lds_mm_camp_cust_cont" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lds_mm_camp_moto_channel" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lds_mm_camp_moto_chan_region" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lds_mm_camp_prod_class" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lds_mm_camp_prod_emo" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lds_mm_cust_cont" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lds_ms_cust_addresses" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lds_ms_pro_fea_clas_rel" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnd_camp_cust_cont" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnd_camp_moto_channel" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnd_camp_moto_chan_region" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnd_camp_prod" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnd_cust_addresses" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnd_cust_cont" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnd_pro_fea_clas_rel" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lks_mm_cust_addr" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lks_mm_cust_cust_parentpartynumber" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lks_ms_cust_addr_ciai" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lks_ms_cust_addr_iats" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lks_ms_inli_invo" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lks_ms_inli_part" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lks_ms_inli_prod" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lks_ms_invo_cust" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lks_ms_prfe_pfca" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lks_ms_prod_prod_rpid" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lks_ms_prse_prod" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnk_cust_addr" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnk_cust_addr_ciai" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnk_cust_addr_iats" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnk_cust_cust_parentpartynumber" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnk_inli_invo" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnk_inli_part" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnk_inli_prod" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnk_invo_cust" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnk_prfe_pfca" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnk_prod_prod_rpid" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."lnk_prse_prod" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_mm_addresses" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_mm_campaigns" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_mm_channels" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_mm_contacts" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_mm_customers" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_mm_e_mails" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_mm_phones" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_mm_products" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_ms_addresses" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_ms_customers_birth" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_ms_customers_name" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_ms_invoice_lines" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_ms_invoices" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_ms_parts" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_ms_prod_featu_class" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_ms_product_features" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_ms_products" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_ms_product_sensors" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."sat_ms_produ_featur_cat" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."hub_addresses" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."hub_campaigns" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."hub_channels" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."hub_contacts" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."hub_customers" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."hub_invoice_lines" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."hub_invoices" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."hub_parts" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."hub_prod_featu_class" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."hub_product_features" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."hub_products" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."hub_product_sensors" 
CASCADE
;
DROP TABLE IF EXISTS "moto_scn01_fl"."hub_produ_featur_cat" 
CASCADE
;

-- END


/* CREATE TABLES */

-- START

CREATE   TABLE "moto_scn01_fl"."hub_addresses"
(
    "addresses_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"street_name_bk" VARCHAR(375)
   ,"street_number_bk" VARCHAR(375)
   ,"postal_code_bk" VARCHAR(375)
   ,"city_bk" VARCHAR(375)
   ,CONSTRAINT "hub_addr_pk" PRIMARY KEY ("addresses_hkey")   
   ,CONSTRAINT "hub_addr_uk" UNIQUE ("street_name_bk", "street_number_bk", "postal_code_bk", "city_bk")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."hub_addresses" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."hub_campaigns"
(
    "campaigns_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"campaign_code_bk" VARCHAR(750)
   ,"campaign_start_date_bk" VARCHAR(750)
   ,CONSTRAINT "hub_campaigns_pk" PRIMARY KEY ("campaigns_hkey")   
   ,CONSTRAINT "hub_campaigns_uk" UNIQUE ("campaign_code_bk", "campaign_start_date_bk")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."hub_campaigns" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."hub_channels"
(
    "channels_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"channel_code_bk" VARCHAR(1500)
   ,CONSTRAINT "hub_channels_pk" PRIMARY KEY ("channels_hkey")   
   ,CONSTRAINT "hub_channels_uk" UNIQUE ("channel_code_bk")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."hub_channels" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."hub_contacts"
(
    "contacts_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"contact_id_bk" VARCHAR(1500)
   ,CONSTRAINT "hub_cont_pk" PRIMARY KEY ("contacts_hkey")   
   ,CONSTRAINT "hub_cont_uk" UNIQUE ("contact_id_bk")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."hub_contacts" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."hub_customers"
(
    "customers_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"src_bk" VARCHAR NOT NULL
   ,"customers_bk" VARCHAR NOT NULL
   ,CONSTRAINT "hub_cust_pk" PRIMARY KEY ("customers_hkey")   
   ,CONSTRAINT "hub_cust_uk" UNIQUE ("src_bk", "customers_bk")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."hub_customers" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."hub_invoice_lines"
(
    "invoice_lines_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"invoice_number_bk" VARCHAR(750)
   ,"invoice_line_number_bk" VARCHAR(750)
   ,CONSTRAINT "hub_inli_pk" PRIMARY KEY ("invoice_lines_hkey")   
   ,CONSTRAINT "hub_inli_uk" UNIQUE ("invoice_number_bk", "invoice_line_number_bk")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."hub_invoice_lines" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."hub_invoices"
(
    "invoices_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"invoice_number_bk" VARCHAR(1500)
   ,CONSTRAINT "hub_invo_pk" PRIMARY KEY ("invoices_hkey")   
   ,CONSTRAINT "hub_invo_uk" UNIQUE ("invoice_number_bk")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."hub_invoices" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."hub_parts"
(
    "parts_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"part_number_bk" VARCHAR(750)
   ,"part_langua_code_bk" VARCHAR(750)
   ,CONSTRAINT "hub_part_pk" PRIMARY KEY ("parts_hkey")   
   ,CONSTRAINT "hub_part_uk" UNIQUE ("part_number_bk", "part_langua_code_bk")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."hub_parts" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."hub_prod_featu_class"
(
    "prod_featu_class_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"pro_fea_cla_code_bk" VARCHAR(1500)
   ,CONSTRAINT "hub_prod_featu_class_pk" PRIMARY KEY ("prod_featu_class_hkey")   
   ,CONSTRAINT "hub_prod_featu_class_uk" UNIQUE ("pro_fea_cla_code_bk")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."hub_prod_featu_class" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."hub_product_features"
(
    "product_features_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"produ_featu_code_bk" VARCHAR(1500)
   ,CONSTRAINT "hub_prfe_pk" PRIMARY KEY ("product_features_hkey")   
   ,CONSTRAINT "hub_prfe_uk" UNIQUE ("produ_featu_code_bk")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."hub_product_features" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."hub_products"
(
    "products_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"src_bk" VARCHAR NOT NULL
   ,"product_cc_bk" VARCHAR(500)
   ,"product_et_code_bk" VARCHAR(500)
   ,"product_part_code_bk" VARCHAR(500)
   ,CONSTRAINT "hub_prod_pk" PRIMARY KEY ("products_hkey")   
   ,CONSTRAINT "hub_prod_uk" UNIQUE ("src_bk", "product_cc_bk", "product_et_code_bk", "product_part_code_bk")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."hub_products" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."hub_product_sensors"
(
    "product_sensors_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"vehicle_number_bk" VARCHAR(1500)
   ,CONSTRAINT "hub_prse_pk" PRIMARY KEY ("product_sensors_hkey")   
   ,CONSTRAINT "hub_prse_uk" UNIQUE ("vehicle_number_bk")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."hub_product_sensors" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."hub_produ_featur_cat"
(
    "produ_featur_cat_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"pro_fea_cat_code_bk" VARCHAR(1500)
   ,CONSTRAINT "hub_pfca_pk" PRIMARY KEY ("produ_featur_cat_hkey")   
   ,CONSTRAINT "hub_pfca_uk" UNIQUE ("pro_fea_cat_code_bk")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."hub_produ_featur_cat" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_mm_addresses"
(
    "addresses_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"province" VARCHAR
   ,"street_number" NUMERIC(16)
   ,"update_timestamp" TIMESTAMP(6)
   ,"city" VARCHAR
   ,"postal_code" VARCHAR
   ,"street_name" VARCHAR
   ,"address_number" NUMERIC(16)
   ,CONSTRAINT "sat_mm_addr_uk" UNIQUE ("addresses_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_mm_addresses" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_mm_campaigns"
(
    "campaigns_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"campaign_code" VARCHAR
   ,"campaign_name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"campaign_start_date" DATE
   ,CONSTRAINT "sat_mm_campaigns_uk" UNIQUE ("campaigns_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_mm_campaigns" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_mm_channels"
(
    "channels_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"channel_id" NUMERIC(16)
   ,"update_timestamp" TIMESTAMP(6)
   ,"channel_description" VARCHAR
   ,CONSTRAINT "sat_mm_channels_uk" UNIQUE ("channels_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_mm_channels" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_mm_contacts"
(
    "contacts_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"contact_id" NUMERIC(16)
   ,"contact_type_desc" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"contact_type" VARCHAR
   ,CONSTRAINT "sat_mm_cont_uk" UNIQUE ("contacts_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_mm_contacts" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_mm_customers"
(
    "customers_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"comments" VARCHAR
   ,"party_number" NUMERIC(16)
   ,"birthdate" DATE
   ,"name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"parent_party_number" NUMERIC(16)
   ,"address_number" NUMERIC(16)
   ,"gender" CHARACTER(24)
   ,"party_type_code" CHARACTER(6)
   ,CONSTRAINT "sat_mm_cust_uk" UNIQUE ("customers_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_mm_customers" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_mm_e_mails"
(
    "contacts_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"contact_id" NUMERIC(16)
   ,"name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,CONSTRAINT "sat_mm_emails_uk" UNIQUE ("contacts_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_mm_e_mails" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_mm_phones"
(
    "contacts_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"contact_id" NUMERIC(16)
   ,"phone_number" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,CONSTRAINT "sat_mm_phones_uk" UNIQUE ("contacts_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_mm_phones" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_mm_products"
(
    "products_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"motorcycle_name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"motorcycle_id" NUMERIC(16)
   ,CONSTRAINT "sat_mm_prod_uk" UNIQUE ("products_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_mm_products" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_ms_addresses"
(
    "addresses_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"street_name" VARCHAR
   ,"street_number" NUMERIC(16)
   ,"postal_code" VARCHAR
   ,"city" VARCHAR
   ,"address_number" NUMERIC(16)
   ,"coordinates" gps
   ,"update_timestamp" TIMESTAMP(6)
   ,CONSTRAINT "sat_ms_addr_uk" UNIQUE ("addresses_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_ms_addresses" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_ms_customers_birth"
(
    "customers_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"national_person_id" VARCHAR
   ,"customer_number" NUMERIC(16)
   ,"birthdate" DATE
   ,"update_timestamp" TIMESTAMP(6)
   ,CONSTRAINT "sat_ms_cust_birth_uk" UNIQUE ("customers_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_ms_customers_birth" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_ms_customers_name"
(
    "customers_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"national_person_id" VARCHAR
   ,"customer_number" NUMERIC(16)
   ,"first_name" VARCHAR
   ,"last_name" VARCHAR
   ,"gender" CHARACTER(24)
   ,"update_timestamp" TIMESTAMP(6)
   ,CONSTRAINT "sat_ms_cust_name_uk" UNIQUE ("customers_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_ms_customers_name" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_ms_invoice_lines"
(
    "invoice_lines_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"invoice_line_number" NUMERIC(16)
   ,"invoice_number" NUMERIC(16)
   ,"amount" NUMERIC(14, 2)
   ,"quantity" NUMERIC(13)
   ,"unit_price" NUMERIC(14, 2)
   ,"update_timestamp" TIMESTAMP(6)
   ,CONSTRAINT "sat_ms_inli_uk" UNIQUE ("invoice_lines_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_ms_invoice_lines" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_ms_invoices"
(
    "invoices_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"invoice_number" NUMERIC(16)
   ,"invoice_date" DATE
   ,"amount" NUMERIC(14, 2)
   ,"discount" INTEGER
   ,"update_timestamp" TIMESTAMP(6)
   ,CONSTRAINT "sat_ms_invo_uk" UNIQUE ("invoices_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_ms_invoices" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_ms_parts"
(
    "parts_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"part_language_code" VARCHAR
   ,"part_id" NUMERIC(16)
   ,"ref_part_number_fk" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"ref_part_language_code_fk" VARCHAR
   ,"part_number" VARCHAR
   ,CONSTRAINT "sat_ms_part_uk" UNIQUE ("parts_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_ms_parts" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_ms_prod_featu_class"
(
    "prod_featu_class_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"product_feature_class_id" NUMERIC(16)
   ,"update_timestamp" TIMESTAMP(6)
   ,"product_feature_class_desc" VARCHAR
   ,CONSTRAINT "sat_ms_prod_featu_class_uk" UNIQUE ("prod_featu_class_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_ms_prod_featu_class" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_ms_product_features"
(
    "product_features_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"product_feature_language_code" VARCHAR
   ,"pro_fea_lan_code_seq" VARCHAR NOT NULL
   ,"product_feature_description" VARCHAR
   ,"product_feature_id" INTEGER
   ,"update_timestamp" TIMESTAMP(6)
   ,"product_feature_code" VARCHAR
   ,CONSTRAINT "sat_ms_prfe_uk" UNIQUE ("product_features_hkey", "load_date", "pro_fea_lan_code_seq")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_ms_product_features" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_ms_products"
(
    "products_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"product_intro_date" DATE
   ,"update_timestamp" TIMESTAMP(6)
   ,"product_name" VARCHAR
   ,"product_id" NUMERIC(16)
   ,CONSTRAINT "sat_ms_prod_uk" UNIQUE ("products_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_ms_products" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_ms_product_sensors"
(
    "product_sensors_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"subsequence_seq" INTEGER NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"sensor_value" NUMERIC(14, 2)
   ,"sensor" VARCHAR
   ,"unit_of_measurement" VARCHAR
   ,"vehicle_number" VARCHAR
   ,CONSTRAINT "sat_ms_prse_uk" UNIQUE ("product_sensors_hkey", "load_date", "subsequence_seq")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_ms_product_sensors" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."sat_ms_produ_featur_cat"
(
    "produ_featur_cat_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"product_feature_category_id" INTEGER
   ,"prod_feat_cat_description" VARCHAR
   ,"prod_feat_cat_language_code" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"pr_fe_ca_lan_cod_seq" VARCHAR NOT NULL
   ,"product_feature_category_code" VARCHAR
   ,CONSTRAINT "sat_ms_pfca_uk" UNIQUE ("produ_featur_cat_hkey", "load_date", "pr_fe_ca_lan_cod_seq")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."sat_ms_produ_featur_cat" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnk_cust_addr"
(
    "lnk_cust_addr_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"addresses_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnk_cust_addr_pk" PRIMARY KEY ("lnk_cust_addr_hkey")   
   ,CONSTRAINT "lnk_cust_addr_uk" UNIQUE ("customers_hkey", "addresses_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnk_cust_addr" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnk_cust_addr_ciai"
(
    "lnk_cust_addr_ciai_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"addresses_ciai_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnk_cust_addr_ciai_pk" PRIMARY KEY ("lnk_cust_addr_ciai_hkey")   
   ,CONSTRAINT "lnk_cust_addr_ciai_uk" UNIQUE ("customers_hkey", "addresses_ciai_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnk_cust_addr_ciai" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnk_cust_addr_iats"
(
    "lnk_cust_addr_iats_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"addresses_iats_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnk_cust_addr_iats_pk" PRIMARY KEY ("lnk_cust_addr_iats_hkey")   
   ,CONSTRAINT "lnk_cust_addr_iats_uk" UNIQUE ("customers_hkey", "addresses_iats_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnk_cust_addr_iats" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnk_cust_cust_parentpartynumber"
(
    "lnk_cust_cust_parentpartynumber_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"customers_parentpartynumber_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnk_cust_cust_parentpartynumber_pk" PRIMARY KEY ("lnk_cust_cust_parentpartynumber_hkey")   
   ,CONSTRAINT "lnk_cust_cust_parentpartynumber_uk" UNIQUE ("customers_hkey", "customers_parentpartynumber_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnk_cust_cust_parentpartynumber" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnk_inli_invo"
(
    "lnk_inli_invo_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"invoice_lines_hkey" VARCHAR(32) NOT NULL
   ,"invoices_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnk_inli_invo_pk" PRIMARY KEY ("lnk_inli_invo_hkey")   
   ,CONSTRAINT "lnk_inli_invo_uk" UNIQUE ("invoice_lines_hkey", "invoices_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnk_inli_invo" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnk_inli_part"
(
    "lnk_inli_part_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"invoice_lines_hkey" VARCHAR(32) NOT NULL
   ,"parts_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnk_inli_part_pk" PRIMARY KEY ("lnk_inli_part_hkey")   
   ,CONSTRAINT "lnk_inli_part_uk" UNIQUE ("invoice_lines_hkey", "parts_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnk_inli_part" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnk_inli_prod"
(
    "lnk_inli_prod_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"invoice_lines_hkey" VARCHAR(32) NOT NULL
   ,"products_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnk_inli_prod_pk" PRIMARY KEY ("lnk_inli_prod_hkey")   
   ,CONSTRAINT "lnk_inli_prod_uk" UNIQUE ("invoice_lines_hkey", "products_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnk_inli_prod" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnk_invo_cust"
(
    "lnk_invo_cust_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"invoices_hkey" VARCHAR(32) NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnk_invo_cust_pk" PRIMARY KEY ("lnk_invo_cust_hkey")   
   ,CONSTRAINT "lnk_invo_cust_uk" UNIQUE ("invoices_hkey", "customers_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnk_invo_cust" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnk_prfe_pfca"
(
    "lnk_prfe_pfca_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"product_features_hkey" VARCHAR(32) NOT NULL
   ,"produ_featur_cat_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnk_prfe_pfca_pk" PRIMARY KEY ("lnk_prfe_pfca_hkey")   
   ,CONSTRAINT "lnk_prfe_pfca_uk" UNIQUE ("product_features_hkey", "produ_featur_cat_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnk_prfe_pfca" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnk_prod_prod_rpid"
(
    "lnk_prod_prod_rpid_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"products_hkey" VARCHAR(32) NOT NULL
   ,"products_rpid_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnk_prod_prod_rpid_pk" PRIMARY KEY ("lnk_prod_prod_rpid_hkey")   
   ,CONSTRAINT "lnk_prod_prod_rpid_uk" UNIQUE ("products_hkey", "products_rpid_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnk_prod_prod_rpid" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnk_prse_prod"
(
    "lnk_prse_prod_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"product_sensors_hkey" VARCHAR(32) NOT NULL
   ,"products_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnk_prse_prod_pk" PRIMARY KEY ("lnk_prse_prod_hkey")   
   ,CONSTRAINT "lnk_prse_prod_uk" UNIQUE ("product_sensors_hkey", "products_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnk_prse_prod" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lks_mm_cust_addr"
(
    "lnk_cust_addr_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"addresses_hkey" VARCHAR(32) NOT NULL
   ,"party_number" NUMERIC(16)
   ,"address_number" NUMERIC(16)
   ,CONSTRAINT "lks_mm_cust_addr_uk" UNIQUE ("lnk_cust_addr_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lks_mm_cust_addr" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lks_mm_cust_cust_parentpartynumber"
(
    "lnk_cust_cust_parentpartynumber_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"customers_parentpartynumber_hkey" VARCHAR(32) NOT NULL
   ,"party_number" NUMERIC(16)
   ,"parent_party_number" NUMERIC(16)
   ,CONSTRAINT "lks_mm_cust_cust_parentpartynumber_uk" UNIQUE ("lnk_cust_cust_parentpartynumber_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lks_mm_cust_cust_parentpartynumber" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lks_ms_cust_addr_ciai"
(
    "lnk_cust_addr_ciai_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"addresses_ciai_hkey" VARCHAR(32) NOT NULL
   ,"customer_invoice_address_id" NUMERIC(16)
   ,"customer_number" NUMERIC(16)
   ,CONSTRAINT "lks_ms_cust_addr_ciai_uk" UNIQUE ("lnk_cust_addr_ciai_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lks_ms_cust_addr_ciai" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lks_ms_cust_addr_iats"
(
    "lnk_cust_addr_iats_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"addresses_iats_hkey" VARCHAR(32) NOT NULL
   ,"customer_number" NUMERIC(16)
   ,"customer_ship_to_address_id" NUMERIC(16)
   ,CONSTRAINT "lks_ms_cust_addr_iats_uk" UNIQUE ("lnk_cust_addr_iats_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lks_ms_cust_addr_iats" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lks_ms_inli_invo"
(
    "lnk_inli_invo_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"invoice_lines_hkey" VARCHAR(32) NOT NULL
   ,"invoices_hkey" VARCHAR(32) NOT NULL
   ,"invoice_line_number" NUMERIC(16)
   ,"invoice_number" NUMERIC(16)
   ,CONSTRAINT "lks_ms_inli_invo_uk" UNIQUE ("lnk_inli_invo_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lks_ms_inli_invo" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lks_ms_inli_part"
(
    "lnk_inli_part_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"invoice_lines_hkey" VARCHAR(32) NOT NULL
   ,"parts_hkey" VARCHAR(32) NOT NULL
   ,"invoice_line_number" NUMERIC(16)
   ,"invoice_number" NUMERIC(16)
   ,"part_id" NUMERIC(16)
   ,CONSTRAINT "lks_ms_inli_part_uk" UNIQUE ("lnk_inli_part_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lks_ms_inli_part" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lks_ms_inli_prod"
(
    "lnk_inli_prod_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"invoice_lines_hkey" VARCHAR(32) NOT NULL
   ,"products_hkey" VARCHAR(32) NOT NULL
   ,"invoice_line_number" NUMERIC(16)
   ,"invoice_number" NUMERIC(16)
   ,"product_id" NUMERIC(16)
   ,CONSTRAINT "lks_ms_inli_prod_uk" UNIQUE ("lnk_inli_prod_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lks_ms_inli_prod" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lks_ms_invo_cust"
(
    "lnk_invo_cust_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"invoices_hkey" VARCHAR(32) NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"invoice_number" NUMERIC(16)
   ,"invoice_customer_id" NUMERIC(16)
   ,CONSTRAINT "lks_ms_invo_cust_uk" UNIQUE ("lnk_invo_cust_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lks_ms_invo_cust" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lks_ms_prfe_pfca"
(
    "lnk_prfe_pfca_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"product_features_hkey" VARCHAR(32) NOT NULL
   ,"produ_featur_cat_hkey" VARCHAR(32) NOT NULL
   ,"product_feature_cat_id" INTEGER
   ,"product_feature_id" INTEGER
   ,CONSTRAINT "lks_ms_prfe_pfca_uk" UNIQUE ("lnk_prfe_pfca_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lks_ms_prfe_pfca" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lks_ms_prod_prod_rpid"
(
    "lnk_prod_prod_rpid_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"products_hkey" VARCHAR(32) NOT NULL
   ,"products_rpid_hkey" VARCHAR(32) NOT NULL
   ,"replacement_product_id" NUMERIC(16)
   ,"product_id" NUMERIC(16)
   ,CONSTRAINT "lks_ms_prod_prod_rpid_uk" UNIQUE ("lnk_prod_prod_rpid_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lks_ms_prod_prod_rpid" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lks_ms_prse_prod"
(
    "lnk_prse_prod_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"product_sensors_hkey" VARCHAR(32) NOT NULL
   ,"products_hkey" VARCHAR(32) NOT NULL
   ,"vehicle_number" VARCHAR
   ,"product_number" NUMERIC(16)
   ,CONSTRAINT "lks_ms_prse_prod_uk" UNIQUE ("lnk_prse_prod_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lks_ms_prse_prod" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnd_camp_cust_cont"
(
    "lnd_camp_cust_cont_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"contacts_hkey" VARCHAR(32) NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"campaigns_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnd_camp_cust_cont_pk" PRIMARY KEY ("lnd_camp_cust_cont_hkey")   
   ,CONSTRAINT "lnd_camp_cust_cont_uk" UNIQUE ("contacts_hkey", "customers_hkey", "campaigns_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnd_camp_cust_cont" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnd_camp_moto_channel"
(
    "lnd_camp_moto_channel_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"channels_hkey" VARCHAR(32) NOT NULL
   ,"campaigns_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnd_campmotochannel_pk" PRIMARY KEY ("lnd_camp_moto_channel_hkey")   
   ,CONSTRAINT "lnd_campmotochannel_uk" UNIQUE ("channels_hkey", "campaigns_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnd_camp_moto_channel" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnd_camp_moto_chan_region"
(
    "lnd_camp_moto_chan_region_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"channels_hkey" VARCHAR(32) NOT NULL
   ,"products_hkey" VARCHAR(32) NOT NULL
   ,"campaigns_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnd_campmotochanregion_pk" PRIMARY KEY ("lnd_camp_moto_chan_region_hkey")   
   ,CONSTRAINT "lnd_campmotochanregion_uk" UNIQUE ("channels_hkey", "products_hkey", "campaigns_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnd_camp_moto_chan_region" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnd_camp_prod"
(
    "lnd_camp_prod_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"products_hkey" VARCHAR(32) NOT NULL
   ,"campaigns_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnd_camp_prod_pk" PRIMARY KEY ("lnd_camp_prod_hkey")   
   ,CONSTRAINT "lnd_camp_prod_uk" UNIQUE ("products_hkey", "campaigns_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnd_camp_prod" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnd_cust_addresses"
(
    "lnd_cust_addresses_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"addresses_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnd_custaddresses_pk" PRIMARY KEY ("lnd_cust_addresses_hkey")   
   ,CONSTRAINT "lnd_custaddresses_uk" UNIQUE ("customers_hkey", "addresses_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnd_cust_addresses" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnd_cust_cont"
(
    "lnd_cust_cont_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"contacts_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnd_cust_cont_pk" PRIMARY KEY ("lnd_cust_cont_hkey")   
   ,CONSTRAINT "lnd_cust_cont_uk" UNIQUE ("customers_hkey", "contacts_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnd_cust_cont" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lnd_pro_fea_clas_rel"
(
    "lnd_pro_fea_clas_rel_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"products_hkey" VARCHAR(32) NOT NULL
   ,"prod_featu_class_hkey" VARCHAR(32) NOT NULL
   ,"product_features_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lnd_pro_fea_clas_rel_pk" PRIMARY KEY ("lnd_pro_fea_clas_rel_hkey")   
   ,CONSTRAINT "lnd_pro_fea_clas_rel_uk" UNIQUE ("products_hkey", "prod_featu_class_hkey", "product_features_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lnd_pro_fea_clas_rel" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lds_mm_camp_cust_cont"
(
    "lnd_camp_cust_cont_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"contacts_hkey" VARCHAR(32) NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"campaigns_hkey" VARCHAR(32) NOT NULL
   ,"contact_id" NUMERIC(16)
   ,"party_number" NUMERIC(16)
   ,"campaign_code" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"campaign_start_date" DATE
   ,"marketing_program_name" CHARACTER(300)
   ,"marketing_program_code" CHARACTER(30)
   ,CONSTRAINT "lds_mm_camp_cust_cont_uk" UNIQUE ("lnd_camp_cust_cont_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lds_mm_camp_cust_cont" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lds_mm_camp_moto_channel"
(
    "lnd_camp_moto_channel_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"channels_hkey" VARCHAR(32) NOT NULL
   ,"campaigns_hkey" VARCHAR(32) NOT NULL
   ,"motorcycle_name" VARCHAR
   ,"from_date" TIMESTAMP(6)
   ,"valid_from_date" TIMESTAMP(6)
   ,"to_date" TIMESTAMP(6)
   ,"campaign_code" VARCHAR
   ,"channel_id" NUMERIC(16)
   ,"update_timestamp" TIMESTAMP(6)
   ,"campaign_start_date" DATE
   ,"valid_to_date" TIMESTAMP(6)
   ,"from_date_seq" TIMESTAMP(6) NOT NULL
   ,CONSTRAINT "lds_mm_campmotochannel_uk" UNIQUE ("lnd_camp_moto_channel_hkey", "from_date_seq", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lds_mm_camp_moto_channel" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lds_mm_camp_moto_chan_region"
(
    "lnd_camp_moto_chan_region_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"channels_hkey" VARCHAR(32) NOT NULL
   ,"products_hkey" VARCHAR(32) NOT NULL
   ,"campaigns_hkey" VARCHAR(32) NOT NULL
   ,"campaign_code" VARCHAR
   ,"region_seq" VARCHAR NOT NULL
   ,"channel_id" NUMERIC(16)
   ,"update_timestamp" TIMESTAMP(6)
   ,"campaign_start_date" DATE
   ,"motorcycle_id" NUMERIC(16)
   ,CONSTRAINT "lds_mm_campmotochanregion_uk" UNIQUE ("lnd_camp_moto_chan_region_hkey", "region_seq", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lds_mm_camp_moto_chan_region" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lds_mm_camp_prod_class"
(
    "lnd_camp_prod_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"products_hkey" VARCHAR(32) NOT NULL
   ,"campaigns_hkey" VARCHAR(32) NOT NULL
   ,"campaign_code" VARCHAR
   ,"motorcycle_class_desc" VARCHAR
   ,"motorcycle_subclass_desc" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"campaign_start_date" DATE
   ,"motorcycle_id" NUMERIC(16)
   ,CONSTRAINT "lds_mm_camp_prod_class_uk" UNIQUE ("lnd_camp_prod_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lds_mm_camp_prod_class" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lds_mm_camp_prod_emo"
(
    "lnd_camp_prod_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"products_hkey" VARCHAR(32) NOT NULL
   ,"campaigns_hkey" VARCHAR(32) NOT NULL
   ,"motorcycle_emotion_desc" VARCHAR
   ,"campaign_code" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"campaign_start_date" DATE
   ,"motorcycle_comment" VARCHAR
   ,"motorcycle_id" NUMERIC(16)
   ,CONSTRAINT "lds_mm_camp_prod_emo_uk" UNIQUE ("lnd_camp_prod_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lds_mm_camp_prod_emo" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lds_mm_cust_cont"
(
    "lnd_cust_cont_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"contacts_hkey" VARCHAR(32) NOT NULL
   ,"contact_id" NUMERIC(16)
   ,"party_number" NUMERIC(16)
   ,"update_timestamp" TIMESTAMP(6)
   ,CONSTRAINT "lds_mm_cust_cont_uk" UNIQUE ("lnd_cust_cont_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lds_mm_cust_cont" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lds_ms_cust_addresses"
(
    "lnd_cust_addresses_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"hash_diff" VARCHAR(32)
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"addresses_hkey" VARCHAR(32) NOT NULL
   ,"customer_number" NUMERIC(16)
   ,"update_timestamp" TIMESTAMP(6)
   ,"address_number" NUMERIC(16)
   ,"address_type_seq" VARCHAR NOT NULL
   ,CONSTRAINT "lds_ms_custaddresses_uk" UNIQUE ("lnd_cust_addresses_hkey", "address_type_seq", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lds_ms_cust_addresses" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."lds_ms_pro_fea_clas_rel"
(
    "lnd_pro_fea_clas_rel_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"products_hkey" VARCHAR(32) NOT NULL
   ,"prod_featu_class_hkey" VARCHAR(32) NOT NULL
   ,"product_features_hkey" VARCHAR(32) NOT NULL
   ,"product_feature_id" INTEGER
   ,"product_feature_class_id" NUMERIC(16)
   ,"update_timestamp" TIMESTAMP(6)
   ,"product_id" NUMERIC(16)
   ,CONSTRAINT "lds_ms_pro_fea_clas_rel_uk" UNIQUE ("lnd_pro_fea_clas_rel_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."lds_ms_pro_fea_clas_rel" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."ref_ms_codes_to_languag"
(
    "load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"code" VARCHAR NOT NULL
   ,"language_code" VARCHAR NOT NULL
   ,"description" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,CONSTRAINT "ms_codes_to_languag_pk" PRIMARY KEY ("code", "language_code")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."ref_ms_codes_to_languag" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_bv"."lna_cust_addr"
(
    "lna_cust_addr_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"addresses_hkey" VARCHAR(32) NOT NULL
   ,CONSTRAINT "lna_cust_addr_pk" PRIMARY KEY ("lna_cust_addr_hkey")   
   ,CONSTRAINT "lna_cust_addr_uk" UNIQUE ("customers_hkey", "addresses_hkey")   
)
;

COMMENT ON TABLE "moto_scn01_bv"."lna_cust_addr" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_bv"."las_mm_cust_addr"
(
    "lna_cust_addr_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"addresses_hkey" VARCHAR(32) NOT NULL
   ,"address_number" NUMERIC(16)
   ,"party_number" NUMERIC(16) NOT NULL
   ,CONSTRAINT "las_mm_cust_addr_uk" UNIQUE ("lna_cust_addr_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_bv"."las_mm_cust_addr" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_bv"."las_mm_cust_addr_tmp"
(
    "lna_cust_addr_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_end_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"delete_flag" VARCHAR(4) NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"addresses_hkey" VARCHAR(32) NOT NULL
   ,"address_number" NUMERIC(16)
   ,"party_number" NUMERIC(16) NOT NULL
   ,"source" VARCHAR
   ,"equal" NUMERIC
)
;

COMMENT ON TABLE "moto_scn01_bv"."las_mm_cust_addr_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_scn01_fl"."nhl_ms_payments"
(
    "nhl_payments_hkey" VARCHAR(32) NOT NULL
   ,"customers_hkey" VARCHAR(32) NOT NULL
   ,"invoices_hkey" VARCHAR(32) NOT NULL
   ,"load_date" TIMESTAMP NOT NULL
   ,"load_cycle_id" INTEGER NOT NULL
   ,"trans_timestamp" TIMESTAMP NOT NULL
   ,"invoice_number" NUMERIC(16)
   ,"transaction_id" VARCHAR
   ,"customer_number" NUMERIC(16)
   ,"update_timestamp" TIMESTAMP(6)
   ,"date_time" TIMESTAMP(6)
   ,"amount" NUMERIC(14, 2)
   ,CONSTRAINT "nhl_payments_pk" PRIMARY KEY ("nhl_payments_hkey", "load_date")   
)
;

COMMENT ON TABLE "moto_scn01_fl"."nhl_ms_payments" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


-- END


