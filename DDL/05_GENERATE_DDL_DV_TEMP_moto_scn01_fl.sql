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
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."lds_mm_camp_cust_cont_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."lds_mm_camp_moto_channel_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."lds_mm_camp_moto_chan_region_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."lds_mm_camp_prod_class_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."lds_mm_camp_prod_emo_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."lds_mm_cust_cont_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."lds_ms_cust_addresses_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."lds_ms_pro_fea_clas_rel_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."lks_mm_cust_addr_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."lks_mm_cust_cust_parentpartynumber_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."lks_ms_cust_addr_ciai_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."lks_ms_cust_addr_iats_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."lks_ms_inli_invo_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."lks_ms_inli_part_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."lks_ms_inli_prod_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."lks_ms_invo_cust_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."lks_ms_prfe_pfca_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."lks_ms_prod_prod_rpid_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."lks_ms_prse_prod_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."sat_mm_addresses_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."sat_mm_campaigns_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."sat_mm_channels_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."sat_mm_contacts_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."sat_mm_customers_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."sat_mm_e_mails_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."sat_mm_phones_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."sat_mm_products_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."sat_ms_addresses_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."sat_ms_customers_birth_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."sat_ms_customers_name_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."sat_ms_invoice_lines_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."sat_ms_invoices_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."sat_ms_parts_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."sat_ms_prod_featu_class_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."sat_ms_product_features_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."sat_ms_product_sensors_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."sat_ms_products_tmp" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."sat_ms_produ_featur_cat_tmp" 
CASCADE
;

-- END


/* CREATE TABLES */

-- START

CREATE   TABLE "moto_mktg_scn01_stg"."sat_mm_addresses_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."sat_mm_addresses_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_mktg_scn01_stg"."sat_mm_campaigns_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."sat_mm_campaigns_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_mktg_scn01_stg"."sat_mm_channels_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."sat_mm_channels_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_mktg_scn01_stg"."sat_mm_contacts_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."sat_mm_contacts_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_mktg_scn01_stg"."sat_mm_customers_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."sat_mm_customers_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_mktg_scn01_stg"."sat_mm_e_mails_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."sat_mm_e_mails_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_mktg_scn01_stg"."sat_mm_phones_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."sat_mm_phones_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_mktg_scn01_stg"."sat_mm_products_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."sat_mm_products_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."sat_ms_addresses_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."sat_ms_addresses_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."sat_ms_customers_birth_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."sat_ms_customers_birth_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."sat_ms_customers_name_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."sat_ms_customers_name_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."sat_ms_invoice_lines_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."sat_ms_invoice_lines_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."sat_ms_invoices_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."sat_ms_invoices_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."sat_ms_parts_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."sat_ms_parts_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."sat_ms_prod_featu_class_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."sat_ms_prod_featu_class_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."sat_ms_product_features_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."sat_ms_product_features_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."sat_ms_product_sensors_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."sat_ms_product_sensors_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."sat_ms_products_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."sat_ms_products_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."sat_ms_produ_featur_cat_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."sat_ms_produ_featur_cat_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_mktg_scn01_stg"."lks_mm_cust_addr_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."lks_mm_cust_addr_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_mktg_scn01_stg"."lks_mm_cust_cust_parentpartynumber_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."lks_mm_cust_cust_parentpartynumber_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."lks_ms_cust_addr_ciai_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."lks_ms_cust_addr_ciai_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."lks_ms_cust_addr_iats_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."lks_ms_cust_addr_iats_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."lks_ms_inli_invo_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."lks_ms_inli_invo_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."lks_ms_inli_part_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."lks_ms_inli_part_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."lks_ms_inli_prod_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."lks_ms_inli_prod_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."lks_ms_invo_cust_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."lks_ms_invo_cust_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."lks_ms_prfe_pfca_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."lks_ms_prfe_pfca_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."lks_ms_prod_prod_rpid_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."lks_ms_prod_prod_rpid_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."lks_ms_prse_prod_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."lks_ms_prse_prod_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_mktg_scn01_stg"."lds_mm_camp_cust_cont_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."lds_mm_camp_cust_cont_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_mktg_scn01_stg"."lds_mm_camp_moto_channel_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."lds_mm_camp_moto_channel_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_mktg_scn01_stg"."lds_mm_camp_moto_chan_region_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
   ,"hash_diff" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."lds_mm_camp_moto_chan_region_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_mktg_scn01_stg"."lds_mm_camp_prod_class_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."lds_mm_camp_prod_class_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_mktg_scn01_stg"."lds_mm_camp_prod_emo_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."lds_mm_camp_prod_emo_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_mktg_scn01_stg"."lds_mm_cust_cont_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."lds_mm_cust_cont_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."lds_ms_cust_addresses_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."lds_ms_cust_addresses_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."lds_ms_pro_fea_clas_rel_tmp"
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
   ,"source" VARCHAR
   ,"equal" NUMERIC
   ,"record_type" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."lds_ms_pro_fea_clas_rel_tmp" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


-- END


