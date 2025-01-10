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

DROP TABLE IF EXISTS "moto_sales_scn01_stg"."addresses" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."cust_addresses" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."customers" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."invoice_lines" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."invoices" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."parts" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."payments" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."product_feat_class_rel" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."product_feature_cat" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."product_feature_class" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."product_features" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."products" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_stg"."product_sensors" 
CASCADE
;
-- END


/* CREATE TABLES */

-- START


CREATE   TABLE "moto_sales_scn01_stg"."addresses"
(
    "addresses_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"street_name_bk" VARCHAR(375)
   ,"street_number_bk" VARCHAR(375)
   ,"postal_code_bk" VARCHAR(375)
   ,"city_bk" VARCHAR(375)
   ,"address_number" NUMERIC(16)
   ,"street_name" VARCHAR
   ,"street_number" NUMERIC(16)
   ,"postal_code" VARCHAR
   ,"city" VARCHAR
   ,"coordinates" gps
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."addresses" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."cust_addresses"
(
    "lnd_cust_addresses_hkey" VARCHAR(32)
   ,"customers_hkey" VARCHAR(32)
   ,"addresses_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"street_number_fk_addressnumber_bk" VARCHAR(375)
   ,"street_name_fk_addressnumber_bk" VARCHAR(375)
   ,"postal_code_fk_addressnumber_bk" VARCHAR(375)
   ,"city_fk_addressnumber_bk" VARCHAR(375)
   ,"national_person_id_fk_customernumber_bk" VARCHAR(1500)
   ,"error_code_cust_addresses" INTEGER
   ,"customer_number" NUMERIC(16)
   ,"address_number" NUMERIC(16)
   ,"address_type_seq" VARCHAR(4)
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."cust_addresses" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."customers"
(
    "customers_hkey" VARCHAR(32)
   ,"lnk_cust_addr_ciai_hkey" VARCHAR(32)
   ,"lnk_cust_addr_iats_hkey" VARCHAR(32)
   ,"addresses_ciai_hkey" VARCHAR(32)
   ,"addresses_iats_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"src_bk" VARCHAR
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"customers_bk" VARCHAR
   ,"national_person_id_bk" VARCHAR(1500)
   ,"city_fk_customerinvoiceaddressid_bk" VARCHAR(375)
   ,"street_name_fk_customerinvoiceaddressid_bk" VARCHAR(375)
   ,"street_number_fk_customerinvoiceaddressid_bk" VARCHAR(375)
   ,"postal_code_fk_customerinvoiceaddressid_bk" VARCHAR(375)
   ,"street_name_fk_customershiptoaddressid_bk" VARCHAR(375)
   ,"street_number_fk_customershiptoaddressid_bk" VARCHAR(375)
   ,"postal_code_fk_customershiptoaddressid_bk" VARCHAR(375)
   ,"city_fk_customershiptoaddressid_bk" VARCHAR(375)
   ,"error_code_cust_addr_ciai" INTEGER
   ,"error_code_cust_addr_iats" INTEGER
   ,"customer_number" NUMERIC(16)
   ,"customer_invoice_address_id" NUMERIC(16)
   ,"customer_ship_to_address_id" NUMERIC(16)
   ,"national_person_id" VARCHAR
   ,"first_name" VARCHAR
   ,"last_name" VARCHAR
   ,"birthdate" DATE
   ,"gender" CHARACTER(24)
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."customers" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."invoice_lines"
(
    "invoice_lines_hkey" VARCHAR(32)
   ,"lnk_inli_invo_hkey" VARCHAR(32)
   ,"lnk_inli_part_hkey" VARCHAR(32)
   ,"lnk_inli_prod_hkey" VARCHAR(32)
   ,"products_hkey" VARCHAR(32)
   ,"parts_hkey" VARCHAR(32)
   ,"invoices_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"invoice_number_bk" VARCHAR(750)
   ,"invoice_line_number_bk" VARCHAR(750)
   ,"part_number_fk_partid_bk" VARCHAR(750)
   ,"invoice_number_fk_invoicenumber_bk" VARCHAR(1500)
   ,"part_langua_code_fk_partid_bk" VARCHAR(750)
   ,"product_cc_fk_productid_bk" VARCHAR(500)
   ,"product_et_code_fk_productid_bk" VARCHAR(500)
   ,"product_part_code_fk_productid_bk" VARCHAR(500)
   ,"error_code_inli_part" INTEGER
   ,"error_code_inli_prod" INTEGER
   ,"invoice_line_number" NUMERIC(16)
   ,"invoice_number" NUMERIC(16)
   ,"product_id" NUMERIC(16)
   ,"part_id" NUMERIC(16)
   ,"amount" NUMERIC(14, 2)
   ,"quantity" NUMERIC(13)
   ,"unit_price" NUMERIC(14, 2)
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."invoice_lines" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."invoices"
(
    "invoices_hkey" VARCHAR(32)
   ,"lnk_invo_cust_hkey" VARCHAR(32)
   ,"customers_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"invoice_number_bk" VARCHAR(1500)
   ,"national_person_id_fk_invoicecustomerid_bk" VARCHAR(1500)
   ,"error_code_invo_cust" INTEGER
   ,"invoice_number" NUMERIC(16)
   ,"invoice_customer_id" NUMERIC(16)
   ,"invoice_date" DATE
   ,"amount" NUMERIC(14, 2)
   ,"discount" INTEGER
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."invoices" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."parts"
(
    "parts_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"part_number_bk" VARCHAR(750)
   ,"part_langua_code_bk" VARCHAR(750)
   ,"part_id" NUMERIC(16)
   ,"ref_part_number_fk" VARCHAR
   ,"ref_part_language_code_fk" VARCHAR
   ,"part_number" VARCHAR
   ,"part_language_code" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."parts" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."payments"
(
    "nhl_payments_hkey" VARCHAR(32)
   ,"customers_hkey" VARCHAR(32)
   ,"invoices_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"invoice_number_fk_invoicenumber_bk" VARCHAR(1500)
   ,"national_person_id_fk_customernumber_bk" VARCHAR(1500)
   ,"error_code_payments" INTEGER
   ,"transaction_id" VARCHAR
   ,"invoice_number" NUMERIC(16)
   ,"customer_number" NUMERIC(16)
   ,"date_time" TIMESTAMP(6)
   ,"amount" NUMERIC(14, 2)
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."payments" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."product_feat_class_rel"
(
    "lnd_pro_fea_clas_rel_hkey" VARCHAR(32)
   ,"products_hkey" VARCHAR(32)
   ,"prod_featu_class_hkey" VARCHAR(32)
   ,"product_features_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"pro_fea_cla_code_fk_productfeatureclassid_bk" VARCHAR(1500)
   ,"produ_featu_code_fk_productfeatureid_bk" VARCHAR(1500)
   ,"product_cc_fk_productid_bk" VARCHAR(500)
   ,"product_et_code_fk_productid_bk" VARCHAR(500)
   ,"product_part_code_fk_productid_bk" VARCHAR(500)
   ,"error_code_pro_fea_clas_rel" INTEGER
   ,"product_feature_id" INTEGER
   ,"product_id" NUMERIC(16)
   ,"product_feature_class_id" NUMERIC(16)
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."product_feat_class_rel" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."product_feature_cat"
(
    "produ_featur_cat_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"pro_fea_cat_code_bk" VARCHAR(1500)
   ,"product_feature_category_id" INTEGER
   ,"product_feature_category_code" VARCHAR
   ,"pr_fe_ca_lan_cod_seq" VARCHAR(10)
   ,"prod_feat_cat_language_code" VARCHAR(10)
   ,"prod_feat_cat_description" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."product_feature_cat" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."product_feature_class"
(
    "prod_featu_class_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"pro_fea_cla_code_bk" VARCHAR(1500)
   ,"product_feature_class_id" NUMERIC(16)
   ,"product_feature_class_desc" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."product_feature_class" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."product_features"
(
    "product_features_hkey" VARCHAR(32)
   ,"lnk_prfe_pfca_hkey" VARCHAR(32)
   ,"produ_featur_cat_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"produ_featu_code_bk" VARCHAR(1500)
   ,"pro_fea_cat_code_fk_productfeaturecatid_bk" VARCHAR(1500)
   ,"error_code_prfe_pfca" INTEGER
   ,"product_feature_id" INTEGER
   ,"product_feature_cat_id" INTEGER
   ,"product_feature_code" VARCHAR
   ,"pro_fea_lan_code_seq" VARCHAR(10)
   ,"product_feature_language_code" VARCHAR(10)
   ,"product_feature_description" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."product_features" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."products"
(
    "products_hkey" VARCHAR(32)
   ,"lnk_prod_prod_rpid_hkey" VARCHAR(32)
   ,"products_rpid_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"src_bk" VARCHAR
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"product_et_code_bk" VARCHAR(500)
   ,"product_cc_bk" VARCHAR(500)
   ,"product_part_code_bk" VARCHAR(500)
   ,"product_cc_fk_replacementproductid_bk" VARCHAR(500)
   ,"product_et_code_fk_replacementproductid_bk" VARCHAR(500)
   ,"product_part_code_fk_replacementproductid_bk" VARCHAR(500)
   ,"error_code_prod_prod_rpid" INTEGER
   ,"product_id" NUMERIC(16)
   ,"replacement_product_id" NUMERIC(16)
   ,"product_intro_date" DATE
   ,"product_name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."products" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


CREATE   TABLE "moto_sales_scn01_stg"."product_sensors"
(
    "product_sensors_hkey" VARCHAR(32)
   ,"lnk_prse_prod_hkey" VARCHAR(32)
   ,"products_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"subsequence_seq" INTEGER
   ,"load_cycle_id" INTEGER
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"vehicle_number_bk" VARCHAR(1500)
   ,"product_cc_fk_productnumber_bk" VARCHAR(500)
   ,"product_et_code_fk_productnumber_bk" VARCHAR(500)
   ,"product_part_code_fk_productnumber_bk" VARCHAR(500)
   ,"vehicle_number" VARCHAR
   ,"product_number" NUMERIC(16)
   ,"sensor" VARCHAR
   ,"sensor_value" NUMERIC(14, 2)
   ,"unit_of_measurement" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_stg"."product_sensors" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36';


-- END


