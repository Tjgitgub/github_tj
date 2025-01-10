/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.14, generation date: 2025/01/09 12:47:27
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04
 */

/* DROP TABLES */

-- START

DROP TABLE IF EXISTS "moto_sales_scn01_ext"."addresses" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."cust_addresses" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."cust_addresses_err" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."customers" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."customers_err" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."invoice_lines" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."invoice_lines_err" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."invoices" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."invoices_err" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."parts" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."payments" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."payments_err" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."product_feat_class_rel" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."product_feat_class_rel_err" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."product_feature_cat" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."product_feature_class" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."product_features" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."product_features_err" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."products" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."product_sensors" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01_ext"."products_err" 
CASCADE
;

-- END


/* CREATE TABLES */

-- START


CREATE   TABLE "moto_sales_scn01_ext"."addresses"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"address_number" NUMERIC(16)
   ,"street_name_bk" VARCHAR(375)
   ,"street_number_bk" VARCHAR(375)
   ,"postal_code_bk" VARCHAR(375)
   ,"city_bk" VARCHAR(375)
   ,"street_name" VARCHAR
   ,"street_number" NUMERIC(16)
   ,"postal_code" VARCHAR
   ,"city" VARCHAR
   ,"coordinates" gps
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."addresses" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."cust_addresses"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"address_number" NUMERIC(16)
   ,"customer_number" NUMERIC(16)
   ,"address_type_seq" VARCHAR(4)
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_cust_addresses" INTEGER
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."cust_addresses" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."cust_addresses_err"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"address_number" NUMERIC(16)
   ,"customer_number" NUMERIC(16)
   ,"address_type_seq" VARCHAR(4)
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_cust_addresses" INTEGER
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."cust_addresses_err" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."customers"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"customer_number" NUMERIC(16)
   ,"customer_invoice_address_id" NUMERIC(16)
   ,"customer_ship_to_address_id" NUMERIC(16)
   ,"national_person_id_bk" VARCHAR(1500)
   ,"national_person_id" VARCHAR
   ,"street_name_fk_customerinvoiceaddressid_bk" VARCHAR(375)
   ,"street_number_fk_customerinvoiceaddressid_bk" VARCHAR(375)
   ,"postal_code_fk_customerinvoiceaddressid_bk" VARCHAR(375)
   ,"city_fk_customerinvoiceaddressid_bk" VARCHAR(375)
   ,"street_name_fk_customershiptoaddressid_bk" VARCHAR(375)
   ,"street_number_fk_customershiptoaddressid_bk" VARCHAR(375)
   ,"postal_code_fk_customershiptoaddressid_bk" VARCHAR(375)
   ,"city_fk_customershiptoaddressid_bk" VARCHAR(375)
   ,"first_name" VARCHAR
   ,"last_name" VARCHAR
   ,"birthdate" DATE
   ,"gender" CHARACTER(24)
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_cust_addr_ciai" INTEGER
   ,"error_code_cust_addr_iats" INTEGER
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."customers" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."customers_err"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"customer_number" NUMERIC(16)
   ,"customer_invoice_address_id" NUMERIC(16)
   ,"customer_ship_to_address_id" NUMERIC(16)
   ,"national_person_id_bk" VARCHAR(1500)
   ,"national_person_id" VARCHAR
   ,"street_name_fk_customerinvoiceaddressid_bk" VARCHAR(375)
   ,"street_number_fk_customerinvoiceaddressid_bk" VARCHAR(375)
   ,"postal_code_fk_customerinvoiceaddressid_bk" VARCHAR(375)
   ,"city_fk_customerinvoiceaddressid_bk" VARCHAR(375)
   ,"street_name_fk_customershiptoaddressid_bk" VARCHAR(375)
   ,"street_number_fk_customershiptoaddressid_bk" VARCHAR(375)
   ,"postal_code_fk_customershiptoaddressid_bk" VARCHAR(375)
   ,"city_fk_customershiptoaddressid_bk" VARCHAR(375)
   ,"first_name" VARCHAR
   ,"last_name" VARCHAR
   ,"birthdate" DATE
   ,"gender" CHARACTER(24)
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_cust_addr_ciai" INTEGER
   ,"error_code_cust_addr_iats" INTEGER
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."customers_err" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."invoice_lines"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"invoice_number" NUMERIC(16)
   ,"invoice_line_number" NUMERIC(16)
   ,"part_id" NUMERIC(16)
   ,"product_id" NUMERIC(16)
   ,"invoice_number_bk" VARCHAR(750)
   ,"invoice_line_number_bk" VARCHAR(750)
   ,"invoice_number_fk_invoicenumber_bk" VARCHAR(1500)
   ,"part_number_fk_partid_bk" VARCHAR(750)
   ,"part_langua_code_fk_partid_bk" VARCHAR(750)
   ,"product_cc_fk_productid_bk" VARCHAR(500)
   ,"product_et_code_fk_productid_bk" VARCHAR(500)
   ,"product_part_code_fk_productid_bk" VARCHAR(500)
   ,"amount" NUMERIC(14, 2)
   ,"quantity" NUMERIC(12)
   ,"unit_price" NUMERIC(14, 2)
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_inli_part" INTEGER
   ,"error_code_inli_prod" INTEGER
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."invoice_lines" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."invoice_lines_err"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"invoice_number" NUMERIC(16)
   ,"invoice_line_number" NUMERIC(16)
   ,"part_id" NUMERIC(16)
   ,"product_id" NUMERIC(16)
   ,"invoice_number_bk" VARCHAR(750)
   ,"invoice_line_number_bk" VARCHAR(750)
   ,"invoice_number_fk_invoicenumber_bk" VARCHAR(1500)
   ,"part_number_fk_partid_bk" VARCHAR(750)
   ,"part_langua_code_fk_partid_bk" VARCHAR(750)
   ,"product_cc_fk_productid_bk" VARCHAR(500)
   ,"product_et_code_fk_productid_bk" VARCHAR(500)
   ,"product_part_code_fk_productid_bk" VARCHAR(500)
   ,"amount" NUMERIC(14, 2)
   ,"quantity" NUMERIC(12)
   ,"unit_price" NUMERIC(14, 2)
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_inli_part" INTEGER
   ,"error_code_inli_prod" INTEGER
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."invoice_lines_err" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."invoices"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"invoice_number" NUMERIC(16)
   ,"invoice_customer_id" NUMERIC(16)
   ,"invoice_number_bk" VARCHAR(1500)
   ,"national_person_id_fk_invoicecustomerid_bk" VARCHAR(1500)
   ,"invoice_date" DATE
   ,"amount" NUMERIC(14, 2)
   ,"discount" INTEGER
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_invo_cust" INTEGER
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."invoices" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."invoices_err"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"invoice_number" NUMERIC(16)
   ,"invoice_customer_id" NUMERIC(16)
   ,"invoice_number_bk" VARCHAR(1500)
   ,"national_person_id_fk_invoicecustomerid_bk" VARCHAR(1500)
   ,"invoice_date" DATE
   ,"amount" NUMERIC(14, 2)
   ,"discount" INTEGER
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_invo_cust" INTEGER
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."invoices_err" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."parts"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"part_id" NUMERIC(16)
   ,"ref_part_number_fk" VARCHAR
   ,"ref_part_language_code_fk" VARCHAR
   ,"part_number_bk" VARCHAR(750)
   ,"part_langua_code_bk" VARCHAR(750)
   ,"part_number" VARCHAR
   ,"part_language_code" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."parts" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."payments"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"customer_number" NUMERIC(16)
   ,"invoice_number" NUMERIC(16)
   ,"invoice_number_fk_invoicenumber_bk" VARCHAR(1500)
   ,"transaction_id" VARCHAR
   ,"date_time" TIMESTAMP(6)
   ,"amount" NUMERIC(14, 2)
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_payments" INTEGER
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."payments" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."payments_err"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"customer_number" NUMERIC(16)
   ,"invoice_number" NUMERIC(16)
   ,"invoice_number_fk_invoicenumber_bk" VARCHAR(1500)
   ,"transaction_id" VARCHAR
   ,"date_time" TIMESTAMP(6)
   ,"amount" NUMERIC(14, 2)
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_payments" INTEGER
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."payments_err" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."product_feat_class_rel"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"product_id" NUMERIC(16)
   ,"product_feature_class_id" NUMERIC(16)
   ,"product_feature_id" INTEGER
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_pro_fea_clas_rel" INTEGER
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."product_feat_class_rel" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."product_feat_class_rel_err"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"product_id" NUMERIC(16)
   ,"product_feature_class_id" NUMERIC(16)
   ,"product_feature_id" INTEGER
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_pro_fea_clas_rel" INTEGER
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."product_feat_class_rel_err" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."product_feature_cat"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"product_feature_category_id" INTEGER
   ,"pro_fea_cat_code_bk" VARCHAR(1500)
   ,"product_feature_category_code" VARCHAR
   ,"pr_fe_ca_lan_cod_seq" VARCHAR(10)
   ,"prod_feat_cat_language_code" VARCHAR(10)
   ,"prod_feat_cat_description" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."product_feature_cat" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."product_feature_class"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"product_feature_class_id" NUMERIC(16)
   ,"pro_fea_cla_code_bk" VARCHAR(1500)
   ,"product_feature_class_desc" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."product_feature_class" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."product_features"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"product_feature_id" INTEGER
   ,"product_feature_cat_id" INTEGER
   ,"produ_featu_code_bk" VARCHAR(1500)
   ,"product_feature_code" VARCHAR
   ,"pro_fea_cat_code_fk_productfeaturecatid_bk" VARCHAR(1500)
   ,"pro_fea_lan_code_seq" VARCHAR(10)
   ,"product_feature_language_code" VARCHAR(10)
   ,"product_feature_description" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_prfe_pfca" INTEGER
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."product_features" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."product_features_err"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"product_feature_id" INTEGER
   ,"product_feature_cat_id" INTEGER
   ,"produ_featu_code_bk" VARCHAR(1500)
   ,"product_feature_code" VARCHAR
   ,"pro_fea_cat_code_fk_productfeaturecatid_bk" VARCHAR(1500)
   ,"pro_fea_lan_code_seq" VARCHAR(10)
   ,"product_feature_language_code" VARCHAR(10)
   ,"product_feature_description" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_prfe_pfca" INTEGER
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."product_features_err" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."products"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"product_id" NUMERIC(16)
   ,"replacement_product_id" NUMERIC(16)
   ,"product_cc_bk" VARCHAR(500)
   ,"product_et_code_bk" VARCHAR(500)
   ,"product_part_code_bk" VARCHAR(500)
   ,"product_cc_fk_replacementproductid_bk" VARCHAR(500)
   ,"product_et_code_fk_replacementproductid_bk" VARCHAR(500)
   ,"product_part_code_fk_replacementproductid_bk" VARCHAR(500)
   ,"product_intro_date" DATE
   ,"product_name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_prod_prod_rpid" INTEGER
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."products" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."product_sensors"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"subsequence_seq" INTEGER
   ,"vehicle_number" VARCHAR
   ,"product_number" NUMERIC(16)
   ,"vehicle_number_bk" VARCHAR(1500)
   ,"product_cc_fk_productnumber_bk" VARCHAR(500)
   ,"product_et_code_fk_productnumber_bk" VARCHAR(500)
   ,"product_part_code_fk_productnumber_bk" VARCHAR(500)
   ,"sensor" VARCHAR
   ,"sensor_value" NUMERIC(14, 2)
   ,"unit_of_measurement" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."product_sensors" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


CREATE   TABLE "moto_sales_scn01_ext"."products_err"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"product_id" NUMERIC(16)
   ,"replacement_product_id" NUMERIC(16)
   ,"product_cc_bk" VARCHAR(500)
   ,"product_et_code_bk" VARCHAR(500)
   ,"product_part_code_bk" VARCHAR(500)
   ,"product_cc_fk_replacementproductid_bk" VARCHAR(500)
   ,"product_et_code_fk_replacementproductid_bk" VARCHAR(500)
   ,"product_part_code_fk_replacementproductid_bk" VARCHAR(500)
   ,"product_intro_date" DATE
   ,"product_name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_prod_prod_rpid" INTEGER
)
;

COMMENT ON TABLE "moto_sales_scn01_ext"."products_err" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:35:04';


-- END


