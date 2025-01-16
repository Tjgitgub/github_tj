/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.16, generation date: 2025/01/16 15:13:29
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08
 */

/* DROP TABLES */

-- START

DROP TABLE IF EXISTS "moto_sales_scn01"."addresses" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01"."codes_to_language" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01"."cust_addresses" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01"."customers" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01"."invoice_lines" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01"."invoices" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01"."moto_parts" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01"."moto_products" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01"."payments" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01"."product_feat_class_rel" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01"."product_feature_cat" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01"."product_feature_class" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01"."product_features" 
CASCADE
;
DROP TABLE IF EXISTS "moto_sales_scn01"."product_sensors" 
CASCADE
;

-- END


/* CREATE TABLES */

-- START


CREATE   TABLE "moto_sales_scn01"."addresses"
(
    "address_number" NUMERIC(16)
   ,"street_name" VARCHAR
   ,"street_number" NUMERIC(16)
   ,"postal_code" VARCHAR
   ,"city" VARCHAR
   ,"coordinates" gps
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01"."addresses" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08';


CREATE   TABLE "moto_sales_scn01"."codes_to_language"
(
    "code" VARCHAR
   ,"language_code" VARCHAR
   ,"description" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01"."codes_to_language" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08';


CREATE   TABLE "moto_sales_scn01"."cust_addresses"
(
    "customer_number" NUMERIC(16)
   ,"address_number" NUMERIC(16)
   ,"address_type" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01"."cust_addresses" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08';


CREATE   TABLE "moto_sales_scn01"."customers"
(
    "customer_number" NUMERIC(16)
   ,"national_person_id" VARCHAR
   ,"first_name" VARCHAR
   ,"last_name" VARCHAR
   ,"birthdate" DATE
   ,"gender" CHARACTER(24)
   ,"customer_invoice_address_id" NUMERIC(16)
   ,"customer_ship_to_address_id" NUMERIC(16)
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01"."customers" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08';


CREATE   TABLE "moto_sales_scn01"."invoice_lines"
(
    "invoice_line_number" NUMERIC(16)
   ,"invoice_number" NUMERIC(16)
   ,"product_id" NUMERIC(16)
   ,"part_id" NUMERIC(16)
   ,"amount" NUMERIC(14, 2)
   ,"quantity" NUMERIC(13)
   ,"unit_price" NUMERIC(14, 2)
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01"."invoice_lines" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08';


CREATE   TABLE "moto_sales_scn01"."invoices"
(
    "invoice_number" NUMERIC(16)
   ,"invoice_date" DATE
   ,"invoice_customer_id" NUMERIC(16)
   ,"amount" NUMERIC(14, 2)
   ,"discount" INTEGER
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01"."invoices" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08';


CREATE   TABLE "moto_sales_scn01"."moto_parts"
(
    "part_id" NUMERIC(16)
   ,"part_number" VARCHAR
   ,"part_language_code" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01"."moto_parts" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08';


CREATE   TABLE "moto_sales_scn01"."moto_products"
(
    "product_id" NUMERIC(16)
   ,"replacement_product_id" NUMERIC(16)
   ,"product_cc" NUMERIC(16)
   ,"product_et_code" CHARACTER(30)
   ,"product_part_code" VARCHAR
   ,"product_intro_date" DATE
   ,"product_name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01"."moto_products" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08';


CREATE   TABLE "moto_sales_scn01"."payments"
(
    "transaction_id" VARCHAR
   ,"date_time" TIMESTAMP(6)
   ,"invoice_number" NUMERIC(16)
   ,"amount" NUMERIC(14, 2)
   ,"customer_number" NUMERIC(16)
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01"."payments" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08';


CREATE   TABLE "moto_sales_scn01"."product_feat_class_rel"
(
    "product_feature_id" INTEGER
   ,"product_id" NUMERIC(16)
   ,"product_feature_class_id" NUMERIC(16)
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01"."product_feat_class_rel" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08';


CREATE   TABLE "moto_sales_scn01"."product_feature_cat"
(
    "product_feature_category_id" INTEGER
   ,"product_feature_category_code" VARCHAR
   ,"prod_feat_cat_language_code" VARCHAR
   ,"prod_feat_cat_description" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01"."product_feature_cat" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08';


CREATE   TABLE "moto_sales_scn01"."product_feature_class"
(
    "product_feature_class_id" NUMERIC(16)
   ,"product_feature_class_code" VARCHAR
   ,"product_feature_class_desc" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01"."product_feature_class" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08';


CREATE   TABLE "moto_sales_scn01"."product_features"
(
    "product_feature_id" INTEGER
   ,"product_feature_cat_id" INTEGER
   ,"product_feature_code" VARCHAR
   ,"product_feature_language_code" VARCHAR
   ,"product_feature_description" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_sales_scn01"."product_features" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08';


CREATE   TABLE "moto_sales_scn01"."product_sensors"
(
    "product_number" NUMERIC(16)
   ,"vehicle_number" VARCHAR
   ,"sensor" VARCHAR
   ,"sensor_value" NUMERIC(14, 2)
   ,"unit_of_measurement" VARCHAR
)
;

COMMENT ON TABLE "moto_sales_scn01"."product_sensors" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:51:08';


-- END


