/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.16, generation date: 2025/01/20 10:46:04
DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14
 */

/* DROP TABLES */

-- START

DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."addresses" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."campaign_motorcycles" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."campaigns" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."camp_moto_channel" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."camp_moto_chan_region" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."camp_part_cont" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."channels" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."contacts" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."e_mails" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."motorcycles" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."party" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."party_contacts" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_stg"."phones" 
CASCADE
;
-- END


/* CREATE TABLES */

-- START


CREATE   TABLE "moto_mktg_scn01_stg"."addresses"
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
   ,"province" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."addresses" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14';


CREATE   TABLE "moto_mktg_scn01_stg"."campaign_motorcycles"
(
    "lnd_camp_prod_hkey" VARCHAR(32)
   ,"products_hkey" VARCHAR(32)
   ,"campaigns_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"campaign_code_fk_campaignstartdate_bk" VARCHAR(750)
   ,"campaign_start_date_fk_campaignstartdate_bk" VARCHAR(750)
   ,"product_cc_fk_motorcycleid_bk" VARCHAR(500)
   ,"product_et_code_fk_motorcycleid_bk" VARCHAR(500)
   ,"product_part_code_fk_motorcycleid_bk" VARCHAR(500)
   ,"error_code_camp_prod" INTEGER
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"motorcycle_id" NUMERIC(16)
   ,"motorcycle_class_desc" VARCHAR
   ,"motorcycle_subclass_desc" VARCHAR
   ,"motorcycle_emotion_desc" VARCHAR
   ,"motorcycle_comment" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."campaign_motorcycles" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14';


CREATE   TABLE "moto_mktg_scn01_stg"."campaigns"
(
    "campaigns_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"campaign_code_bk" VARCHAR(750)
   ,"campaign_start_date_bk" VARCHAR(750)
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"campaign_name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."campaigns" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14';


CREATE   TABLE "moto_mktg_scn01_stg"."camp_moto_channel"
(
    "lnd_camp_moto_channel_hkey" VARCHAR(32)
   ,"channels_hkey" VARCHAR(32)
   ,"campaigns_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"campaign_code_fk_campaignstartdate_bk" VARCHAR(750)
   ,"campaign_start_date_fk_campaignstartdate_bk" VARCHAR(750)
   ,"channel_code_fk_channelid_bk" VARCHAR(1500)
   ,"error_code_camp_moto_channel" INTEGER
   ,"channel_id" NUMERIC(16)
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"from_date_seq" TIMESTAMP(6)
   ,"from_date" TIMESTAMP(6)
   ,"motorcycle_name" VARCHAR
   ,"to_date" TIMESTAMP(6)
   ,"valid_from_date" TIMESTAMP(6)
   ,"valid_to_date" TIMESTAMP(6)
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."camp_moto_channel" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14';


CREATE   TABLE "moto_mktg_scn01_stg"."camp_moto_chan_region"
(
    "lnd_camp_moto_chan_region_hkey" VARCHAR(32)
   ,"channels_hkey" VARCHAR(32)
   ,"products_hkey" VARCHAR(32)
   ,"campaigns_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"channel_code_fk_channelid_bk" VARCHAR(1500)
   ,"campaign_code_fk_campaignstartdate_bk" VARCHAR(750)
   ,"campaign_start_date_fk_campaignstartdate_bk" VARCHAR(750)
   ,"product_cc_fk_motorcycleid_bk" VARCHAR(500)
   ,"product_et_code_fk_motorcycleid_bk" VARCHAR(500)
   ,"product_part_code_fk_motorcycleid_bk" VARCHAR(500)
   ,"error_code_camp_moto_chan_region" INTEGER
   ,"channel_id" NUMERIC(16)
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"motorcycle_id" NUMERIC(16)
   ,"region_seq" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."camp_moto_chan_region" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14';


CREATE   TABLE "moto_mktg_scn01_stg"."camp_part_cont"
(
    "lnd_camp_cust_cont_hkey" VARCHAR(32)
   ,"contacts_hkey" VARCHAR(32)
   ,"customers_hkey" VARCHAR(32)
   ,"campaigns_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"name_fk_partynumber_bk" VARCHAR(375)
   ,"campaign_code_fk_campaignstartdate_bk" VARCHAR(750)
   ,"campaign_start_date_fk_campaignstartdate_bk" VARCHAR(750)
   ,"contact_id_fk_contactid_bk" VARCHAR(1500)
   ,"birthdate_fk_partynumber_bk" VARCHAR(375)
   ,"gender_fk_partynumber_bk" VARCHAR(375)
   ,"party_type_code_fk_partynumber_bk" VARCHAR(375)
   ,"error_code_camp_cust_cont" INTEGER
   ,"party_number" NUMERIC(16)
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"contact_id" NUMERIC(16)
   ,"marketing_program_code" CHARACTER(30)
   ,"marketing_program_name" CHARACTER(300)
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."camp_part_cont" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14';


CREATE   TABLE "moto_mktg_scn01_stg"."channels"
(
    "channels_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"channel_code_bk" VARCHAR(1500)
   ,"channel_id" NUMERIC(16)
   ,"channel_description" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."channels" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14';


CREATE   TABLE "moto_mktg_scn01_stg"."contacts"
(
    "contacts_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"contact_id_bk" VARCHAR(1500)
   ,"contact_id" NUMERIC(16)
   ,"contact_type" VARCHAR
   ,"contact_type_desc" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."contacts" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14';


CREATE   TABLE "moto_mktg_scn01_stg"."e_mails"
(
    "contacts_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"contact_id_fk_contactid_bk" VARCHAR(1500)
   ,"contact_id" NUMERIC(16)
   ,"name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."e_mails" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14';


CREATE   TABLE "moto_mktg_scn01_stg"."motorcycles"
(
    "products_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"src_bk" VARCHAR
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"product_cc_bk" VARCHAR(500)
   ,"product_et_code_bk" VARCHAR(500)
   ,"product_part_code_bk" VARCHAR(500)
   ,"motorcycle_id" NUMERIC(16)
   ,"motorcycle_name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."motorcycles" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14';


CREATE   TABLE "moto_mktg_scn01_stg"."party"
(
    "customers_hkey" VARCHAR(32)
   ,"lnk_cust_addr_hkey" VARCHAR(32)
   ,"lnk_cust_cust_parentpartynumber_hkey" VARCHAR(32)
   ,"addresses_hkey" VARCHAR(32)
   ,"customers_parentpartynumber_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"src_bk" VARCHAR
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"customers_bk" VARCHAR
   ,"name_bk" VARCHAR(375)
   ,"birthdate_bk" VARCHAR(375)
   ,"gender_bk" VARCHAR(375)
   ,"party_type_code_bk" VARCHAR(375)
   ,"street_number_fk_addressnumber_bk" VARCHAR(375)
   ,"street_name_fk_addressnumber_bk" VARCHAR(375)
   ,"postal_code_fk_addressnumber_bk" VARCHAR(375)
   ,"city_fk_addressnumber_bk" VARCHAR(375)
   ,"name_fk_parentpartynumber_bk" VARCHAR(375)
   ,"birthdate_fk_parentpartynumber_bk" VARCHAR(375)
   ,"gender_fk_parentpartynumber_bk" VARCHAR(375)
   ,"party_type_code_fk_parentpartynumber_bk" VARCHAR(375)
   ,"error_code_cust_addr" INTEGER
   ,"error_code_cust_cust_parentpartynumber" INTEGER
   ,"party_number" NUMERIC(16)
   ,"parent_party_number" NUMERIC(16)
   ,"address_number" NUMERIC(16)
   ,"name" VARCHAR
   ,"birthdate" DATE
   ,"gender" CHARACTER(24)
   ,"party_type_code" CHARACTER(6)
   ,"comments" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."party" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14';


CREATE   TABLE "moto_mktg_scn01_stg"."party_contacts"
(
    "lnd_cust_cont_hkey" VARCHAR(32)
   ,"customers_hkey" VARCHAR(32)
   ,"contacts_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"contact_id_fk_contactid_bk" VARCHAR(1500)
   ,"name_fk_partynumber_bk" VARCHAR(375)
   ,"birthdate_fk_partynumber_bk" VARCHAR(375)
   ,"gender_fk_partynumber_bk" VARCHAR(375)
   ,"party_type_code_fk_partynumber_bk" VARCHAR(375)
   ,"error_code_cust_cont" INTEGER
   ,"party_number" NUMERIC(16)
   ,"contact_id" NUMERIC(16)
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."party_contacts" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14';


CREATE   TABLE "moto_mktg_scn01_stg"."phones"
(
    "contacts_hkey" VARCHAR(32)
   ,"load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"contact_id_fk_contactid_bk" VARCHAR(1500)
   ,"contact_id" NUMERIC(16)
   ,"phone_number" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_stg"."phones" IS 'DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14';


-- END


