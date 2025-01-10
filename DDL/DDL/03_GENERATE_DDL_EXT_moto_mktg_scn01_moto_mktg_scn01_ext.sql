/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.14, generation date: 2025/01/09 12:47:27
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51
 */

/* DROP TABLES */

-- START

DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."addresses" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."campaign_motorcycles" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."campaign_motorcycles_err" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."campaigns" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."camp_moto_channel" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."camp_moto_channel_err" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."camp_moto_chan_region" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."camp_moto_chan_region_err" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."camp_part_cont" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."camp_part_cont_err" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."channels" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."contacts" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."e_mails" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."motorcycles" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."party" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."party_contacts" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."party_contacts_err" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."party_err" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01_ext"."phones" 
CASCADE
;

-- END


/* CREATE TABLES */

-- START


CREATE   TABLE "moto_mktg_scn01_ext"."addresses"
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
   ,"province" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."addresses" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."campaign_motorcycles"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"motorcycle_id" NUMERIC(16)
   ,"campaign_code_fk_campaignstartdate_bk" VARCHAR(750)
   ,"campaign_start_date_fk_campaignstartdate_bk" VARCHAR(750)
   ,"motorcycle_class_desc" VARCHAR
   ,"motorcycle_subclass_desc" VARCHAR
   ,"motorcycle_emotion_desc" VARCHAR
   ,"motorcycle_comment" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_camp_prod" INTEGER
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."campaign_motorcycles" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."campaign_motorcycles_err"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"motorcycle_id" NUMERIC(16)
   ,"campaign_code_fk_campaignstartdate_bk" VARCHAR(750)
   ,"campaign_start_date_fk_campaignstartdate_bk" VARCHAR(750)
   ,"motorcycle_class_desc" VARCHAR
   ,"motorcycle_subclass_desc" VARCHAR
   ,"motorcycle_emotion_desc" VARCHAR
   ,"motorcycle_comment" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_camp_prod" INTEGER
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."campaign_motorcycles_err" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."campaigns"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"campaign_code_bk" VARCHAR(750)
   ,"campaign_start_date_bk" VARCHAR(750)
   ,"campaign_name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."campaigns" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."camp_moto_channel"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"channel_id" NUMERIC(16)
   ,"campaign_code_fk_campaignstartdate_bk" VARCHAR(750)
   ,"campaign_start_date_fk_campaignstartdate_bk" VARCHAR(750)
   ,"from_date_seq" TIMESTAMP(6)
   ,"from_date" TIMESTAMP(6)
   ,"motorcycle_name" VARCHAR
   ,"to_date" TIMESTAMP(6)
   ,"valid_from_date" TIMESTAMP(6)
   ,"valid_to_date" TIMESTAMP(6)
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_camp_moto_channel" INTEGER
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."camp_moto_channel" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."camp_moto_channel_err"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"channel_id" NUMERIC(16)
   ,"campaign_code_fk_campaignstartdate_bk" VARCHAR(750)
   ,"campaign_start_date_fk_campaignstartdate_bk" VARCHAR(750)
   ,"from_date_seq" TIMESTAMP(6)
   ,"from_date" TIMESTAMP(6)
   ,"motorcycle_name" VARCHAR
   ,"to_date" TIMESTAMP(6)
   ,"valid_from_date" TIMESTAMP(6)
   ,"valid_to_date" TIMESTAMP(6)
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_camp_moto_channel" INTEGER
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."camp_moto_channel_err" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."camp_moto_chan_region"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"channel_id" NUMERIC(16)
   ,"motorcycle_id" NUMERIC(16)
   ,"campaign_code_fk_campaignstartdate_bk" VARCHAR(750)
   ,"campaign_start_date_fk_campaignstartdate_bk" VARCHAR(750)
   ,"region_seq" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_camp_moto_chan_region" INTEGER
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."camp_moto_chan_region" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."camp_moto_chan_region_err"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"channel_id" NUMERIC(16)
   ,"motorcycle_id" NUMERIC(16)
   ,"campaign_code_fk_campaignstartdate_bk" VARCHAR(750)
   ,"campaign_start_date_fk_campaignstartdate_bk" VARCHAR(750)
   ,"region_seq" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_camp_moto_chan_region" INTEGER
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."camp_moto_chan_region_err" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."camp_part_cont"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"contact_id" NUMERIC(16)
   ,"party_number" NUMERIC(16)
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"campaign_code_fk_campaignstartdate_bk" VARCHAR(750)
   ,"campaign_start_date_fk_campaignstartdate_bk" VARCHAR(750)
   ,"contact_id_fk_contactid_bk" VARCHAR(1500)
   ,"marketing_program_code" CHARACTER(30)
   ,"marketing_program_name" CHARACTER(300)
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_camp_cust_cont" INTEGER
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."camp_part_cont" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."camp_part_cont_err"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"contact_id" NUMERIC(16)
   ,"party_number" NUMERIC(16)
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"campaign_code_fk_campaignstartdate_bk" VARCHAR(750)
   ,"campaign_start_date_fk_campaignstartdate_bk" VARCHAR(750)
   ,"contact_id_fk_contactid_bk" VARCHAR(1500)
   ,"marketing_program_code" CHARACTER(30)
   ,"marketing_program_name" CHARACTER(300)
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_camp_cust_cont" INTEGER
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."camp_part_cont_err" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."channels"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"channel_id" NUMERIC(16)
   ,"channel_code_bk" VARCHAR(1500)
   ,"channel_description" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."channels" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."contacts"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"contact_id" NUMERIC(16)
   ,"contact_id_bk" VARCHAR(1500)
   ,"contact_type" VARCHAR
   ,"contact_type_desc" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."contacts" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."e_mails"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"contact_id" NUMERIC(16)
   ,"contact_id_fk_contactid_bk" VARCHAR(1500)
   ,"name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."e_mails" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."motorcycles"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"motorcycle_id" NUMERIC(16)
   ,"product_cc_bk" VARCHAR(500)
   ,"product_et_code_bk" VARCHAR(500)
   ,"product_part_code_bk" VARCHAR(500)
   ,"motorcycle_name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."motorcycles" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."party"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"party_number" NUMERIC(16)
   ,"address_number" NUMERIC(16)
   ,"parent_party_number" NUMERIC(16)
   ,"name_bk" VARCHAR(375)
   ,"birthdate_bk" VARCHAR(375)
   ,"gender_bk" VARCHAR(375)
   ,"party_type_code_bk" VARCHAR(375)
   ,"name" VARCHAR
   ,"birthdate" DATE
   ,"gender" CHARACTER(24)
   ,"party_type_code" CHARACTER(6)
   ,"street_name_fk_addressnumber_bk" VARCHAR(375)
   ,"street_number_fk_addressnumber_bk" VARCHAR(375)
   ,"postal_code_fk_addressnumber_bk" VARCHAR(375)
   ,"city_fk_addressnumber_bk" VARCHAR(375)
   ,"name_fk_parentpartynumber_bk" VARCHAR(375)
   ,"birthdate_fk_parentpartynumber_bk" VARCHAR(375)
   ,"gender_fk_parentpartynumber_bk" VARCHAR(375)
   ,"party_type_code_fk_parentpartynumber_bk" VARCHAR(375)
   ,"comments" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_cust_addr" INTEGER
   ,"error_code_cust_cust_parentpartynumber" INTEGER
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."party" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."party_contacts"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"party_number" NUMERIC(16)
   ,"contact_id" NUMERIC(16)
   ,"contact_id_fk_contactid_bk" VARCHAR(1500)
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_cust_cont" INTEGER
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."party_contacts" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."party_contacts_err"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"party_number" NUMERIC(16)
   ,"contact_id" NUMERIC(16)
   ,"contact_id_fk_contactid_bk" VARCHAR(1500)
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_cust_cont" INTEGER
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."party_contacts_err" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."party_err"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"party_number" NUMERIC(16)
   ,"address_number" NUMERIC(16)
   ,"parent_party_number" NUMERIC(16)
   ,"name_bk" VARCHAR(375)
   ,"birthdate_bk" VARCHAR(375)
   ,"gender_bk" VARCHAR(375)
   ,"party_type_code_bk" VARCHAR(375)
   ,"name" VARCHAR
   ,"birthdate" DATE
   ,"gender" CHARACTER(24)
   ,"party_type_code" CHARACTER(6)
   ,"street_name_fk_addressnumber_bk" VARCHAR(375)
   ,"street_number_fk_addressnumber_bk" VARCHAR(375)
   ,"postal_code_fk_addressnumber_bk" VARCHAR(375)
   ,"city_fk_addressnumber_bk" VARCHAR(375)
   ,"name_fk_parentpartynumber_bk" VARCHAR(375)
   ,"birthdate_fk_parentpartynumber_bk" VARCHAR(375)
   ,"gender_fk_parentpartynumber_bk" VARCHAR(375)
   ,"party_type_code_fk_parentpartynumber_bk" VARCHAR(375)
   ,"comments" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"error_code_cust_addr" INTEGER
   ,"error_code_cust_cust_parentpartynumber" INTEGER
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."party_err" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01_ext"."phones"
(
    "load_date" TIMESTAMP
   ,"load_cycle_id" INTEGER
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"record_type" VARCHAR
   ,"contact_id" NUMERIC(16)
   ,"contact_id_fk_contactid_bk" VARCHAR(1500)
   ,"phone_number" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01_ext"."phones" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


-- END


