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

DROP TABLE IF EXISTS "moto_mktg_scn01"."addresses" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."campaign_motorcycles" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."campaigns" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."camp_moto_channel" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."camp_moto_chan_region" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."camp_part_cont" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."channels" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."contacts" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."e_mails" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."motorcycles" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."party" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."party_contacts" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."phones" 
CASCADE
;

-- END


/* CREATE TABLES */

-- START


CREATE   TABLE "moto_mktg_scn01"."addresses"
(
    "address_number" NUMERIC(16)
   ,"street_name" VARCHAR
   ,"street_number" NUMERIC(16)
   ,"postal_code" VARCHAR
   ,"city" VARCHAR
   ,"province" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01"."addresses" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01"."campaign_motorcycles"
(
    "campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"motorcycle_id" NUMERIC(16)
   ,"motorcycle_class_desc" VARCHAR
   ,"motorcycle_subclass_desc" VARCHAR
   ,"motorcycle_emotion_desc" VARCHAR
   ,"motorcycle_comment" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01"."campaign_motorcycles" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01"."campaigns"
(
    "campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"campaign_name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01"."campaigns" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01"."camp_moto_channel"
(
    "channel_id" NUMERIC(16)
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"motorcycle_name" VARCHAR
   ,"from_date" TIMESTAMP(6)
   ,"to_date" TIMESTAMP(6)
   ,"valid_from_date" TIMESTAMP(6)
   ,"valid_to_date" TIMESTAMP(6)
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01"."camp_moto_channel" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01"."camp_moto_chan_region"
(
    "channel_id" NUMERIC(16)
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"motorcycle_id" NUMERIC(16)
   ,"region" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01"."camp_moto_chan_region" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01"."camp_part_cont"
(
    "party_number" NUMERIC(16)
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"contact_id" NUMERIC(16)
   ,"marketing_program_code" CHARACTER(30)
   ,"marketing_program_name" CHARACTER(300)
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01"."camp_part_cont" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01"."channels"
(
    "channel_id" NUMERIC(16)
   ,"channel_code" VARCHAR
   ,"channel_description" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01"."channels" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01"."contacts"
(
    "contact_id" NUMERIC(16)
   ,"contact_type" VARCHAR
   ,"contact_type_desc" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01"."contacts" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01"."e_mails"
(
    "contact_id" NUMERIC(16)
   ,"name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01"."e_mails" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01"."motorcycles"
(
    "motorcycle_id" NUMERIC(16)
   ,"motorcycle_cc" NUMERIC(16)
   ,"motorcycle_et_code" CHARACTER(30)
   ,"motorcycle_part_code" VARCHAR
   ,"motorcycle_name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01"."motorcycles" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01"."party"
(
    "party_number" NUMERIC(16)
   ,"parent_party_number" NUMERIC(16)
   ,"name" VARCHAR
   ,"birthdate" DATE
   ,"gender" CHARACTER(24)
   ,"party_type_code" CHARACTER(6)
   ,"comments" VARCHAR
   ,"address_number" NUMERIC(16)
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01"."party" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01"."party_contacts"
(
    "party_number" NUMERIC(16)
   ,"contact_id" NUMERIC(16)
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01"."party_contacts" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


CREATE   TABLE "moto_mktg_scn01"."phones"
(
    "contact_id" NUMERIC(16)
   ,"phone_number" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
)
;

COMMENT ON TABLE "moto_mktg_scn01"."phones" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/09 09:38:36, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/09 09:37:51';


-- END


