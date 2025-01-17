/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.16, generation date: 2025/01/17 07:03:56
DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46
 */

/* DROP TABLES */

-- START

DROP TABLE IF EXISTS "moto_mktg_scn01"."jrn_addresses" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."jrn_campaign_motorcycles" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."jrn_campaigns" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."jrn_camp_moto_channel" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."jrn_camp_moto_chan_region" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."jrn_camp_part_cont" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."jrn_channels" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."jrn_contacts" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."jrn_e_mails" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."jrn_motorcycles" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."jrn_party" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."jrn_party_contacts" 
CASCADE
;
DROP TABLE IF EXISTS "moto_mktg_scn01"."jrn_phones" 
CASCADE
;

-- END


/* CREATE TABLES */

-- START


CREATE   TABLE "moto_mktg_scn01"."jrn_addresses"
(
    "address_number" NUMERIC(16)
   ,"street_name" VARCHAR
   ,"street_number" NUMERIC(16)
   ,"postal_code" VARCHAR
   ,"city" VARCHAR
   ,"province" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"image_type" VARCHAR
   ,"trans_id" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01"."jrn_addresses" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46';


CREATE   TABLE "moto_mktg_scn01"."jrn_campaign_motorcycles"
(
    "campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"motorcycle_id" NUMERIC(16)
   ,"motorcycle_class_desc" VARCHAR
   ,"motorcycle_subclass_desc" VARCHAR
   ,"motorcycle_emotion_desc" VARCHAR
   ,"motorcycle_comment" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"image_type" VARCHAR
   ,"trans_id" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01"."jrn_campaign_motorcycles" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46';


CREATE   TABLE "moto_mktg_scn01"."jrn_campaigns"
(
    "campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"campaign_name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"image_type" VARCHAR
   ,"trans_id" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01"."jrn_campaigns" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46';


CREATE   TABLE "moto_mktg_scn01"."jrn_camp_moto_channel"
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
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"image_type" VARCHAR
   ,"trans_id" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01"."jrn_camp_moto_channel" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46';


CREATE   TABLE "moto_mktg_scn01"."jrn_camp_moto_chan_region"
(
    "channel_id" NUMERIC(16)
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"motorcycle_id" NUMERIC(16)
   ,"region" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"image_type" VARCHAR
   ,"trans_id" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01"."jrn_camp_moto_chan_region" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46';


CREATE   TABLE "moto_mktg_scn01"."jrn_camp_part_cont"
(
    "party_number" NUMERIC(16)
   ,"campaign_code" VARCHAR
   ,"campaign_start_date" DATE
   ,"contact_id" NUMERIC(16)
   ,"marketing_program_code" CHARACTER(30)
   ,"marketing_program_name" CHARACTER(300)
   ,"update_timestamp" TIMESTAMP(6)
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"image_type" VARCHAR
   ,"trans_id" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01"."jrn_camp_part_cont" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46';


CREATE   TABLE "moto_mktg_scn01"."jrn_channels"
(
    "channel_id" NUMERIC(16)
   ,"channel_code" VARCHAR
   ,"channel_description" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"image_type" VARCHAR
   ,"trans_id" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01"."jrn_channels" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46';


CREATE   TABLE "moto_mktg_scn01"."jrn_contacts"
(
    "contact_id" NUMERIC(16)
   ,"contact_type" VARCHAR
   ,"contact_type_desc" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"image_type" VARCHAR
   ,"trans_id" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01"."jrn_contacts" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46';


CREATE   TABLE "moto_mktg_scn01"."jrn_e_mails"
(
    "contact_id" NUMERIC(16)
   ,"name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"image_type" VARCHAR
   ,"trans_id" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01"."jrn_e_mails" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46';


CREATE   TABLE "moto_mktg_scn01"."jrn_motorcycles"
(
    "motorcycle_id" NUMERIC(16)
   ,"motorcycle_cc" NUMERIC(16)
   ,"motorcycle_et_code" CHARACTER(30)
   ,"motorcycle_part_code" VARCHAR
   ,"motorcycle_name" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"image_type" VARCHAR
   ,"trans_id" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01"."jrn_motorcycles" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46';


CREATE   TABLE "moto_mktg_scn01"."jrn_party"
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
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"image_type" VARCHAR
   ,"trans_id" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01"."jrn_party" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46';


CREATE   TABLE "moto_mktg_scn01"."jrn_party_contacts"
(
    "party_number" NUMERIC(16)
   ,"contact_id" NUMERIC(16)
   ,"update_timestamp" TIMESTAMP(6)
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"image_type" VARCHAR
   ,"trans_id" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01"."jrn_party_contacts" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46';


CREATE   TABLE "moto_mktg_scn01"."jrn_phones"
(
    "contact_id" NUMERIC(16)
   ,"phone_number" VARCHAR
   ,"update_timestamp" TIMESTAMP(6)
   ,"trans_timestamp" TIMESTAMP
   ,"operation" VARCHAR
   ,"image_type" VARCHAR
   ,"trans_id" VARCHAR
)
;

COMMENT ON TABLE "moto_mktg_scn01"."jrn_phones" IS 'DV_NAME: moto_scn01 - Release: R1(1) - Comment: VaultSpeed setup automation - Release date: 2025/01/16 14:54:27, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46';


-- END


