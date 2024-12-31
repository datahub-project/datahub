-- create datahub database
CREATE DATABASE IF NOT EXISTS `DATAHUB_DB_NAME` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
USE `DATAHUB_DB_NAME`;

-- create metadata aspect table
create table if not exists metadata_aspect_v2 (
  urn                           varchar(500) not null,
  aspect                        varchar(200) not null,
  version                       bigint(20) not null,
  metadata                      longtext not null,
  systemmetadata                longtext,
  createdon                     datetime(6) not null,
  createdby                     varchar(255) not null,
  createdfor                    varchar(255),
  constraint pk_metadata_aspect_v2 primary key (urn,aspect,version),
  INDEX timeIndex (createdon)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- create default records for datahub user if not exists
DROP TABLE if exists temp_metadata_aspect_v2;
CREATE TABLE temp_metadata_aspect_v2 LIKE metadata_aspect_v2;
INSERT INTO temp_metadata_aspect_v2 (urn, aspect, version, metadata, createdon, createdby) VALUES(
  'urn:li:corpuser:datahub',
  'corpUserInfo',
  0,
  '{"displayName":"Data Hub","active":true,"fullName":"Data Hub","email":"datahub@linkedin.com"}',
  now(),
  'urn:li:corpuser:__datahub_system'
), (
  'urn:li:corpuser:datahub',
  'corpUserEditableInfo',
  0,
  '{"skills":[],"teams":[],"pictureLink":"https://raw.githubusercontent.com/datahub-project/datahub/master/datahub-web-react/src/images/default_avatar.png"}',
  now(),
  'urn:li:corpuser:__datahub_system'
);
-- only add default records if metadata_aspect is empty
INSERT INTO metadata_aspect_v2
SELECT * FROM temp_metadata_aspect_v2
WHERE NOT EXISTS (SELECT * from metadata_aspect_v2);
DROP TABLE temp_metadata_aspect_v2;

DROP TABLE IF EXISTS metadata_index;
