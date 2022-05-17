-- create metadata aspect table
CREATE TABLE IF NOT EXISTS metadata_aspect_v2 (
  urn                           varchar(500) not null,
  aspect                        varchar(200) not null,
  version                       bigint not null,
  metadata                      text not null,
  systemmetadata                text,
  createdon                     timestamp not null,
  createdby                     varchar(255) not null,
  createdfor                    varchar(255),
  CONSTRAINT pk_metadata_aspect_v2 PRIMARY KEY (urn, aspect, version)
);

-- create default records for datahub user if not exists
CREATE TEMP TABLE temp_metadata_aspect_v2 AS TABLE metadata_aspect_v2;
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
