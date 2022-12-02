-- create metadata aspect table
CREATE TABLE metadata_aspect_v2 (
  urn                           VARCHAR(500) NOT NULL,
  aspect                        VARCHAR(200) NOT NULL,
  version                       bigint(20) NOT NULL,
  metadata                      longtext NOT NULL,
  systemmetadata                longtext,
  createdon                     datetime(6) NOT NULL,
  createdby                     VARCHAR(255) NOT NULL,
  createdfor                    VARCHAR(255),
  constraint pk_metadata_aspect_v2 primary key (urn,aspect,version),
  INDEX timeIndex (createdon)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

INSERT INTO metadata_aspect_v2 (urn, aspect, version, metadata, createdon, createdby) VALUES(
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

-- create metadata index table
CREATE TABLE metadata_index (
 `id` BIGINT NOT NULL AUTO_INCREMENT,
 `urn` VARCHAR(200) NOT NULL,
 `aspect` VARCHAR(150) NOT NULL,
 `path` VARCHAR(150) NOT NULL,
 `longVal` BIGINT,
 `stringVal` VARCHAR(200),
 `doubleVal` DOUBLE,
 CONSTRAINT id_pk PRIMARY KEY (id),
 INDEX longIndex (`urn`,`aspect`,`path`,`longVal`),
 INDEX stringIndex (`urn`,`aspect`,`path`,`stringVal`),
 INDEX doubleIndex (`urn`,`aspect`,`path`,`doubleVal`)
);
