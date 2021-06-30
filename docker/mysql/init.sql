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
  CONSTRAINT pk_metadata_aspect_v2 PRIMARY KEY (urn,aspect,version)
);

INSERT INTO metadata_aspect_v2 (urn, aspect, version, metadata, createdon, createdby) VALUES(
  'urn:li:corpuser:datahub',
  'corpUserInfo',
  0,
  '{"displayName":"Data Hub","active":true,"fullName":"Data Hub","email":"datahub@linkedin.com"}',
  now(),
  'urn:li:principal:datahub'
), (
  'urn:li:corpuser:datahub',
  'corpUserEditableInfo',
  0,
  '{"skills":[],"teams":[],"pictureLink":"https://raw.githubusercontent.com/linkedin/datahub/master/datahub-web/packages/data-portal/public/assets/images/default_avatar.png"}',
  now(),
  'urn:li:principal:datahub'
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
