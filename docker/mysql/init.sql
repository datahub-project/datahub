-- create metadata aspect table
create table metadata_aspect (
  urn                           varchar(500) not null,
  aspect                        varchar(200) not null,
  version                       bigint(20) not null,
  metadata                      longtext not null,
  createdon                     datetime(6) not null,
  createdby                     varchar(255) not null,
  createdfor                    varchar(255),
  constraint pk_metadata_aspect primary key (urn,aspect,version)
);

-- create default records for datahub user
insert into metadata_aspect (urn, aspect, version, metadata, createdon, createdby) values(
  'urn:li:corpuser:datahub',
  'com.linkedin.identity.CorpUserInfo',
  0,
  '{"displayName":"Data Hub","active":true,"fullName":"Data Hub","email":"datahub@linkedin.com"}',
  now(),
  'urn:li:principal:datahub'
), (
  'urn:li:corpuser:datahub',
  'com.linkedin.identity.CorpUserEditableInfo',
  0,
  '{"skills":[],"teams":[],"pictureLink":"https://raw.githubusercontent.com/linkedin/datahub/master/datahub-web/packages/data-portal/public/assets/images/default_avatar.png"}',
  now(),
  'urn:li:principal:datahub'
);