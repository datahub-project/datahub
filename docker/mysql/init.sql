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

-- create default records for data-platform user
insert into metadata_aspect (urn, aspect, version, metadata, createdon, createdby) values(
  'urn:li:corpuser:data-platform',
  'com.linkedin.identity.CorpUserInfo',
  0,
  '{"displayName":"Data Platform","active":true,"fullName":"Data Platform","email":"dops@typeform.com"}',
  now(),
  'urn:li:principal:data-platform'
), (
  'urn:li:corpuser:data-platform',
  'com.linkedin.identity.CorpUserEditableInfo',
  0,
  '{"skills":[],"teams":[],"pictureLink":"https://raw.githubusercontent.com/afranzi/datahub/master/datahub-web/packages/data-portal/public/assets/images/icons/ai.png"}',
  now(),
  'urn:li:principal:data-platform'
);