create keyspace if not exists datahub WITH REPLICATION = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 1
};

create table if not exists datahub.metadata_aspect_v2 (
  urn                           varchar,
  aspect                        varchar,
  version                       bigint,
  metadata                      text,
  systemmetadata                text,
  createdon                     timestamp,
  createdby                     varchar,
  createdfor                    varchar,
  entity                        varchar,
  primary key ((urn), aspect, version))
with clustering order by (aspect asc, version asc);

insert into datahub.metadata_aspect_v2 (urn, aspect, version, metadata, createdon, createdby, entity) values(
  'urn:li:corpuser:datahub',
  'corpUserInfo',
  0,
  '{"displayName":"Data Hub","active":true,"fullName":"Data Hub","email":"datahub@linkedin.com"}',
  toTimestamp(now()),
  'urn:li:corpuser:__datahub_system',
  'corpuser'
) if not exists;

insert into datahub.metadata_aspect_v2 (urn, aspect, version, metadata, createdon, createdby, entity) values(
  'urn:li:corpuser:datahub',
  'corpUserEditableInfo',
  0,
  '{"skills":[],"teams":[],"pictureLink":"https://raw.githubusercontent.com/linkedin/datahub/master/datahub-web-react/src/images/default_avatar.png"}',
  toTimestamp(now()),
  'urn:li:corpuser:__datahub_system',
  'corpuser'
) if not exists;
