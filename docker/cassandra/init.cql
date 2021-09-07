create keyspace datahubpoc WITH REPLICATION = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 1
};

create table datahubpoc.metadata_aspect (
  urn                           varchar,
  aspect                        varchar,
  version                       bigint,
  metadata                      text,
  systemmetadata                text,
  createdon                     timestamp,
  createdby                     varchar,
  createdfor                    varchar,
  entity                        varchar,
  PRIMARY KEY (urn,aspect,version)
);