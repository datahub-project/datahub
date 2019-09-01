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