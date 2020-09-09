create table metadata_aspect (
  urn                           varchar(500) not null,
  aspect                        varchar(200) not null,
  version                       bigint not null,
  metadata                      clob not null,
  createdon                     timestamp not null,
  createdby                     varchar(255) not null,
  createdfor                    varchar(255),
  constraint pk_metadata_aspect primary key (urn,aspect,version)
);

create table metadata_id (
  namespace                     varchar(255) not null,
  id                            bigint not null,
  constraint uq_metadata_id_namespace_id unique (namespace,id)
);

create table metadata_index (
  id                            bigint auto_increment not null,
  urn                           varchar(500) not null,
  aspect                        varchar(200) not null,
  path                          varchar(200) not null,
  longval                       bigint,
  stringval                     varchar(500),
  doubleval                     double,
  constraint pk_metadata_index primary key (id)
);

create index idx_long_val on metadata_index (aspect,path,longval,urn);
create index idx_string_val on metadata_index (aspect,path,stringval,urn);
create index idx_double_val on metadata_index (aspect,path,doubleval,urn);
create index idx_urn on metadata_index (urn);
