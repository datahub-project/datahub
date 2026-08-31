create database postgrestest;

\c postgrestest;

-- create metadata aspect table
create table metadata_aspect_v2 (
  urn                           varchar(500) not null,
  aspect                        varchar(200) not null,
  version                       bigint not null,
  metadata                      text not null,
  systemmetadata                text,
  createdon                     timestamp not null,
  createdby                     varchar(255) not null,
  createdfor                    varchar(255),
  constraint pk_metadata_aspect_v2 primary key (urn,aspect,version),
  metadata_json json not null
);

create index timeIndex ON metadata_aspect_v2 (createdon);

insert into metadata_aspect_v2 (urn, aspect, version, metadata, createdon, createdby, metadata_json) values(
  'urn:li:corpuser:datahub',
  'corpUserInfo',
  0,
  '{"displayName":"Data Hub","active":true,"fullName":"Data Hub","email":"datahub@linkedin.com"}',
  now(),
  'urn:li:corpuser:__datahub_system',
  '{"displayName":"Data Hub","active":true,"fullName":"Data Hub","email":"datahub@linkedin.com"}'
), (
  'urn:li:corpuser:datahub',
  'corpUserEditableInfo',
  0,
  '{"skills":[],"teams":[],"pictureLink":"https://raw.githubusercontent.com/datahub-project/datahub/master/datahub-web-react/src/images/default_avatar.png"}',
  now(),
  'urn:li:corpuser:__datahub_system',
  '{"skills":[],"teams":[],"pictureLink":"https://raw.githubusercontent.com/datahub-project/datahub/master/datahub-web-react/src/images/default_avatar.png"}'
);

create view metadata_aspect_view as select urn, aspect from metadata_aspect_v2 where version=0;
-- To get estimate counts of table rows after analyze
ANALYZE;

CREATE PROCEDURE add_row_to_metadata_aspect_v2(
  urn                           varchar(500),
  aspect                        varchar(200),
  version                       bigint,
  metadata                      text,
  createdon                     timestamp,
  createdby                     varchar(255),
  metadata_json json
)
LANGUAGE SQL
AS $$
    insert into metadata_aspect_v2 (urn, aspect, version, metadata, createdon, createdby, metadata_json) values(
        urn,
        aspect,
        version,
        metadata,
        createdon,
        createdby,
        metadata_json
    )
$$;

-- ETL use case: tables and procedure for lineage testing
CREATE TABLE raw_orders (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    amount DECIMAL(10, 2) NOT NULL
);

CREATE TABLE processed_orders (
    order_id INT,
    customer_id INT,
    total DECIMAL(10, 2)
);

CREATE PROCEDURE etl_process_orders()
LANGUAGE SQL
AS $$
    INSERT INTO processed_orders (order_id, customer_id, total)
    SELECT id AS order_id, customer_id, amount AS total FROM raw_orders;
$$;
