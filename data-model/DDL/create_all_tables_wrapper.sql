/* wrapper sql to call all individual DDL */
use wherehows;

source ETL_DDL/dataset_metadata.sql;
source ETL_DDL/etl_configure_tables.sql;
source ETL_DDL/executor_metadata.sql;
source ETL_DDL/git_metadata.sql;
source ETL_DDL/lineage_metadata.sql;
source ETL_DDL/metric_metadata.sql;
source ETL_DDL/owner_metadata.sql;
source ETL_DDL/patterns.sql;

source WEB_DDL/track.sql;
source WEB_DDL/users.sql;

show tables;
