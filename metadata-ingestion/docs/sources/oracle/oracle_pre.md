### Prerequisites

#### Data Dictionary Mode/Views

The Oracle ingestion source supports two modes for extracting metadata information (see `data_dictionary_mode` option): `ALL` and `DBA`. In the `ALL` mode, the SQLAlchemy backend queries `ALL_` data dictionary views to extract metadata information. In the `DBA` mode, the Oracle ingestion source directly queries `DBA_` data dictionary views to extract metadata information. `ALL_` views only provide information accessible to the user used for ingestion while `DBA_` views provide information for the entire database (that is, all schema objects in the database).

The following table contains a brief description of what each data dictionary view is used for:

| Data Dictionary View | What's it used for? |
| --- | --- |
| `ALL_TABLES` or `DBA_TABLES` | Get list of all relational tables in the database |
| `ALL_VIEWS` or `DBA_VIEWS` | Get list of all views in the database |
| `ALL_TAB_COMMENTS` or `DBA_TAB_COMMENTS` | Get comments on tables and views |
| `ALL_TAB_COLS` or `DBA_TAB_COLS` | Get description of the columns of tables and views |
| `ALL_COL_COMMENTS` or `DBA_COL_COMMENTS` | Get comments on the columns of tables and views |
| `ALL_TAB_IDENTITY_COLS` or `DBA_TAB_IDENTITY_COLS` | Get table identity columns |
| `ALL_CONSTRAINTS` or `DBA_CONSTRAINTS` | Get constraint definitions on tables |
| `ALL_CONS_COLUMNS` or `DBA_CONS_COLUMNS` | Get list of columns that are specified in constraints |
| `ALL_USERS` or `DBA_USERS` | Get all schema names |

#### Data Dictionary Views accessible information and required privileges

- `ALL_` views display all the information accessible to the user used for ingestion, including information from the user's schema as well as information from objects in other schemas, if the user has access to those objects by way of grants of privileges or roles.
- `DBA_` views display all relevant information in the entire database. They can be queried only by users with the `SYSDBA` system privilege or `SELECT ANY DICTIONARY` privilege, or `SELECT_CATALOG_ROLE` role, or by users with direct privileges granted to them.
