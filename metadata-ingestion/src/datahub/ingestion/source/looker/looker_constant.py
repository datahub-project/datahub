import re

IMPORTED_PROJECTS = "imported_projects"
IMPORTED_PROJECTS_PREFIX = f"{IMPORTED_PROJECTS}/"
DIMENSIONS = "dimensions"
MEASURES = "measures"
DIMENSION_GROUPS = "dimension_groups"
SQL_TABLE_NAME = "sql_table_name"
DATAHUB_TRANSFORMED_SQL_TABLE_NAME = "datahub_transformed_sql_table_name"
DERIVED_TABLE = "derived_table"
SQL = "sql"
DATAHUB_TRANSFORMED_SQL = "datahub_transformed_sql"
prod = "prod"
dev = "dev"
NAME = "name"
DERIVED_DOT_SQL = "derived.sql"

VIEW_FIELD_TYPE_ATTRIBUTE = "type"
VIEW_FIELD_INTERVALS_ATTRIBUTE = "intervals"
VIEW_FIELD_TIMEFRAMES_ATTRIBUTE = "timeframes"

# Identifies the links the looker source manages on Explores: links with this label are
# replaced or removed on each run, so changing it would orphan links already written.
EXPLORE_GIT_LINK_LABEL = "View in Git"
# Looker's default when a project has no production branch configured.
LOOKER_DEFAULT_GIT_BRANCH = "master"
# LookML IDE link path: /projects/<project>/files/<url-encoded repo path>
LOOKML_LINK_PATH_PATTERN = re.compile(r"^/projects/[^/]+/files/(.+)$")
# SCP-style SSH remotes like git@github.com:org/repo.git
GIT_SCP_REMOTE_PATTERN = re.compile(r"^[^@/]+@([^:/]+):(.+)$")
