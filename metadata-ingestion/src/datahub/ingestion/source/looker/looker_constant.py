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
# Reading an Explore's links failed with a bad token (401) or throttling (429): it would
# fail for every Explore, so link reads stop for the run instead of skipping one Explore.
EXPLORE_GIT_LINK_RUN_WIDE_HTTP_STATUSES = frozenset({401, 429})
# Looker's default when a project has no production branch configured.
LOOKER_DEFAULT_GIT_BRANCH = "master"
# LookML IDE link path: /projects/<project>/files/<url-encoded repo path>
LOOKML_LINK_PATH_PATTERN = re.compile(r"^/projects/[^/]+/files/(.+)$")
# SCP-style SSH remotes like git@github.com:org/repo.git; the user is optional.
# No "@" after the user, so user:token@host/path can't parse with the token as host.
GIT_SCP_REMOTE_PATTERN = re.compile(r"^(?:[^@/]+@)?([^:/@]+):([^@]+)$")
# Alternate hosts that serve the same repositories as github.com and gitlab.com.
GIT_HOST_ALIASES = {
    "www.github.com": "github.com",
    "ssh.github.com": "github.com",
    "altssh.gitlab.com": "gitlab.com",
}
