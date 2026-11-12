# Moved to datahub.ingestion.source.repository_documents.okf_frontmatter; this module keeps
# the old import path working for code built on github-documents.
from datahub.ingestion.source.repository_documents.okf_frontmatter import (
    MAX_FRONTMATTER_BYTES as MAX_FRONTMATTER_BYTES,
    OkfFrontmatter as OkfFrontmatter,
    parse_frontmatter as parse_frontmatter,
    parse_okf_frontmatter as parse_okf_frontmatter,
)
