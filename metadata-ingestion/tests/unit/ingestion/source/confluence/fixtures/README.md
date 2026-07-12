# Confluence Test Fixtures

This directory contains filesystem-based test fixtures for the Confluence source connector.

## Structure

Each fixture is a folder representing a complete Confluence instance:

```
fixtures/
  fixture_name/
    SPACE_KEY/              # Folder for each Confluence space
      _space.json           # Space metadata
      PAGE_ID.md            # Page files (markdown with YAML frontmatter)
```

### Space Metadata (`_space.json`)

Each space folder contains a `_space.json` file with space metadata:

```json
{
  "key": "TEAM",
  "name": "Team Space",
  "type": "global"
}
```

### Page Files (`*.md`)

Pages are stored as markdown files named by their page ID (e.g., `10001.md`). Each file contains:

1. **YAML frontmatter** with page metadata
2. **Markdown body** with page content

Example:

```markdown
---
id: "10001"
title: "Team Home"
status: "current"
version:
  number: 5
  when: "2024-01-15T10:00:00.000Z"
ancestors: []
---

# Welcome to Team Space

This is our team collaboration hub.
```

#### Frontmatter Fields

- `id` (required): Page ID as a string
- `title` (required): Page title
- `status` (optional): Page status (default: "current")
- `version` (optional): Version information with `number` and `when` fields
- `ancestors` (optional): List of ancestor pages (parent hierarchy)
  - Each ancestor has `id` and `title` fields
  - Listed from root to immediate parent

Example with parent hierarchy:

```yaml
ancestors:
  - id: "20001"
    title: "API Documentation"
  - id: "20002"
    title: "REST API"
```

## Default Fixture (`default_repo`)

The `default_repo` fixture contains a realistic Confluence instance with:

### Spaces

- **TEAM**: Team collaboration space (3 pages)
- **DOCS**: Technical documentation (6 pages with deep hierarchy)
- **PUBLIC**: Public-facing documentation (3 pages)
- **~johndoe**: Personal space (1 page)
- **ARCHIVE**: Archived content (2 pages)

### Page Hierarchy

**TEAM Space:**

```
Team Home (10001)
├── Meeting Notes (10002)
└── Project Plans (10003)
```

**DOCS Space:**

```
API Documentation (20001)
├── REST API (20002)
│   ├── Authentication (20003)
│   │   └── OAuth 2.0 Guide (20004)
│   └── Endpoints (20005)
└── GraphQL API (20006)
```

**PUBLIC Space:**

```
Getting Started (30001)
├── Installation (30002)
└── Configuration (30003)
```

## Usage in Tests

### Loading a Fixture

```python
from tests.unit.ingestion.source.confluence.confluence_test_fixtures import (
    create_mock_confluence_client_from_fixture,
)

# Create a mock client from the fixture
mock_client = create_mock_confluence_client_from_fixture("default_repo")

# Use with patch
with patch("...Confluence") as mock_confluence:
    mock_confluence.return_value = mock_client
    # ... test code
```

### Accessing Fixture Data

```python
# Access internal state for assertions
spaces = mock_client._test_impl.spaces
pages = mock_client._test_impl.pages

# Verify specific page
team_home = pages["10001"]
assert team_home["title"] == "Team Home"
```

## Creating New Fixtures

To create a new test fixture:

1. Create a new folder under `fixtures/`
2. Create space folders for each space
3. Add `_space.json` in each space folder
4. Add `.md` files for each page

Example:

```bash
cd fixtures
mkdir my_fixture
mkdir my_fixture/MYSPACE
```

Create `my_fixture/MYSPACE/_space.json`:

```json
{
  "key": "MYSPACE",
  "name": "My Test Space",
  "type": "global"
}
```

Create `my_fixture/MYSPACE/100.md`:

```markdown
---
id: "100"
title: "Test Page"
status: "current"
ancestors: []
---

This is a test page.
```

## Benefits of This Approach

1. **Source-Neutral**: Structure can be reused for other document sources
2. **Easy to Understand**: Folders and markdown files are intuitive
3. **Easy to Modify**: Add/remove pages by editing files
4. **Version Control Friendly**: Text-based format works well with Git
5. **Readable**: Test data is human-readable and easy to review
6. **Realistic**: Content closely matches actual Confluence structure
7. **No Network Calls**: Tests run fast without API dependencies
