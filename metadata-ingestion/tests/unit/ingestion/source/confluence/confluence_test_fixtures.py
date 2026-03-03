"""
Mock Confluence client and realistic test data for comprehensive testing.

This module provides:
1. MockConfluenceClient - A complete mock of atlassian.Confluence
2. Realistic page data with HTML content, metadata, and relationships
3. Test scenarios: deep nesting, pagination, cycles, error cases
"""

from typing import Any, Dict, Generator, List, Optional, Tuple, cast
from unittest.mock import MagicMock


class MockConfluenceClient:
    """
    Mock implementation of atlassian.Confluence client.

    Simulates a Confluence instance with:
    - Multiple spaces (TEAM, DOCS, PUBLIC)
    - Hierarchical page structures
    - Pagination support
    - Error scenarios
    """

    def __init__(self, scenario: str = "default"):
        """
        Initialize mock client with a specific test scenario.

        Args:
            scenario: Test scenario to simulate
                - "default": Standard hierarchy with 3 spaces
                - "deep_nesting": 5-level deep page hierarchy
                - "large_space": Space with 250 pages (tests pagination)
                - "circular_refs": Pages with circular parent references
                - "empty": No spaces or pages
                - "api_errors": Simulates API failures
        """
        self.scenario = scenario
        self.spaces = self._create_spaces()
        self.pages = self._create_pages()
        self.api_calls: List[
            Tuple[str, Dict[str, Any]]
        ] = []  # Track API calls for assertions

    def _create_spaces(self) -> Dict[str, Dict[str, Any]]:
        """Create mock space data."""
        if self.scenario == "empty":
            return {}

        return {
            "TEAM": {
                "key": "TEAM",
                "name": "Team Space",
                "type": "global",
                "_links": {"webui": "/spaces/TEAM"},
            },
            "DOCS": {
                "key": "DOCS",
                "name": "Documentation Space",
                "type": "global",
                "_links": {"webui": "/spaces/DOCS"},
            },
            "PUBLIC": {
                "key": "PUBLIC",
                "name": "Public Space",
                "type": "global",
                "_links": {"webui": "/spaces/PUBLIC"},
            },
        }

    def _create_pages(self) -> Dict[str, Dict[str, Any]]:
        """Create mock page data with realistic content and hierarchies."""
        if self.scenario == "empty":
            return {}

        pages = {}

        # TEAM Space - Simple hierarchy
        pages["10001"] = {
            "id": "10001",
            "type": "page",
            "status": "current",
            "title": "Team Home",
            "space": {"key": "TEAM", "name": "Team Space"},
            "version": {"number": 5, "when": "2024-01-15T10:00:00.000Z"},
            "ancestors": cast(List[Dict[str, str]], []),
            "body": {
                "storage": {
                    "value": "<h1>Welcome to Team Space</h1><p>This is our team collaboration hub.</p>",
                    "representation": "storage",
                }
            },
            "_links": {
                "webui": "/spaces/TEAM/pages/10001/Team+Home",
                "self": "https://example.atlassian.net/wiki/rest/api/content/10001",
            },
        }

        pages["10002"] = {
            "id": "10002",
            "type": "page",
            "status": "current",
            "title": "Meeting Notes",
            "space": {"key": "TEAM", "name": "Team Space"},
            "version": {"number": 12, "when": "2024-01-20T14:30:00.000Z"},
            "ancestors": [{"id": "10001", "title": "Team Home"}],
            "body": {
                "storage": {
                    "value": "<h2>Weekly Sync - Jan 2024</h2><ul><li>Action item 1</li><li>Action item 2</li></ul>",
                    "representation": "storage",
                }
            },
            "_links": {"webui": "/spaces/TEAM/pages/10002/Meeting+Notes"},
        }

        # DOCS Space - Deep hierarchy for API documentation
        pages["20001"] = {
            "id": "20001",
            "type": "page",
            "status": "current",
            "title": "API Documentation",
            "space": {"key": "DOCS", "name": "Documentation Space"},
            "version": {"number": 3, "when": "2024-01-10T09:00:00.000Z"},
            "ancestors": cast(List[Dict[str, str]], []),
            "body": {
                "storage": {
                    "value": "<h1>API Documentation</h1><p>Complete API reference for our platform.</p>",
                    "representation": "storage",
                }
            },
            "_links": {"webui": "/spaces/DOCS/pages/20001/API+Documentation"},
        }

        pages["20002"] = {
            "id": "20002",
            "type": "page",
            "status": "current",
            "title": "REST API",
            "space": {"key": "DOCS", "name": "Documentation Space"},
            "version": {"number": 8, "when": "2024-01-12T11:00:00.000Z"},
            "ancestors": [{"id": "20001", "title": "API Documentation"}],
            "body": {
                "storage": {
                    "value": "<h2>REST API Reference</h2><p>Our RESTful API endpoints and examples.</p><h3>Authentication</h3><p>Use API tokens for authentication.</p>",
                    "representation": "storage",
                }
            },
            "_links": {"webui": "/spaces/DOCS/pages/20002/REST+API"},
        }

        pages["20003"] = {
            "id": "20003",
            "type": "page",
            "status": "current",
            "title": "Authentication",
            "space": {"key": "DOCS", "name": "Documentation Space"},
            "version": {"number": 4, "when": "2024-01-13T10:00:00.000Z"},
            "ancestors": [
                {"id": "20001", "title": "API Documentation"},
                {"id": "20002", "title": "REST API"},
            ],
            "body": {
                "storage": {
                    "value": "<h3>Authentication Methods</h3><ul><li>API Key</li><li>OAuth 2.0</li><li>JWT</li></ul>",
                    "representation": "storage",
                }
            },
            "_links": {"webui": "/spaces/DOCS/pages/20003/Authentication"},
        }

        pages["20004"] = {
            "id": "20004",
            "type": "page",
            "status": "current",
            "title": "OAuth 2.0 Guide",
            "space": {"key": "DOCS", "name": "Documentation Space"},
            "version": {"number": 2, "when": "2024-01-14T15:00:00.000Z"},
            "ancestors": [
                {"id": "20001", "title": "API Documentation"},
                {"id": "20002", "title": "REST API"},
                {"id": "20003", "title": "Authentication"},
            ],
            "body": {
                "storage": {
                    "value": "<h4>OAuth 2.0 Implementation</h4><p>Step-by-step guide to implement OAuth 2.0.</p><code>client_id=xxx&client_secret=yyy</code>",
                    "representation": "storage",
                }
            },
            "_links": {"webui": "/spaces/DOCS/pages/20004/OAuth+2.0+Guide"},
        }

        pages["20005"] = {
            "id": "20005",
            "type": "page",
            "status": "current",
            "title": "Endpoints",
            "space": {"key": "DOCS", "name": "Documentation Space"},
            "version": {"number": 6, "when": "2024-01-15T12:00:00.000Z"},
            "ancestors": [
                {"id": "20001", "title": "API Documentation"},
                {"id": "20002", "title": "REST API"},
            ],
            "body": {
                "storage": {
                    "value": "<h3>API Endpoints</h3><ul><li>GET /api/users</li><li>POST /api/users</li><li>DELETE /api/users/:id</li></ul>",
                    "representation": "storage",
                }
            },
            "_links": {"webui": "/spaces/DOCS/pages/20005/Endpoints"},
        }

        pages["20006"] = {
            "id": "20006",
            "type": "page",
            "status": "current",
            "title": "GraphQL API",
            "space": {"key": "DOCS", "name": "Documentation Space"},
            "version": {"number": 3, "when": "2024-01-16T09:30:00.000Z"},
            "ancestors": [{"id": "20001", "title": "API Documentation"}],
            "body": {
                "storage": {
                    "value": "<h2>GraphQL API</h2><p>Query our data using GraphQL.</p><pre>query { user { name email } }</pre>",
                    "representation": "storage",
                }
            },
            "_links": {"webui": "/spaces/DOCS/pages/20006/GraphQL+API"},
        }

        # PUBLIC Space - Getting Started guide
        pages["30001"] = {
            "id": "30001",
            "type": "page",
            "status": "current",
            "title": "Getting Started",
            "space": {"key": "PUBLIC", "name": "Public Space"},
            "version": {"number": 10, "when": "2024-01-18T08:00:00.000Z"},
            "ancestors": cast(List[Dict[str, str]], []),
            "body": {
                "storage": {
                    "value": "<h1>Getting Started Guide</h1><p>Welcome! Here's how to get started with our platform.</p>",
                    "representation": "storage",
                }
            },
            "_links": {"webui": "/spaces/PUBLIC/pages/30001/Getting+Started"},
        }

        pages["30002"] = {
            "id": "30002",
            "type": "page",
            "status": "current",
            "title": "Installation",
            "space": {"key": "PUBLIC", "name": "Public Space"},
            "version": {"number": 5, "when": "2024-01-19T10:00:00.000Z"},
            "ancestors": [{"id": "30001", "title": "Getting Started"}],
            "body": {
                "storage": {
                    "value": "<h2>Installation Instructions</h2><p>Install via pip:</p><code>pip install our-package</code>",
                    "representation": "storage",
                }
            },
            "_links": {"webui": "/spaces/PUBLIC/pages/30002/Installation"},
        }

        pages["30003"] = {
            "id": "30003",
            "type": "page",
            "status": "current",
            "title": "Configuration",
            "space": {"key": "PUBLIC", "name": "Public Space"},
            "version": {"number": 7, "when": "2024-01-20T11:00:00.000Z"},
            "ancestors": [{"id": "30001", "title": "Getting Started"}],
            "body": {
                "storage": {
                    "value": "<h2>Configuration Guide</h2><p>Configure your installation with these settings.</p>",
                    "representation": "storage",
                }
            },
            "_links": {"webui": "/spaces/PUBLIC/pages/30003/Configuration"},
        }

        # Add deep nesting scenario
        if self.scenario == "deep_nesting":
            pages["40001"] = {
                "id": "40001",
                "type": "page",
                "status": "current",
                "title": "Level 1",
                "space": {"key": "DOCS", "name": "Documentation Space"},
                "version": {"number": 1, "when": "2024-01-21T09:00:00.000Z"},
                "ancestors": cast(List[Dict[str, str]], []),
                "body": {"storage": {"value": "<p>Level 1 content</p>"}},
                "_links": {"webui": "/spaces/DOCS/pages/40001/Level+1"},
            }
            for i in range(2, 6):  # Create 5 levels
                page_id = f"4000{i}"
                ancestors: List[Dict[str, str]] = [
                    {"id": f"4000{j}", "title": f"Level {j}"} for j in range(1, i)
                ]
                pages[page_id] = {
                    "id": page_id,
                    "type": "page",
                    "status": "current",
                    "title": f"Level {i}",
                    "space": {"key": "DOCS", "name": "Documentation Space"},
                    "version": {"number": 1, "when": f"2024-01-21T0{i}:00:00.000Z"},
                    "ancestors": ancestors,
                    "body": {"storage": {"value": f"<p>Level {i} content</p>"}},
                    "_links": {"webui": f"/spaces/DOCS/pages/{page_id}/Level+{i}"},
                }

        # Add large space scenario for pagination testing
        if self.scenario == "large_space":
            for i in range(1, 251):  # 250 pages
                page_id = f"5{i:04d}"
                pages[page_id] = {
                    "id": page_id,
                    "type": "page",
                    "status": "current",
                    "title": f"Page {i}",
                    "space": {"key": "LARGE", "name": "Large Space"},
                    "version": {"number": 1, "when": "2024-01-22T10:00:00.000Z"},
                    "ancestors": cast(List[Dict[str, str]], []),
                    "body": {"storage": {"value": f"<p>Content for page {i}</p>"}},
                    "_links": {"webui": f"/spaces/LARGE/pages/{page_id}/Page+{i}"},
                }

        return pages

    def get_all_spaces(
        self, start: int = 0, limit: int = 25, expand: Optional[str] = None
    ) -> Dict[str, Any]:
        """Mock get_all_spaces API call."""
        self.api_calls.append(("get_all_spaces", {"start": start, "limit": limit}))

        if self.scenario == "api_errors" and start == 0:
            raise Exception("API Error: Rate limit exceeded")

        spaces_list = list(self.spaces.values())
        end = min(start + limit, len(spaces_list))

        return {
            "results": spaces_list[start:end],
            "start": start,
            "limit": limit,
            "size": len(spaces_list[start:end]),
            "_links": {
                "next": f"/rest/api/space?start={end}&limit={limit}"
                if end < len(spaces_list)
                else None
            },
        }

    def get_page_by_id(
        self, page_id: str, expand: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Mock get_page_by_id API call."""
        self.api_calls.append(
            ("get_page_by_id", {"page_id": page_id, "expand": expand})
        )

        if self.scenario == "api_errors" and page_id == "20001":
            raise Exception("API Error: Page not found")

        return self.pages.get(page_id)

    def get_child_pages(
        self, page_id: str, start: int = 0, limit: int = 25
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Mock get_child_pages API call.
        Returns a generator of child pages.
        """
        self.api_calls.append(
            ("get_child_pages", {"page_id": page_id, "start": start, "limit": limit})
        )

        # Find all pages where this page is the direct parent
        children = []
        for page in self.pages.values():
            ancestors = page.get("ancestors", [])
            if ancestors and ancestors[-1].get("id") == page_id:
                children.append(page)

        # Return as generator (matching real API behavior)
        for child in children:
            yield child

    def get_all_pages_from_space(
        self, space: str, start: int = 0, limit: int = 100, expand: Optional[str] = None
    ) -> Dict[str, Any]:
        """Mock get_all_pages_from_space API call with pagination."""
        self.api_calls.append(
            (
                "get_all_pages_from_space",
                {"space": space, "start": start, "limit": limit},
            )
        )

        if self.scenario == "api_errors" and space == "TEAM":
            raise Exception("API Error: Space not accessible")

        # Filter pages by space
        space_pages = [
            p for p in self.pages.values() if p.get("space", {}).get("key") == space
        ]

        # Sort by ID for consistent pagination
        space_pages.sort(key=lambda p: p["id"])

        end = min(start + limit, len(space_pages))

        return {
            "results": space_pages[start:end],
            "start": start,
            "limit": limit,
            "size": len(space_pages[start:end]),
            "_links": {
                "next": f"/rest/api/space/{space}/content/page?start={end}&limit={limit}"
                if end < len(space_pages)
                else None
            },
        }


def create_mock_confluence_client(scenario: str = "default") -> MagicMock:
    """
    Create a MagicMock configured to behave like the Confluence client.

    Args:
        scenario: Test scenario (default, deep_nesting, large_space, etc.)

    Returns:
        MagicMock configured with MockConfluenceClient methods
    """
    mock_client_impl = MockConfluenceClient(scenario=scenario)
    mock_client = MagicMock()

    # Wire up the methods
    mock_client.get_all_spaces = mock_client_impl.get_all_spaces
    mock_client.get_page_by_id = mock_client_impl.get_page_by_id
    mock_client.get_child_pages = mock_client_impl.get_child_pages
    mock_client.get_all_pages_from_space = mock_client_impl.get_all_pages_from_space

    # Expose internal state for test assertions
    mock_client._test_impl = mock_client_impl

    return mock_client


def load_fixture_from_folder(fixture_name: str) -> MockConfluenceClient:
    """
    Load a Confluence repository fixture from a folder structure.

    Fixture structure:
        fixtures/
          fixture_name/
            SPACE_KEY/              # Folder for each space
              _space.json           # Space metadata
              PAGE_ID.md            # Page files (markdown with YAML frontmatter)

    Args:
        fixture_name: Name of the fixture folder

    Returns:
        MockConfluenceClient configured with fixture data
    """
    import json
    from pathlib import Path

    import yaml

    # Get the fixtures directory relative to this file
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixture_path = fixtures_dir / fixture_name

    if not fixture_path.exists():
        raise FileNotFoundError(f"Fixture not found: {fixture_path}")

    # Create a mock client with empty scenario
    mock_client = MockConfluenceClient(scenario="empty")

    # Load spaces and pages
    mock_client.spaces = {}
    mock_client.pages = {}

    # Iterate through space folders
    for space_dir in fixture_path.iterdir():
        if not space_dir.is_dir():
            continue

        space_key = space_dir.name

        # Load space metadata
        space_metadata_file = space_dir / "_space.json"
        if space_metadata_file.exists():
            with open(space_metadata_file) as f:
                space_metadata = json.load(f)
        else:
            # Default space metadata if not provided
            space_metadata = {
                "key": space_key,
                "name": space_key,
                "type": "global",
            }

        # Add standard fields
        space_metadata["_links"] = {"webui": f"/spaces/{space_key}"}
        mock_client.spaces[space_key] = space_metadata

        # Load pages from markdown files
        for page_file in space_dir.iterdir():
            if not page_file.is_file() or page_file.suffix != ".md":
                continue

            # Read markdown file with YAML frontmatter
            with open(page_file) as f:
                content = f.read()

            # Split frontmatter and body
            if content.startswith("---\n"):
                parts = content.split("---\n", 2)
                if len(parts) >= 3:
                    frontmatter = yaml.safe_load(parts[1])
                    body_content = parts[2].strip()
                else:
                    continue
            else:
                continue

            # Build page object
            page_id = frontmatter["id"]
            page = {
                "id": page_id,
                "type": "page",
                "status": frontmatter.get("status", "current"),
                "title": frontmatter["title"],
                "space": {
                    "key": space_key,
                    "name": space_metadata["name"],
                },
                "version": frontmatter.get("version", {"number": 1}),
                "ancestors": frontmatter.get("ancestors", []),
                "body": {
                    "storage": {
                        "value": f"<p>{body_content}</p>",  # Convert markdown to simple HTML
                        "representation": "storage",
                    }
                },
                "_links": {
                    "webui": f"/spaces/{space_key}/pages/{page_id}/{frontmatter['title'].replace(' ', '+')}",
                    "self": f"https://example.atlassian.net/wiki/rest/api/content/{page_id}",
                },
            }

            mock_client.pages[page_id] = page

    return mock_client


def create_mock_confluence_client_from_fixture(fixture_name: str) -> MagicMock:
    """
    Create a MagicMock configured with data from a folder-based fixture.

    Args:
        fixture_name: Name of the fixture folder

    Returns:
        MagicMock configured with fixture data
    """
    mock_client_impl = load_fixture_from_folder(fixture_name)
    mock_client = MagicMock()

    # Wire up the methods
    mock_client.get_all_spaces = mock_client_impl.get_all_spaces
    mock_client.get_page_by_id = mock_client_impl.get_page_by_id
    mock_client.get_child_pages = mock_client_impl.get_child_pages
    mock_client.get_all_pages_from_space = mock_client_impl.get_all_pages_from_space

    # Expose internal state for test assertions
    mock_client._test_impl = mock_client_impl

    return mock_client
