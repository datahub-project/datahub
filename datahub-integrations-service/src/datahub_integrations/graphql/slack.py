from .common import GET_ENTITY_QUERY, SEARCH_QUERY

# Re-export common queries with Slack-specific names for backward compatibility
SLACK_SEARCH_QUERY = SEARCH_QUERY
SLACK_GET_ENTITY_QUERY = GET_ENTITY_QUERY
