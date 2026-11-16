from datahub.utilities.str_enum import StrEnum

# Names from the Confluence Storage Format:
#   https://confluence.atlassian.com/doc/confluence-storage-format-790796544.html
# and the macro reference:
#   https://confluence.atlassian.com/display/DOC/Macros


class StorageTag(StrEnum):
    MACRO = "ac:structured-macro"
    PARAMETER = "ac:parameter"
    RICH_TEXT_BODY = "ac:rich-text-body"
    PLAIN_TEXT_BODY = "ac:plain-text-body"

    LINK = "ac:link"
    LINK_BODY = "ac:link-body"
    PLAIN_TEXT_LINK_BODY = "ac:plain-text-link-body"
    IMAGE = "ac:image"
    EMOTICON = "ac:emoticon"
    PLACEHOLDER = "ac:placeholder"

    TASK_LIST = "ac:task-list"
    TASK = "ac:task"
    TASK_STATUS = "ac:task-status"
    TASK_BODY = "ac:task-body"

    ADF_EXTENSION = "ac:adf-extension"
    ADF_FALLBACK = "ac:adf-fallback"
    ADF_ATTRIBUTE = "ac:adf-attribute"

    USER = "ri:user"
    PAGE = "ri:page"
    BLOG_POST = "ri:blog-post"
    ATTACHMENT = "ri:attachment"
    SPACE = "ri:space"
    URL = "ri:url"

    TIME = "time"


class StorageAttr(StrEnum):
    NAME = "ac:name"
    ALT = "ac:alt"
    TITLE = "ac:title"
    ANCHOR = "ac:anchor"
    EMOJI_FALLBACK = "ac:emoji-fallback"
    EMOJI_ID = "ac:emoji-id"

    ACCOUNT_ID = "ri:account-id"
    USERKEY = "ri:userkey"
    USERNAME = "ri:username"
    CONTENT_TITLE = "ri:content-title"
    FILENAME = "ri:filename"
    SPACE_KEY = "ri:space-key"
    VALUE = "ri:value"

    DATETIME = "datetime"


class MacroName(StrEnum):
    CODE = "code"
    NOFORMAT = "noformat"
    STATUS = "status"
    JIRA = "jira"


class MacroParam(StrEnum):
    LANGUAGE = "language"
    TITLE = "title"
    KEY = "key"
    SERVER = "server"
    SERVER_ID = "serverId"


TASK_STATUS_COMPLETE = "complete"

# Tags in the storage-format namespaces (ac: macros, ri: resource
# identifiers, at: template variables).
NAMESPACED_TAG_PATTERN = r"^(ac|ri|at):"

# Shown in place of a mention whose user cannot be looked up, so the sentence
# still reads as naming someone rather than silently losing a word.
UNRESOLVED_USER_TEXT = "@user"

# Macros that are navigation or UI chrome, or that render content pulled from
# elsewhere at view time. Their storage form carries no authored text.
DISCARD_MACROS = frozenset(
    [
        # Table of contents / navigation
        "toc",
        "table-of-contents",
        "toc-zone",
        "anchor",
        "pagetree",
        "pagetreesearch",
        "page-tree",
        "page-index",
        "navmap",
        # Children / related pages, and content included from other pages,
        # which is ingested from those pages instead.
        "children",
        "children-display",
        "include",
        "include-page",
        "excerpt-include",
        # Dynamic content aggregators (not authored text)
        "recently-updated",
        "recently-updated-dashboard",
        "blogposts",
        "contentbylabel",
        "contentbyuser",
        "content-report-table",
        "taskreport",
        "labels-list",
        "popular-labels",
        "recently-used-labels",
        "global-reports",
        # Attachments / files / media viewers
        "attachments",
        "gallery",
        "pdf",
        "viewfile",
        "multimedia",
        "office-excel",
        "office-powerpoint",
        "office-word",
        # User / collaboration chrome
        "contributors",
        "contributors-summary",
        "profile",
        "user-profile",
        "profilepicture",
        "im",
        "network",
        "favpages",
        # Search / live widgets
        "livesearch",
        "search",
        "rss",
        "rssfeed",
        "chart",
        "jirachart",
        "widget",
        "roadmapplanner",
        "calendar",
        # Template / space scaffolding
        "create-from-template",
        "create-space-button",
    ]
)

# Content API settings shared by every page listing, so pages fetched by
# space, by id and as children all carry the same fields.
PAGE_EXPAND = "ancestors,version,history,space,body.storage"
PAGE_PAGE_SIZE = 100
SPACE_PAGE_SIZE = 100

PAGE_CONTENT_TYPE = "page"
FOLDER_CONTENT_TYPE = "folder"

# Bump when page text extraction changes (conversion, macro handling,
# normalization) so every page is re-extracted and re-embedded on the next run
# even if its Confluence body is unchanged.
EXTRACTION_ALGO_VERSION = "4"

# The rendered page, which Confluence produces with Jira macros already turned
# into links on the right Jira server.
VIEW_EXPAND = "body.view"
JIRA_BROWSE_PATH = "/browse/"
# The Jira servers linked to this Confluence, as listed for the Jira macro.
JIRA_SERVERS_PATH = "rest/jiraanywhere/1.0/servers"
JIRA_ISSUE_KEY_PATTERN = r"/browse/([A-Za-z][A-Za-z0-9_]*-\d+)"

# Searchable field of CorpUserInfo used to match Confluence users to DataHub users.
CORPUSER_EMAIL_FIELD = "email"

# Confluence's built-in emoticons, which older content stores by name only.
EMOTICON_NAMES = {
    "smile": "\U0001f642",
    "sad": "\u2639\ufe0f",
    "cheeky": "\U0001f61b",
    "laugh": "\U0001f603",
    "wink": "\U0001f609",
    "thumbs-up": "\U0001f44d",
    "thumbs-down": "\U0001f44e",
    "information": "\u2139\ufe0f",
    "tick": "\u2705",
    "cross": "\u274c",
    "warning": "\u26a0\ufe0f",
    "plus": "\u2795",
    "minus": "\u2796",
    "question": "\u2753",
    "light-on": "\U0001f4a1",
    "light-off": "\U0001f4a1",
    "yellow-star": "\u2b50",
    "red-star": "\u2b50",
    "green-star": "\u2b50",
    "blue-star": "\u2b50",
    "heart": "\u2764\ufe0f",
    "broken-heart": "\U0001f494",
}
