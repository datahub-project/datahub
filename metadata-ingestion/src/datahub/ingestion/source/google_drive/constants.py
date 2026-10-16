import re

PLATFORM = "google_drive"
DOCUMENT_ID_PREFIX = "google_drive."

DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

MIME_FOLDER = "application/vnd.google-apps.folder"
MIME_DOCUMENT = "application/vnd.google-apps.document"
MIME_SHORTCUT = "application/vnd.google-apps.shortcut"
EXPORT_MARKDOWN = "text/markdown"
EXPORT_HTML = "text/html"
MARKDOWNIFY_HEADING_STYLE = "ATX"

DRIVE_API_NAME = "drive"
DRIVE_API_VERSION = "v3"
SHARED_DRIVE_URL_TEMPLATE = "https://drive.google.com/drive/folders/{drive_id}"

STAGE_DISCOVERY = "Discover Drive folders and Google Docs"
STAGE_FOLDERS = "Emit folder documents"
STAGE_DOCUMENTS = "Ingest Google Docs"
PROGRESS_LOG_INTERVAL = 100
# Stored documents are read back from DataHub in pages of this size for change
# detection, instead of one request per document.
CHANGE_DETECTION_BATCH_SIZE = 100
CAPABILITY_SEMANTIC_SEARCH = "Semantic Search"

# Bump when export cleaning or text normalisation changes, to force a re-export of
# every document on the next run even if its Drive version is unchanged.
EXTRACTION_ALGO_VERSION = "2"

API_MAX_RETRIES = 5
LIST_PAGE_SIZE = 1000

FILE_FIELDS = (
    "id,name,mimeType,driveId,version,size,createdTime,modifiedTime,"
    "webViewLink,owners(emailAddress),lastModifyingUser(emailAddress)"
)
LIST_FIELDS = f"nextPageToken,files({FILE_FIELDS})"
FILE_WITH_PARENTS_FIELDS = f"{FILE_FIELDS},parents,trashed"
# Folder levels `explain` climbs from a Doc looking for a configured root.
MAX_FOLDER_DEPTH = 100
ABOUT_FIELDS = "user(emailAddress)"
DRIVE_FIELDS = "id,name,createdTime"
DRIVES_LIST_FIELDS = f"nextPageToken,drives({DRIVE_FIELDS})"
# The largest page drives.list accepts.
DRIVES_PAGE_SIZE = 100
ORDER_BY = "folder,name"

CHILDREN_QUERY_TEMPLATE = (
    "'{folder_id}' in parents and trashed=false and ("
    f"mimeType='{MIME_FOLDER}' or mimeType='{MIME_DOCUMENT}' "
    f"or mimeType='{MIME_SHORTCUT}')"
)

API_FILES_LIST = "files.list"
API_FILES_GET = "files.get"
API_FILES_EXPORT = "files.export"
API_DRIVES_GET = "drives.get"
API_DRIVES_LIST = "drives.list"
API_ABOUT_GET = "about.get"

PROP_FILE_ID = "drive_file_id"
PROP_MIME_TYPE = "drive_mime_type"
PROP_FOLDER_PATH = "drive_folder_path"
PROP_PARENT_ID = "drive_parent_id"
PROP_DRIVE_ID = "drive_id"
PROP_VERSION = "drive_version"
PROP_SIZE_BYTES = "drive_size_bytes"
PROP_INGEST_FINGERPRINT = "drive_ingest_fingerprint"
PROP_LAST_MODIFYING_USER = "last_modifying_user"
PROP_CONTENT_HASH = "content_hash"
PROP_EXTRACTION_ALGO_VERSION = "extraction_algo_version"

FOLDER_PATH_SEPARATOR = "/"

ELEMENT_TYPE_NARRATIVE_TEXT = "NarrativeText"
DOCUMENT_ENTITY_TYPE = "document"
ASPECT_DOCUMENT_INFO = "documentInfo"
ASPECT_STATUS = "status"
ASPECT_OWNERSHIP = "ownership"
OWNERSHIP_ATTRIBUTION_ACTOR = "urn:li:corpuser:datahub"
DOCUMENT_URN_PREFIX = "urn:li:document:"

FAILURE_NOT_FOUND = "not_found"
FAILURE_TOO_LARGE = "too_large"
FAILURE_EXPORT_FAILED = "export_failed"

DRIVE_URL_FOLDER_ID_RE = re.compile(r"/folders/([A-Za-z0-9_-]+)")
DRIVE_URL_QUERY_ID_RE = re.compile(r"[?&]id=([A-Za-z0-9_-]+)")
# https://docs.google.com/document/d/<id>/edit and https://drive.google.com/file/d/<id>
DRIVE_URL_FILE_ID_RE = re.compile(r"/d/([A-Za-z0-9_-]+)")
DRIVE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{10,}$")
PLATFORM_INSTANCE_RE = re.compile(r"^[a-zA-Z0-9_-]+$")

# Google Docs' markdown export embeds images as base64 data URIs, either inline or
# as reference definitions at the end of the document.
INLINE_DATA_IMAGE_RE = re.compile(r"!\[([^\]]*)\]\(<?data:[^)]*\)")
REFERENCE_DATA_IMAGE_RE = re.compile(
    r"^\[([^\]]+)\]:\s*<?data:[^\s>]*>?[ \t]*$", re.MULTILINE
)
REFERENCE_IMAGE_USE_RE = re.compile(r"!\[([^\]]*)\]\[([^\]]+)\]")
# Stripped images become plain text rather than `![alt]()`: an image with an empty
# source renders as a broken-image icon in the DataHub UI.
IMAGE_PLACEHOLDER = "*[Image]*"
IMAGE_PLACEHOLDER_WITH_ALT = "*[Image: {alt}]*"
EXCESS_BLANK_LINES_RE = re.compile(r"\n{3,}")

EXPORT_TOO_LARGE_REASON = "exportSizeLimitExceeded"
HTTP_FORBIDDEN = 403
HTTP_NOT_FOUND = 404
# Drive answers quota exhaustion with 403 too; that is not an access problem.
RATE_LIMIT_REASONS = frozenset({"rateLimitExceeded", "userRateLimitExceeded"})

# OAuth token-endpoint error codes (RFC 6749 section 5.2) that a service account
# hits during setup.
OAUTH_UNAUTHORIZED_CLIENT = "unauthorized_client"
OAUTH_INVALID_GRANT = "invalid_grant"

# Docs carry no subtype, so the probe names their kind itself; folders use
# DocumentSubTypes.FOLDER, as ingestion does.
KIND_GOOGLE_DOC = "Google Doc"
# Scalar facts on probe listing records, which `probe filter --from-run` hands back
# to the verdict override: the folder paths above a folder, and the folder paths
# down to a Doc's folder, root first and newline-separated (a Drive name has none).
ATTR_ANCESTORS = "ancestors"
ATTR_FOLDERS = "folders"
CHAIN_SEPARATOR = "\n"

ROOT_KIND_SHARED_DRIVE = "shared_drive"
ROOT_KIND_SHARED_DRIVE_FOLDER = "shared_drive_folder"
ROOT_KIND_MY_DRIVE_FOLDER = "my_drive_folder"
ROOT_PROBLEM_NOT_FOUND = "not_found_or_not_shared"
ROOT_PROBLEM_FORBIDDEN = "forbidden"
ROOT_PROBLEM_NOT_A_FOLDER = "not_a_folder"
CREDENTIAL_SERVICE_ACCOUNT = "service_account"
CREDENTIAL_APPLICATION_DEFAULT = "application_default"
