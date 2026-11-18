### Capabilities

#### Common Use Cases

##### Ingest a shared drive under an existing DataHub document

```yaml
source:
  type: google-drive
  config:
    root_folder_ids:
      - "0AXyZsharedDriveId00"
    parent_document_urn: "urn:li:document:team-handbook"
```

##### Ingest editable copies instead of read-only references

`document_import_mode: NATIVE` imports Docs as editable DataHub documents. The default, `EXTERNAL`, imports read-only references that link back to Google Drive.

##### Ingest only the top level of a folder

```yaml
source:
  type: google-drive
  config:
    root_folder_ids:
      - "1AbCdEfGhIjKlMnOpQrStUv"
    recursive: false
```

#### Filtering

- `folder_pattern` regexes are matched against the folder path, starting with the root folder's name (for example `Handbook/Engineering/Archive`). A denied folder is never listed, so nothing beneath it is ingested. Configured roots are never filtered.
- `document_pattern` regexes are matched against Doc titles.

#### How Change Detection Works

On each run the connector lists every configured folder, then compares each Doc's Drive version, title, parent folder, and the connector settings that shape its output with what DataHub already stores. Unchanged Docs are not exported again, which keeps re-runs cheap. Docs that were deleted, trashed, moved out of scope, or newly filtered out are soft-deleted when stateful ingestion is enabled.

#### Probe support

`datahub recipe probe` checks a recipe against Google Drive without running ingestion. It returns metadata only, never a Doc's text.

- `identity`: the account the recipe connects as and, when `delegated_user` is set, whether domain-wide delegation is in effect.
- `roots`: each configured root's name and kind (My Drive folder, shared drive, or a folder in a shared drive), or why it cannot be opened and how to fix it.
- `shared_drives`: the shared drives the account can see, with the ID to use in `root_folder_ids`.
- `scope`: what a run would pick up, counted the way ingestion counts it: folders and Docs found, how many each pattern skips, and whether `max_documents` would stop the run.
- `folders`: every folder under the roots, by the path `folder_pattern` is matched against, including folders the recipe skips.
- `documents --folder <path>`: the Google Docs in one folder, by title, including Docs `document_pattern` skips.

`datahub recipe probe filter` explains each verdict, and needs no connection. Folders are judged as `Folder` and Docs as `Google Doc`, together with the folders above them:

```shell
datahub recipe probe methods --recipe google_drive_recipe.yml
datahub recipe probe run roots --recipe google_drive_recipe.yml
datahub recipe probe run folders --recipe google_drive_recipe.yml --report-to folders.json
datahub recipe probe filter --recipe google_drive_recipe.yml --from-run folders.json
datahub recipe probe filter --recipe google_drive_recipe.yml --kind "Google Doc" \
  --parent "Handbook/Engineering" --name "Copy of Runbook"
```

- `explain --document <ID or URL>`: whether one file would be ingested, and if not, which rule decides. It applies the folder and title rules first, then exports the Doc to check the text-length rules (empty, shorter than `min_text_length`, or longer than `max_text_length`). It reports the text's length, never the text, and names the file only when it is under a configured root.

`explain` is the only command that exports a Doc. The listings and `probe filter` judge folders and titles only, so a Doc they keep can still be skipped for its text. None of them account for `max_documents`, which stops a run once that many Docs are embedded; `scope` says whether a run can reach it.

When Google refuses a token, for example because domain-wide delegation is not authorized, the probe exits with code 3 and says what to change.

### Limitations

- Only Google Docs are ingested. Sheets, Slides, PDFs, uploaded files, and shortcuts are skipped.
- Images are not ingested. Each embedded image is replaced with a text placeholder such as `[Image: <alt text>]`.
- Docs with multiple tabs are ingested as a single Document. Each tab begins with its title as a heading.
- Comments, suggestions, and some smart chips are not part of Google's Markdown export.
- Google Drive refuses to export Docs larger than 10 MB. Those Docs are skipped with a warning.
- Docs whose text is longer than `max_text_length` characters (default 1,000,000) are skipped with a warning. If such a Doc was ingested earlier and stateful ingestion is enabled, it is soft-deleted. Set `max_text_length: 0` to remove the limit.
- A run stops with a failure once `max_documents` Docs (default 10,000) have been embedded. Raise the limit, or set it to `0` to remove it, for larger folders.
- Permissions are not mirrored. Anyone who can read the Document in DataHub can read its text, so choose roots whose audience matches your DataHub audience.
- Files in shared drives have no owners, so they are ingested without ownership.
- Drive owners are added alongside any owners you assign in DataHub. When a Doc's Drive owner changes, only the owner this source added is removed.
- Each run lists every folder in scope. Unchanged Docs are not exported again, and unchanged folders are not written again.
- Re-ingesting a changed Doc or folder replaces its related assets and related documents, so links you add to them in DataHub are not kept.
- Do not point two recipes at overlapping folders. With stateful ingestion enabled, one recipe can soft-delete Documents that the other still ingests.

### Troubleshooting

#### A root folder is reported as not accessible

Either the ID is wrong or the source cannot see the folder.

- Check that the ID or URL in `root_folder_ids` points to a folder or shared drive, not a file.
- Share the folder with the account the source authenticates as: the service account's `client_email` when you set `credential`, or the Application Default Credentials account otherwise. Alternatively, set `delegated_user` to a user who can open it.

Run `datahub check source-connection` to see which roots fail, or `datahub recipe probe run roots --recipe <recipe>` to see why each one fails and how to fix it.

When any root or folder fails, the run reports a failure and stale-entity removal is skipped for that run, so nothing is soft-deleted because of a partial listing.

#### Docs are missing

- Check `documents_filtered`, `folders_filtered`, `documents_skipped_empty`, `documents_skipped_too_long`, `documents_export_failed`, `documents_unparseable`, `documents_failed`, and `shortcuts_skipped` in the ingestion report. `failed_documents` lists the Docs that could not be ingested; their existing copies in DataHub are kept.
- Without `delegated_user`, the service account only sees files shared with it. Files that a folder's owner can see are not automatically visible to it.
- Docs in the trash are never ingested.

#### Embeddings are missing for some Docs

If embedding generation fails, the Doc is still ingested and the failure is reported as a warning. The Doc is retried on the next run. `documents_embedding_deferred` counts these Docs, and `num_documents_truncated_oversized` and `num_documents_dropped_oversized` count Docs whose embeddings were truncated or dropped for size.

#### Every run exports every Doc again

Unchanged Docs are skipped by comparing them with what DataHub already stores. If `change_detection_lookup_failures` is non-zero, the ingestion token could not read Documents back from DataHub; grant it read access to Document entities.
