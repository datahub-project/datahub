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

### Limitations

- Only Google Docs are ingested. Sheets, Slides, PDFs, uploaded files, and shortcuts are skipped.
- Images are not ingested. Each embedded image is replaced with a text placeholder such as `[Image: <alt text>]`.
- Comments, suggestions, and some smart chips are not part of Google's Markdown export.
- Google Drive refuses to export Docs larger than 10 MB. Those Docs are skipped with a warning.
- Permissions are not mirrored. Anyone who can read the Document in DataHub can read its text, so choose roots whose audience matches your DataHub audience.
- Files in shared drives have no owners, so they are ingested without ownership.
- Each run lists every folder in scope. Unchanged Docs are not exported again.
- Do not point two recipes at overlapping folders. With stateful ingestion enabled, one recipe can soft-delete Documents that the other still ingests.

### Troubleshooting

#### A root folder is reported as not accessible

The service account cannot see the folder. Share the folder with the service account's `client_email`, or set `delegated_user` to a user who can open it. Run `datahub check source-connection` to see which roots fail.

When any root or folder fails, the run reports a failure and stale-entity removal is skipped for that run, so nothing is soft-deleted because of a partial listing.

#### Docs are missing

- Check `documents_filtered`, `folders_filtered`, `documents_skipped_empty`, and `shortcuts_skipped` in the ingestion report.
- Without `delegated_user`, the service account only sees files shared with it. Files that a folder's owner can see are not automatically visible to it.
- Docs in the trash are never ingested.

#### Embeddings are missing for some Docs

If embedding generation fails, the Doc is still ingested and the failure is reported as a warning. The Doc is retried on the next run.
