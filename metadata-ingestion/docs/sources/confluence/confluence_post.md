### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

:::caution Not Supported with Remote Executor
This source is not supported with the Remote Executor in DataHub Cloud. It must be run using a self-hosted ingestion setup.
:::

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

#### Limitations and Considerations

##### Confluence API Limits

- **Rate Limits**: Confluence enforces rate limits (Cloud: varies by plan, Data Center: configurable)
- **Content Types**: Complex macros may not extract perfectly (e.g., embedded content, custom macros)
- **Attachments**: File attachments are not ingested (only page content)

##### Performance Considerations

- **Large Spaces**: First run may take significant time for large spaces (1000+ pages)
- **Embedding Generation**: Adds processing time proportional to content volume
- **API Costs**: Embedding providers may incur costs based on usage

##### Content Extraction

- **Supported Content**: Text, headings, lists, code blocks, tables, panels
- **Limited Support**: Some macros extract as text/links
- **Not Supported**: Attachments, complex custom macros, embedded Jira issues (content only)

### Troubleshooting

#### Common Issues

**"401 Unauthorized" or "Authentication failed" errors:**

- **Cloud**: Verify `username` (email) and `api_token` are correct
- **Data Center**: Verify `personal_access_token` is valid and not expired
- Check that `cloud: true/false` matches your Confluence type
- Ensure the URL includes `/wiki` suffix for Cloud (e.g., `https://domain.atlassian.net/wiki`)

**"403 Forbidden" or "Space not found" errors:**

- Verify the user has read access to the specified spaces
- Check that space keys are correct (case-sensitive)
- For Cloud, ensure user is added to private spaces
- For Data Center, verify "View Space" permissions

**Empty or missing content:**

- Verify pages contain text (empty pages are skipped by default with `skip_empty_documents: true`)
- Check `min_text_length` filter setting (default: 50 characters)
- Ensure `recursive: true` if expecting child pages
- Check that pages are not restricted or have special permissions

**Slow ingestion:**

- Increase `processing.parallelism.num_processes` (default: 2)
- Consider filtering specific spaces instead of all spaces
- First run is always slower - subsequent runs use incremental updates
- Large spaces with 1000+ pages may take several minutes

**Embedding generation failures:**

- Verify provider API key is correct
- Check provider-specific rate limits (Cohere: 10k requests/min)
- Ensure embedding model name is valid for your provider
- For Bedrock: verify IAM permissions and model access is enabled in AWS Console

**Stateful ingestion not working:**

- Ensure `stateful_ingestion.enabled: true` in config
- Check DataHub connection (source needs to query previous state)
- Verify state file path is writable (if using file-based state)
- Look for state persistence logs in ingestion output

**Missing hierarchy/parent relationships:**

- Verify `hierarchy.enabled: true` (default)
- Check that parent pages are being ingested
- Ensure `recursive: true` to discover parent-child relationships
- Parent pages must be accessible to the API credentials

**Page IDs not working:**

- For Cloud, use the numeric page ID from the URL (after `/pages/`)
- For Data Center, page IDs may differ - use the ID from the page URL or query param `?pageId=`
- Alternatively, use full page URLs instead of IDs in `page_allow` or `page_deny`

**How to find space keys and page IDs:**

- **Space key**: Visible in the space URL: `https://domain.atlassian.net/wiki/spaces/ENGINEERING` → key is `ENGINEERING`
- **Page ID (Cloud)**: In the page URL after `/pages/`: `https://domain.atlassian.net/wiki/spaces/ENG/pages/123456/Title` → ID is `123456`
- **Page ID (Data Center)**: In the URL query parameter: `https://confluence.company.com/pages/viewpage.action?pageId=123456` → ID is `123456`
- **Personal space key**: Format is `~username` (e.g., `~john.doe` for user john.doe)

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
