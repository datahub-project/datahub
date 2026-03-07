### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

:::caution Not Supported with Remote Executor
This source is not supported with the Remote Executor in DataHub Cloud. It must be run using a self-hosted ingestion setup.
:::

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

#### Limitations and Considerations

##### Notion API Limits

- **Rate Limits**: Notion enforces rate limits (3 requests/second for paid workspaces, 1/second for free)
- **Access Scope**: Integration only sees explicitly shared pages
- **Content Types**: Some Notion blocks may not extract perfectly (e.g., complex embeds, synced blocks)

##### Performance Considerations

- **Large Workspaces**: First run may take significant time for large workspaces
- **Embedding Generation**: Adds processing time proportional to content volume
- **API Costs**: Unstructured API and embedding providers may incur costs

##### Content Extraction

- **Supported Blocks**: Text, headings, lists, code blocks, tables, callouts, toggles, quotes
- **Limited Support**: Embeds, equations, files (extracted as links/references)
- **Not Supported**: Live charts, board/gallery/timeline views (database views)

### Troubleshooting

#### Common Issues

**"Integration not found" or "Unauthorized" errors:**

- Verify the `api_key` is correct (should start with `secret_`)
- Ensure pages are shared with the integration
- Check that the integration has "Read content" capability

**Empty or missing content:**

- Verify pages contain text (empty pages are skipped by default with `skip_empty_documents: true`)
- Check `min_text_length` filter setting (default: 50 characters)
- Ensure `recursive: true` if expecting child pages
- Check that child pages are not explicitly restricted

**Slow ingestion:**

- Increase `processing.parallelism.num_processes` (default: 2)
- Consider using `partition_by_api: false` for local processing (requires more memory)
- Filter specific pages instead of entire workspace using `page_ids`
- First run is always slower - subsequent runs use incremental updates

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
- Parent pages must be accessible to the integration

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
