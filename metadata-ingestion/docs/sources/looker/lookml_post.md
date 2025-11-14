### Configuration Notes

#### API-Based Lineage Extraction and Reachable Views

When `use_api_for_view_lineage: true` is enabled, DataHub uses the `LookerQueryAPIBasedViewUpstream` implementation to extract lineage. This approach:

- **Uses SQL from Looker API**: The system queries the Looker API to generate fully resolved SQL statements for views, which are then parsed to extract column-level and table-level lineage. This provides more accurate lineage than regex-based parsing.

- **Works Only for Reachable Views**: The Looker Query API requires an explore name to generate SQL queries. Therefore, this method only works for views that are **reachable** from explores defined in your LookML model files. A view is considered "reachable" if it is referenced by at least one explore (either directly or through joins).

- **Fallback Behavior**: Views that are not reachable from any explore cannot use the API-based approach and will automatically fall back to regex-based parsing. If `emit_reachable_views_only: true` (default), unreachable views are skipped entirely.

**Example:**

```yml
source:
  type: lookml
  config:
    # Enable API-based lineage (requires reachable views)
    use_api_for_view_lineage: true

    # Control whether unreachable views are processed
    # If true (default), only views referenced by explores are processed
    # If false, all views are processed, but unreachable ones use regex parsing
    emit_reachable_views_only: true
```

**When a view is not reachable:**

- If `emit_reachable_views_only: true`: The view is skipped and a warning is logged
- If `emit_reachable_views_only: false`: The view is processed using regex-based parsing (may have limited lineage accuracy)

1. Handling Liquid Templates

   If a view contains a liquid template, for example:

   ```
   sql_table_name: {{ user_attributes['db'] }}.kafka_streaming.events
   ```

   where `db=ANALYTICS_PROD`, you need to specify the values of those variables in the liquid_variables configuration as shown below:

   ```yml
   liquid_variables:
     user_attributes:
       db: ANALYTICS_PROD
   ```

2. Resolving LookML Constants

   If a view contains a LookML constant, for example:

   ```
   sql_table_name: @{db}.kafka_streaming.events;
   ```

   Ingestion attempts to resolve it's value by looking at project manifest files

   ```yml
   manifest.lkml
     constant: db {
         value: "ANALYTICS_PROD"
     }
   ```

   - If the constant's value is not resolved or incorrectly resolved, you can specify `lookml_constants` configuration in ingestion recipe as shown below. The constant value in recipe takes precedence over constant values resolved from manifest.

     ```yml
     lookml_constants:
       db: ANALYTICS_PROD
     ```

**Liquid Template Support Limits:**

- Supported: Simple variable interpolation (`{{ var }}`) and condition directives (`{% condition filter_name %} field {% endcondition %}`)
- Unsupported: Conditional logic with `if`/`else`/`endif` and custom Looker tags like `date_start`, `date_end`, and `parameter`

Unsupported templates may cause lineage extraction to fail for some assets.

**Additional Notes**

Although liquid variables and LookML constants can be used anywhere in LookML code, their values are currently resolved only for LookML views by DataHub LookML ingestion. This behavior is sufficient since LookML ingestion processes only views and their upstream dependencies.

### Multi-Project LookML (Advanced)

Looker projects support organization as multiple git repos, with [remote includes that can refer to projects that are stored in a different repo](https://cloud.google.com/looker/docs/importing-projects#include_files_from_an_imported_project). If your Looker implementation uses multi-project setup, you can configure the LookML source to pull in metadata from your remote projects as well.

If you are using local or remote dependencies, you will see include directives in your lookml files that look like this:

```
include: "//e_flights/views/users.view.lkml"
include: "//e_commerce/public/orders.view.lkml"
```

Also, you will see projects that are being referred to listed in your `manifest.lkml` file. Something like this:

```
project_name: this_project

local_dependency: {
    project: "my-remote-project"
}

remote_dependency: ga_360_block {
  url: "https://github.com/llooker/google_ga360"
  ref: "0bbbef5d8080e88ade2747230b7ed62418437c21"
}
```

To ingest Looker repositories that are including files defined in other projects, you will need to use the `project_dependencies` directive within the configuration section.
Consider the following scenario:

- Your primary project refers to a remote project called `my_remote_project`
- The remote project is homed in the GitHub repo `my_org/my_remote_project`
- You have provisioned a GitHub deploy key and stored the credential in the environment variable (or UI secret), `${MY_REMOTE_PROJECT_DEPLOY_KEY}`

In this case, you can add this section to your recipe to activate multi-project LookML ingestion.

```
source:
  type: lookml
  config:
    ... other config variables

    project_dependencies:
      my_remote_project:
         repo: my_org/my_remote_project
         deploy_key: ${MY_REMOTE_PROJECT_DEPLOY_KEY}
```

Under the hood, DataHub will check out your remote repository using the provisioned deploy key, and use it to navigate includes that you have in the model files from your primary project.

If you have the remote project checked out locally, and do not need DataHub to clone the project for you, you can provide DataHub directly with the path to the project like the config snippet below:

```
source:
  type: lookml
  config:
    ... other config variables

    project_dependencies:
      my_remote_project: /path/to/local_git_clone_of_remote_project
```

:::note

This is not the same as ingesting the remote project as a primary Looker project because DataHub will not be processing the model files that might live in the remote project. If you want to additionally include the views accessible via the models in the remote project, create a second recipe where your remote project is the primary project.

:::

### Handling Large Views with Many Fields

For Looker views with a large number of fields (100+), DataHub automatically uses field splitting to ensure reliable lineage extraction. This feature splits large field sets into manageable chunks, processes them in parallel, and combines the results.

:::important

**API Configuration Required:** Field splitting requires Looker API credentials to be configured. You must:

1. Provide the `api` configuration section with your Looker credentials
2. Set `use_api_for_view_lineage: true` to enable API-based lineage extraction

Without API configuration, field splitting will not be available and the system will fall back to regex-based parsing, which may fail for large views.

**Reachable Views Only:** The `LookerQueryAPIBasedViewUpstream` implementation (used for field splitting) works by querying the Looker API to generate SQL statements for views. This approach only works for **reachable views** - views that are referenced by explores defined in your LookML model files. Views that are not reachable from any explore cannot be queried via the Looker API and will fall back to regex-based parsing. The `emit_reachable_views_only` configuration option controls whether only reachable views are processed.

:::

#### When Field Splitting is Used

Field splitting is automatically triggered when:

- `use_api_for_view_lineage: true` is set
- Looker API credentials are provided
- A view has more fields than the configured threshold (default: 100 fields)

You can adjust this threshold based on your needs:

```yml
source:
  type: lookml
  config:
    # Adjust the threshold for field splitting (default: 100)
    field_threshold_for_splitting: 100
```

**When to adjust the threshold:**

- **Lower the threshold** (e.g., 50) if you experience SQL parsing failures with views that have 50-100 fields
- **Raise the threshold** (e.g., 150) if your views consistently have 100+ fields and you want to minimize API calls

#### Partial Lineage Results

By default, DataHub will return partial lineage results even if some field chunks fail to parse. This ensures you get lineage information for working fields rather than complete failure.

```yml
source:
  type: lookml
  config:
    # Allow partial lineage when some chunks fail (default: true)
    allow_partial_lineage_results: true
```

**When to disable:**

- Set to `false` if you want strict validation and prefer complete failure over partial results
- Useful for debugging to identify problematic views that need attention

#### Individual Field Fallback

When a chunk of fields fails, DataHub can automatically attempt to process each field individually. This helps:

- Maximize lineage extraction by processing working fields
- Identify specific problematic fields that cause issues
- Provide detailed reporting on which fields fail

```yml
source:
  type: lookml
  config:
    # Enable individual field processing when chunks fail (default: true)
    enable_individual_field_fallback: true
```

**When to disable:**

- Set to `false` if you want faster processing and don't need to identify problematic fields
- Useful if you know all fields in a view are valid and want to skip the fallback overhead

#### Parallel Processing Performance

Field chunks are processed in parallel to improve performance. You can control the number of worker threads:

```yml
source:
  type: lookml
  config:
    # Number of parallel workers (default: 10, max: 100)
    max_workers_for_parallel_processing: 10
```

**Performance tuning:**

- **Increase workers** (e.g., 20-30) for faster processing if you have many large views and sufficient system resources
- **Decrease workers** (e.g., 5) if you're hitting API rate limits or have limited system resources
- **Set to 1** to process sequentially (useful for debugging)

**Important:** The maximum allowed value is 100 to prevent resource exhaustion. Values above 100 will be automatically capped with a warning.

#### Complete Configuration Example

Here's a complete example configuration for handling large views:

```yml
source:
  type: lookml
  config:
    base_folder: /path/to/lookml

    # API configuration (REQUIRED for field splitting)
    api:
      base_url: "https://your-instance.cloud.looker.com"
      client_id: ${LOOKER_CLIENT_ID}
      client_secret: ${LOOKER_CLIENT_SECRET}

    # Enable API-based lineage extraction (REQUIRED for field splitting)
    use_api_for_view_lineage: true

    # Optional: Enable API caching for better performance
    use_api_cache_for_view_lineage: true

    # Large view handling configuration
    field_threshold_for_splitting: 100 # Split views with >100 fields
    allow_partial_lineage_results: true # Return partial results on errors
    enable_individual_field_fallback: true # Process fields individually on chunk failure
    max_workers_for_parallel_processing: 10 # Parallel processing workers
```

**Important Notes:**

- The `api` section with credentials is **required** for field splitting to work
- `use_api_for_view_lineage: true` must be set to enable API-based lineage extraction
- Without API configuration, field splitting features are not available
- **Reachable Views Only**: Field splitting via `LookerQueryAPIBasedViewUpstream` only works for views that are reachable from explores. The Looker Query API requires an explore name to generate SQL, so views not referenced by any explore will use regex-based parsing instead
- The `emit_reachable_views_only` configuration (default: `true`) controls whether unreachable views are processed at all

**Check ingestion logs for:**

- Field splitting statistics: `View 'view_name' has X fields, exceeding threshold of Y. Splitting into multiple queries`
- Success rates: `Combined results for view 'view_name': X tables, Y column lineages, success rate: Z%`
- Problematic fields: Warnings about specific fields that fail processing

**Common issues:**

- **Field splitting not working**: Verify `use_api_for_view_lineage: true` and API credentials are configured
- **Low success rate (<50%)**: Consider lowering `field_threshold_for_splitting` or investigating problematic fields
- **API rate limiting**: Reduce `max_workers_for_parallel_processing` to decrease concurrent requests
- **Memory issues**: Reduce `max_workers_for_parallel_processing` if you experience memory pressure

### Troubleshooting Large View Lineage Extraction

If you have Looker views with many fields (100+) and are experiencing lineage extraction issues, the following troubleshooting steps can help:

:::important

**Prerequisites:** Field splitting requires Looker API configuration. Ensure you have:

- `api` section with valid credentials configured
- `use_api_for_view_lineage: true` enabled

:::

#### Issue: Field splitting not working

**Symptoms:**

- Large views still fail even with field splitting configuration
- No field splitting messages in logs
- Views fall back to regex-based parsing

**Solutions:**

1. **Verify API configuration:**

   ```yml
   source:
     type: lookml
     config:
       api:
         base_url: "https://your-instance.cloud.looker.com"
         client_id: ${LOOKER_CLIENT_ID}
         client_secret: ${LOOKER_CLIENT_SECRET}
       use_api_for_view_lineage: true # Must be enabled
   ```

2. **Check API credentials:**

   - Verify credentials have admin privileges (required for API access)
   - Test API connection separately if needed
   - Check logs for authentication errors

3. **Verify view-to-explore mapping:**
   - Field splitting requires views to be mapped to explores (views must be reachable from explores)
   - Check logs for warnings about missing explore mappings
   - Ensure your views are referenced by at least one explore in your model files
   - If `emit_reachable_views_only: true` (default), unreachable views are skipped entirely

#### Issue: Lineage extraction fails for large views

**Symptoms:**

- Views with 100+ fields show no lineage
- Error messages about SQL parsing failures
- Incomplete lineage information

**Solutions:**

1. **Verify field splitting is working:**
   Check your ingestion logs for messages like:

   ```
   View 'your_view' has 150 fields, exceeding threshold of 100. Splitting into multiple queries for partial lineage.
   ```

   If you don't see this message, field splitting may not be triggered. Lower the threshold:

   ```yml
   field_threshold_for_splitting: 50 # Lower threshold
   ```

2. **Check success rates:**
   Look for statistics in logs:

   ```
   Combined results for view 'your_view': 5 tables, 120 column lineages, success rate: 80.0%
   ```

   - **High success rate (>80%)**: System is working well
   - **Medium success rate (50-80%)**: Some fields may be problematic, but partial lineage is available
   - **Low success rate (<50%)**: Consider investigating specific fields or lowering threshold

3. **Enable individual field fallback:**
   If chunks are failing, enable individual field processing to identify problematic fields:

   ```yml
   enable_individual_field_fallback: true
   ```

   Check logs for warnings about specific fields that fail.

4. **Adjust parallel processing:**
   If you're hitting API rate limits, reduce workers:
   ```yml
   max_workers_for_parallel_processing: 5 # Reduce from default 10
   ```

#### Issue: Slow processing for large views

**Symptoms:**

- Ingestion takes a long time for views with many fields
- Processing appears sequential

**Solutions:**

1. **Increase parallel workers:**

   ```yml
   max_workers_for_parallel_processing: 20 # Increase from default 10
   ```

   **Note:** Monitor system resources and API rate limits

2. **Enable API caching:**

   ```yml
   use_api_cache_for_view_lineage: true # Enable server-side caching
   ```

3. **Verify parallel processing is active:**
   Check logs for concurrent processing indicators. If processing appears sequential, verify `max_workers_for_parallel_processing` is set correctly.

#### Issue: Memory or resource exhaustion

**Symptoms:**

- Ingestion process runs out of memory
- System becomes unresponsive during ingestion

**Solutions:**

1. **Reduce parallel workers:**

   ```yml
   max_workers_for_parallel_processing: 5 # Reduce concurrent processing
   ```

2. **Process sequentially:**

   ```yml
   max_workers_for_parallel_processing: 1 # Disable parallel processing
   ```

3. **Increase chunk size:**
   ```yml
   field_threshold_for_splitting: 150 # Larger chunks = fewer concurrent operations
   ```

#### Issue: Incomplete lineage for some fields

**Symptoms:**

- Some fields show lineage, others don't
- Partial lineage information available

**Solutions:**

1. **This is expected behavior** when `allow_partial_lineage_results: true` (default)

   - Partial lineage is better than no lineage
   - Check logs for specific fields that fail

2. **To identify problematic fields:**

   - Enable `enable_individual_field_fallback: true` (default)
   - Check logs for warnings about specific fields
   - Review those fields in Looker to identify issues

3. **For strict validation:**
   ```yml
   allow_partial_lineage_results: false # Fail completely if any chunk fails
   ```
   **Note:** This may result in no lineage for large views if any chunk fails

#### Best Practices

1. **Start with defaults:** The default configuration works well for most cases
2. **Monitor logs:** Check field splitting statistics and success rates
3. **Tune gradually:** Adjust one parameter at a time and monitor results
4. **Consider your environment:**
   - **High-resource systems:** Can increase `max_workers_for_parallel_processing`
   - **Rate-limited APIs:** Should decrease `max_workers_for_parallel_processing`
   - **Many problematic fields:** Enable `enable_individual_field_fallback`
   - **Strict validation needs:** Disable `allow_partial_lineage_results`

### Debugging LookML Parsing Errors

If you see messages like `my_file.view.lkml': "failed to load view file: Unable to find a matching expression for '<literal>' on line 5"` in the failure logs, it indicates a parsing error for the LookML file.

The first thing to check is that the Looker IDE can validate the file without issues. You can check this by clicking this "Validate LookML" button in the IDE when in development mode.

If that's not the issue, it might be because DataHub's parser, which is based on the [joshtemple/lkml](https://github.com/joshtemple/lkml) library, is slightly more strict than the official Looker parser.
Note that there's currently only one known discrepancy between the two parsers, and it's related to using [leading colons in blocks](https://github.com/joshtemple/lkml/issues/90).

To check if DataHub can parse your LookML file syntax, you can use the `lkml` CLI tool. If this raises an exception, DataHub will fail to parse the file.

```sh
pip install lkml

lkml path/to/my_file.view.lkml
```
