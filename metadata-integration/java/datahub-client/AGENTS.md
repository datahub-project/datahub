# datahub-client — Agent Development Guide

Java SDK V2 integration tests - guidance for Claude Code.

## Running Integration Tests

**Prerequisites:** Running DataHub instance (start with `scripts/dev/datahub-dev.sh start`; see root `AGENTS.md`)

```bash
# Run all integration tests
ADMIN_USERNAME=datahub ADMIN_PASSWORD=datahub ./gradlew :metadata-integration:java:datahub-client:test --tests "*Integration*"

# Run specific entity tests
./gradlew :metadata-integration:java:datahub-client:test --tests "DatasetIntegrationTest"

# Run single test method
./gradlew :metadata-integration:java:datahub-client:test --tests "DatasetIntegrationTest.testDatasetCreateMinimal"

# With verbose output
./gradlew :metadata-integration:java:datahub-client:test --tests "*Integration*" --info
```

## Environment Variables

| Variable         | Required | Default                 | Description                                    |
| ---------------- | -------- | ----------------------- | ---------------------------------------------- |
| `DATAHUB_SERVER` | Yes      | `http://localhost:8080` | DataHub GMS server URL                         |
| `DATAHUB_TOKEN`  | No       | _auto-generated_        | Personal access token (skips auth if provided) |
| `ADMIN_USERNAME` | No       | `datahub`               | Admin username for authentication              |
| `ADMIN_PASSWORD` | No       | `datahub`               | Admin password for authentication              |

**Note:** `BaseIntegrationTest` automatically generates tokens from admin credentials if `DATAHUB_TOKEN` not provided.

## Test Organization

Integration tests cover 8 entity types (Dataset, Chart, Dashboard, DataJob, DataFlow, Container, MLModel, MLModelGroup), plus emit modes (`EmitModeIntegrationTest`) and `EntityClient.get()` (`EntityClientGetIntegrationTest`). Name new test classes `*IntegrationTest`: CI selects them with `--tests "*Integration*"`.

All tests extend `BaseIntegrationTest` which handles authentication, token generation, client initialization, and cleanup.

## Adding New Integration Tests

```java
public class MyEntityIntegrationTest extends BaseIntegrationTest {
  @Test
  public void testMyEntity() throws Exception {
    // Use inherited 'client' field
    MyEntity entity = MyEntity.builder()
        .id("test_entity_" + System.currentTimeMillis())  // Use timestamps for unique IDs
        .build();
    client.entities().upsert(entity);
    assertNotNull(entity.getUrn());
  }
}
```

**Important:** Use timestamp-based IDs to avoid conflicts. No cleanup needed - `BaseIntegrationTest` handles lifecycle.

## Troubleshooting

**Tests skipped:** Verify DataHub running and environment set:

```bash
curl http://localhost:8080/health
export DATAHUB_SERVER=http://localhost:8080
```

**Authentication failures:** Check credentials:

```bash
export ADMIN_USERNAME=datahub
export ADMIN_PASSWORD=datahub
```

**Connection issues:** the tests default to `http://localhost:8080`; a per-worktree stack
started by `scripts/dev/datahub-dev.sh` uses its own port, so export `DATAHUB_SERVER` from the
`DATAHUB_GMS_URL` that `scripts/dev/datahub-dev.sh shell-env` prints. Then:

```bash
scripts/dev/datahub-dev.sh status                 # ports, containers, health
curl "$DATAHUB_SERVER/health"
scripts/dev/datahub-dev.sh logs gms               # local mode; Docker: docker logs <container from status>
```
