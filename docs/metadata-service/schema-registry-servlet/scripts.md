# Kafka Message Extraction Script

This script extracts the first message from multiple Kafka topics to create binary test fixtures for integration testing.

## Usage

```bash
cd metadata-service/schema-registry-servlet/scripts
chmod +x extract_kafka_messages.sh
./extract_kafka_messages.sh
```

## What It Does

1. **Extracts messages** from these topics:

   - `DataHubUpgradeHistory_v1`
   - `FailedMetadataChangeProposal_v1`
   - `MetadataChangeLog_Timeseries_v1`
   - `MetadataChangeLog_Versioned_v1`
   - `MetadataChangeProposal_v1`

2. **Creates binary fixtures** in `../src/test/resources/v1/`:

   - `{TopicName}_test_fixture.bin` for each topic with messages

3. **Shows message details**:
   - File size and hex dump of first 32 bytes
   - Deserialized content preview

## Prerequisites

- **kcat** installed (`brew install kcat` on macOS)
- Local Kafka running on `localhost:9092`
- Schema registry accessible at `http://localhost:8080/schema-registry/api`

## Output

The script will create binary fixture files that can be used in your integration tests to verify raw message handling capabilities.
