#!/bin/bash

# Script to extract the first message from multiple Kafka topics
# This creates binary fixture files for testing

BOOTSTRAP_SERVERS="localhost:9092"
SCHEMA_REGISTRY_URL="http://localhost:8080/schema-registry/api"
OUTPUT_DIR="../src/test/resources/v1"

# Topics to extract (add or remove as needed)
TOPICS=(
    "DataHubUpgradeHistory_v1"
    "FailedMetadataChangeProposal_v1"
    "MetadataChangeLog_Timeseries_v1"
    "MetadataChangeLog_Versioned_v1"
    "MetadataChangeProposal_v1"
)

echo "Extracting first message from multiple Kafka topics"
echo "Bootstrap servers: $BOOTSTRAP_SERVERS"
echo "Schema registry: $SCHEMA_REGISTRY_URL"
echo "Output directory: $OUTPUT_DIR"
echo ""

# Check if kcat is available
if ! command -v kcat &> /dev/null; then
    echo "Error: kcat is not installed. Please install kcat first."
    echo "On macOS: brew install kcat"
    echo "On Ubuntu: sudo apt-get install kcat"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Function to extract message from a topic
extract_topic_message() {
    local topic=$1
    local output_file="$OUTPUT_DIR/${topic}_test_fixture.bin"
    
    echo "Processing topic: $topic"
    
    # Check if topic exists and has messages
    local topic_info=$(kcat -b $BOOTSTRAP_SERVERS -L -t $topic 2>/dev/null)
    if [ $? -ne 0 ]; then
        echo "  ❌ Topic $topic does not exist or is not accessible"
        return 1
    fi
    
    # Check if topic has messages by trying to get the first message
    local message_content=$(kcat -b $BOOTSTRAP_SERVERS -t $topic -c 1 -o beginning -f "%s" -r $SCHEMA_REGISTRY_URL 2>/dev/null)
    if [ -z "$message_content" ]; then
        echo "  ⚠️  Topic $topic exists but has no messages"
        return 1
    fi
    
    # Extract the first message as deserialized content
    echo "  📥 Extracting first message..."
    kcat -b $BOOTSTRAP_SERVERS \
         -t $topic \
         -c 1 \
         -o beginning \
         -r $SCHEMA_REGISTRY_URL \
         -f "%s" \
         > "$output_file" 2>/dev/null
    
    if [ $? -eq 0 ]; then
        local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null)
        echo "  ✅ Successfully extracted to ${topic}_test_fixture.bin ($file_size bytes)"
        
        # Show first few bytes as hex for verification
        echo "  🔍 First 32 bytes (hex):"
        hexdump -C "$output_file" | head -2 | sed 's/^/    /'
        
        # Also show the deserialized content if possible
        echo "  📝 Deserialized content:"
        kcat -b $BOOTSTRAP_SERVERS -t $topic -c 1 -o beginning -f "%s" -r $SCHEMA_REGISTRY_URL 2>/dev/null | sed 's/^/    /'
    else
        echo "  ❌ Failed to extract message from topic $topic"
        return 1
    fi
    
    echo ""
}

# Main execution
echo "Starting extraction process..."
echo "=================================="

success_count=0
total_count=${#TOPICS[@]}

for topic in "${TOPICS[@]}"; do
    if extract_topic_message "$topic"; then
        ((success_count++))
    fi
done

echo "=================================="
echo "Extraction complete!"
echo "Successfully extracted: $success_count/$total_count topics"
echo ""
echo "Generated fixture files:"
for topic in "${TOPICS[@]}"; do
    fixture_file="$OUTPUT_DIR/${topic}_test_fixture.bin"
    if [ -f "$fixture_file" ]; then
        file_size=$(stat -f%z "$fixture_file" 2>/dev/null || stat -c%s "$fixture_file" 2>/dev/null)
        echo "  ✅ ${topic}_test_fixture.bin ($file_size bytes)"
    fi
done
echo ""
echo "These fixture files can now be used in your integration tests!"
