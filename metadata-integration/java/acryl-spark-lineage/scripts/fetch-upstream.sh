#!/bin/bash
#
# Fetch upstream OpenLineage files from GitHub
#
# Usage: ./scripts/fetch-upstream.sh <version>
# Example: ./scripts/fetch-upstream.sh 1.38.0
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(dirname "$SCRIPT_DIR")"

VERSION="${1:-1.33.0}"

if [ -z "$VERSION" ]; then
  echo "Usage: $0 <version>"
  echo "Example: $0 1.38.0"
  exit 1
fi

GITHUB_BASE="https://raw.githubusercontent.com/OpenLineage/OpenLineage"
OUTPUT_DIR="$MODULE_DIR/patches/upstream-$VERSION"

# Files to fetch - array of "path|file" pairs
# Format: "upstream_path|relative_file_path"
FILES=(
  "integration/spark/shared/src/main/java/io/openlineage|spark/api/Vendors.java"
  "integration/spark/shared/src/main/java/io/openlineage|spark/api/VendorsContext.java"
  "integration/spark/shared/src/main/java/io/openlineage|spark/api/VendorsImpl.java"
  "integration/spark/shared/src/main/java/io/openlineage|spark/agent/util/PathUtils.java"
  "integration/spark/shared/src/main/java/io/openlineage|spark/agent/util/PlanUtils.java"
  "integration/spark/shared/src/main/java/io/openlineage|spark/agent/util/RddPathUtils.java"
  "integration/spark/shared/src/main/java/io/openlineage|spark/agent/util/RemovePathPatternUtils.java"
  "integration/spark/shared/src/main/java/io/openlineage|spark/agent/lifecycle/SparkOpenLineageExtensionVisitorWrapper.java"
  "integration/spark/shared/src/main/java/io/openlineage|spark/agent/lifecycle/plan/FileStreamMicroBatchStreamStrategy.java"
  "integration/spark/shared/src/main/java/io/openlineage|spark/agent/lifecycle/plan/SaveIntoDataSourceCommandVisitor.java"
  "integration/spark/shared/src/main/java/io/openlineage|spark/agent/lifecycle/plan/StreamingDataSourceV2RelationVisitor.java"
  "integration/spark/shared/src/main/java/io/openlineage|spark/agent/lifecycle/plan/WriteToDataSourceV2Visitor.java"
  "integration/spark/spark3/src/main/java/io/openlineage|spark3/agent/lifecycle/plan/MergeIntoCommandEdgeInputDatasetBuilder.java"
  "integration/spark/spark3/src/main/java/io/openlineage|spark3/agent/lifecycle/plan/MergeIntoCommandInputDatasetBuilder.java"
)

echo "Fetching OpenLineage $VERSION files from GitHub..."
echo ""

mkdir -p "$OUTPUT_DIR"

SUCCESS_COUNT=0
FAIL_COUNT=0

for entry in "${FILES[@]}"; do
  # Split on pipe: upstream_path|file_path
  IFS='|' read -r upstream_path file_path <<< "$entry"

  URL="$GITHUB_BASE/$VERSION/$upstream_path/$file_path"
  OUTPUT="$OUTPUT_DIR/$file_path"

  mkdir -p "$(dirname "$OUTPUT")"

  echo -n "Fetching: $file_path ... "

  if curl -sf "$URL" -o "$OUTPUT" 2>/dev/null; then
    echo "✓"
    ((SUCCESS_COUNT++))
  else
    echo "✗ (not found - may be DataHub-specific)"
    rm -f "$OUTPUT"
    ((FAIL_COUNT++))
  fi
done

echo ""
echo "========================================="
echo "Fetch complete!"
echo "Success: $SUCCESS_COUNT files"
echo "Not found: $FAIL_COUNT files"
echo "Output directory: $OUTPUT_DIR"
echo "========================================="