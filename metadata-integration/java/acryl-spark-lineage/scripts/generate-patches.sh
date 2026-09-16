#!/bin/bash
#
# Generate patch files showing DataHub customizations vs upstream OpenLineage
#
# Usage: ./scripts/generate-patches.sh <upstream-version>
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(dirname "$SCRIPT_DIR")"
UPSTREAM_VERSION="${1:-1.33.0}"
UPSTREAM_DIR="$MODULE_DIR/patches/upstream-$UPSTREAM_VERSION"
PATCHES_DIR="$MODULE_DIR/patches"
DATAHUB_SRC="$MODULE_DIR/src/main/java/io/openlineage"

# Files to compare
FILES=(
  "spark/api/Vendors.java"
  "spark/api/VendorsContext.java"
  "spark/api/VendorsImpl.java"
  "spark/agent/util/PathUtils.java"
  "spark/agent/util/PlanUtils.java"
  "spark/agent/util/RemovePathPatternUtils.java"
  "spark/agent/lifecycle/SparkOpenLineageExtensionVisitorWrapper.java"
  "spark/agent/lifecycle/plan/SaveIntoDataSourceCommandVisitor.java"
  "spark/agent/lifecycle/plan/StreamingDataSourceV2RelationVisitor.java"
  "spark/agent/lifecycle/plan/TopicPartitionProxy.java"
  "spark/agent/lifecycle/plan/WriteToDataSourceV2Visitor.java"
  "spark3/agent/lifecycle/plan/MergeIntoCommandEdgeInputDatasetBuilder.java"
  "spark3/agent/lifecycle/plan/MergeIntoCommandInputDatasetBuilder.java"
)

echo "Generating patches for DataHub customizations vs OpenLineage $UPSTREAM_VERSION..."

# Check if upstream directory exists
if [ ! -d "$UPSTREAM_DIR" ]; then
  echo "Error: Upstream directory not found: $UPSTREAM_DIR"
  echo "Please fetch upstream files first using scripts/fetch-upstream.sh"
  exit 1
fi

# Create versioned output directory for patches
mkdir -p "$PATCHES_DIR/datahub-customizations/v${UPSTREAM_VERSION}"

# Generate patches for each file
for file in "${FILES[@]}"; do
  UPSTREAM_FILE="$UPSTREAM_DIR/$file"
  DATAHUB_FILE="$DATAHUB_SRC/$file"
  PATCH_FILE="$PATCHES_DIR/datahub-customizations/v${UPSTREAM_VERSION}/$(basename "$file" .java).patch"
  # Recorded in the patch header instead of the absolute paths diff would otherwise emit. Those are
  # not cosmetic: they are what `patch` reads to locate the target, so a header naming this machine's
  # home directory produces a patch nobody else can apply. Relative, a/-b/ prefixed, same as git.
  UPSTREAM_LABEL="a/patches/upstream-$UPSTREAM_VERSION/$file"
  DATAHUB_LABEL="b/src/main/java/io/openlineage/$file"

  if [ ! -f "$UPSTREAM_FILE" ]; then
    echo "Warning: Upstream file not found: $UPSTREAM_FILE (skipping)"
    continue
  fi

  if [ ! -f "$DATAHUB_FILE" ]; then
    echo "Warning: DataHub file not found: $DATAHUB_FILE (DataHub-specific file)"
    # Create a note file indicating this is a DataHub-specific addition
    echo "# DataHub-specific file - not present in upstream OpenLineage $UPSTREAM_VERSION" > "${PATCH_FILE}.note"
    continue
  fi

  # Generate unified diff
  if diff -u --label "$UPSTREAM_LABEL" --label "$DATAHUB_LABEL" \
       "$UPSTREAM_FILE" "$DATAHUB_FILE" > "$PATCH_FILE" 2>/dev/null; then
    # No differences found
    echo "No customizations in: $file"
    rm -f "$PATCH_FILE"
  else
    # Differences found
    echo "Generated patch: $(basename "$PATCH_FILE")"

    # Add header to patch file
    TEMP_PATCH=$(mktemp)
    cat > "$TEMP_PATCH" <<EOF
# Patch for DataHub customizations in $(basename "$file")
# Upstream version: OpenLineage $UPSTREAM_VERSION
# Generated: $(date -u +"%Y-%m-%d %H:%M:%S UTC")
#
# To apply this patch to a new upstream version, from the acryl-spark-lineage module directory:
#   patch src/main/java/io/openlineage/$file < patches/datahub-customizations/v${UPSTREAM_VERSION}/$(basename "$PATCH_FILE")
#
# The target is named explicitly rather than left to -p<n>: both header paths exist during an
# upgrade, and patch's "fewest components wins" heuristic picks the upstream reference copy, so
# -p1 would silently rewrite patches/upstream-*/ instead of the source tree.
#
EOF
    cat "$PATCH_FILE" >> "$TEMP_PATCH"
    mv "$TEMP_PATCH" "$PATCH_FILE"
  fi
done

echo ""
echo "Patch generation complete!"
echo "Patches stored in: $PATCHES_DIR/datahub-customizations/"
echo ""
echo "To apply these patches to a new upstream version:"
echo "  1. Place new upstream files in patches/upstream-<version>/"
echo "  2. Copy files to src/main/java/io/openlineage/"
echo "  3. Apply patches, from the module directory (target read from each patch's +++ header):"
echo "       for p in patches/datahub-customizations/v${UPSTREAM_VERSION}/*.patch; do"
echo "         patch \"\$(sed -n 's|^+++ b/||p' \"\$p\" | head -1)\" < \"\$p\""
echo "       done"
