#!/bin/bash
#
# Automate OpenLineage version upgrades for acryl-spark-lineage
#
# This script:
# 1. Fetches upstream OpenLineage files at specified versions
# 2. Compares old vs new upstream versions
# 3. Applies DataHub customizations to the new version
# 4. Updates the version in build.gradle
#
# Usage: ./scripts/upgrade-openlineage.sh <old-version> <new-version>
# Example: ./scripts/upgrade-openlineage.sh 1.33.0 1.38.0
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(dirname "$SCRIPT_DIR")"
ROOT_DIR="$(cd "$MODULE_DIR/../../../.." && pwd)"

OLD_VERSION="$1"
NEW_VERSION="$2"

if [ -z "$OLD_VERSION" ] || [ -z "$NEW_VERSION" ]; then
  echo "Usage: $0 <old-version> <new-version>"
  echo "Example: $0 1.33.0 1.38.0"
  exit 1
fi

echo "========================================="
echo "OpenLineage Upgrade: $OLD_VERSION → $NEW_VERSION"
echo "========================================="
echo ""

# Files to upgrade - array of "path|file" pairs
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
  "integration/spark/shared/src/main/java/io/openlineage|spark/agent/lifecycle/plan/SaveIntoDataSourceCommandVisitor.java"
  "integration/spark/shared/src/main/java/io/openlineage|spark/agent/lifecycle/plan/StreamingDataSourceV2RelationVisitor.java"
  "integration/spark/shared/src/main/java/io/openlineage|spark/agent/lifecycle/plan/WriteToDataSourceV2Visitor.java"
  "integration/spark/spark3/src/main/java/io/openlineage|spark3/agent/lifecycle/plan/MergeIntoCommandEdgeInputDatasetBuilder.java"
  "integration/spark/spark3/src/main/java/io/openlineage|spark3/agent/lifecycle/plan/MergeIntoCommandInputDatasetBuilder.java"
)

GITHUB_BASE="https://raw.githubusercontent.com/OpenLineage/OpenLineage"

OLD_UPSTREAM_DIR="$MODULE_DIR/patches/upstream-$OLD_VERSION"
NEW_UPSTREAM_DIR="$MODULE_DIR/patches/upstream-$NEW_VERSION"
DATAHUB_SRC="$MODULE_DIR/src/main/java/io/openlineage"
PATCHES_DIR="$MODULE_DIR/patches/datahub-customizations"
BACKUP_DIR="$MODULE_DIR/patches/backup-$(date +%Y%m%d-%H%M%S)"

echo "Step 1: Fetching upstream files..."
echo "-----------------------------------"

mkdir -p "$NEW_UPSTREAM_DIR"

for entry in "${FILES[@]}"; do
  # Split on pipe: upstream_path|file_path
  IFS='|' read -r upstream_path file_path <<< "$entry"

  URL="$GITHUB_BASE/$NEW_VERSION/$upstream_path/$file_path"
  OUTPUT="$NEW_UPSTREAM_DIR/$file_path"

  mkdir -p "$(dirname "$OUTPUT")"

  echo "Fetching: $file_path"
  if curl -sf "$URL" -o "$OUTPUT"; then
    echo "  ✓ Downloaded"
  else
    echo "  ✗ Not found (may be DataHub-specific or moved)"
    rm -f "$OUTPUT"
  fi
done

echo ""
echo "Step 2: Comparing upstream changes..."
echo "--------------------------------------"

CHANGED_FILES=()
UNCHANGED_FILES=()

for entry in "${FILES[@]}"; do
  # Split on pipe: upstream_path|file_path
  IFS='|' read -r upstream_path file_path <<< "$entry"

  OLD_FILE="$OLD_UPSTREAM_DIR/$file_path"
  NEW_FILE="$NEW_UPSTREAM_DIR/$file_path"

  if [ ! -f "$OLD_FILE" ] && [ ! -f "$NEW_FILE" ]; then
    continue
  fi

  if [ ! -f "$OLD_FILE" ]; then
    echo "NEW: $file_path (added in $NEW_VERSION)"
    CHANGED_FILES+=("$file_path")
  elif [ ! -f "$NEW_FILE" ]; then
    echo "REMOVED: $file_path (removed in $NEW_VERSION)"
    CHANGED_FILES+=("$file_path")
  elif ! diff -q "$OLD_FILE" "$NEW_FILE" > /dev/null 2>&1; then
    echo "CHANGED: $file_path"
    CHANGED_FILES+=("$file_path")
  else
    UNCHANGED_FILES+=("$file_path")
  fi
done

echo ""
if [ ${#UNCHANGED_FILES[@]} -gt 0 ]; then
  echo "Unchanged files (${#UNCHANGED_FILES[@]}):"
  for file in "${UNCHANGED_FILES[@]}"; do
    echo "  - $file"
  done
  echo ""
fi

if [ ${#CHANGED_FILES[@]} -eq 0 ]; then
  echo "No upstream changes detected. Skipping file updates."
else
  echo "Changed files (${#CHANGED_FILES[@]}):"
  for file in "${CHANGED_FILES[@]}"; do
    echo "  - $file"
  done

  echo ""
  echo "Step 3: Backing up current files..."
  echo "------------------------------------"

  mkdir -p "$BACKUP_DIR"
  for file in "${CHANGED_FILES[@]}"; do
    DATAHUB_FILE="$DATAHUB_SRC/$file"
    if [ -f "$DATAHUB_FILE" ]; then
      BACKUP_FILE="$BACKUP_DIR/$file"
      mkdir -p "$(dirname "$BACKUP_FILE")"
      cp "$DATAHUB_FILE" "$BACKUP_FILE"
      echo "Backed up: $file"
    fi
  done

  echo ""
  echo "Step 4: Updating files with new upstream..."
  echo "---------------------------------------------"

  for file in "${CHANGED_FILES[@]}"; do
    NEW_FILE="$NEW_UPSTREAM_DIR/$file"
    DATAHUB_FILE="$DATAHUB_SRC/$file"

    if [ -f "$NEW_FILE" ]; then
      mkdir -p "$(dirname "$DATAHUB_FILE")"
      cp "$NEW_FILE" "$DATAHUB_FILE"
      echo "Updated: $file"
    elif [ -f "$DATAHUB_FILE" ]; then
      echo "Removed upstream file (keeping DataHub version): $file"
    fi
  done

  echo ""
  echo "Step 5: Applying DataHub customizations..."
  echo "--------------------------------------------"

  PATCH_CONFLICTS=()

  for file in "${CHANGED_FILES[@]}"; do
    PATCH_FILE="$PATCHES_DIR/$(basename "$file" .java).patch"
    DATAHUB_FILE="$DATAHUB_SRC/$file"

    if [ -f "$PATCH_FILE" ] && [ -f "$DATAHUB_FILE" ]; then
      echo "Applying patch: $(basename "$PATCH_FILE")"

      if patch -p0 --dry-run < "$PATCH_FILE" > /dev/null 2>&1; then
        patch -p0 < "$PATCH_FILE"
        echo "  ✓ Applied successfully"
      else
        echo "  ✗ Patch conflict detected - manual merge required"
        PATCH_CONFLICTS+=("$file")
        # Restore backup
        BACKUP_FILE="$BACKUP_DIR/$file"
        if [ -f "$BACKUP_FILE" ]; then
          cp "$BACKUP_FILE" "$DATAHUB_FILE"
          echo "  ↻ Restored original file"
        fi
      fi
    fi
  done

  if [ ${#PATCH_CONFLICTS[@]} -gt 0 ]; then
    echo ""
    echo "⚠️  WARNING: Patch conflicts detected in:"
    for file in "${PATCH_CONFLICTS[@]}"; do
      echo "  - $file"
    done
    echo ""
    echo "Manual merge required. See upgrade guide in CLAUDE.md"
    echo "Backup files available in: $BACKUP_DIR"
  fi
fi

echo ""
echo "Step 6: Updating version in build.gradle..."
echo "---------------------------------------------"

BUILD_GRADLE="$ROOT_DIR/build.gradle"

if [ -f "$BUILD_GRADLE" ]; then
  if grep -q "ext.openLineageVersion = '$OLD_VERSION'" "$BUILD_GRADLE"; then
    sed -i.bak "s/ext.openLineageVersion = '$OLD_VERSION'/ext.openLineageVersion = '$NEW_VERSION'/" "$BUILD_GRADLE"
    echo "✓ Updated build.gradle: $OLD_VERSION → $NEW_VERSION"
    rm -f "$BUILD_GRADLE.bak"
  else
    echo "⚠️  Warning: Could not find version $OLD_VERSION in build.gradle"
    echo "   Please update manually: ext.openLineageVersion = '$NEW_VERSION'"
  fi
else
  echo "⚠️  Warning: build.gradle not found at $BUILD_GRADLE"
fi

echo ""
echo "========================================="
echo "Upgrade Summary"
echo "========================================="
echo "Old version: $OLD_VERSION"
echo "New version: $NEW_VERSION"
echo "Changed files: ${#CHANGED_FILES[@]}"
echo "Patch conflicts: ${#PATCH_CONFLICTS[@]}"
echo ""

if [ ${#PATCH_CONFLICTS[@]} -eq 0 ]; then
  echo "✓ Upgrade completed successfully!"
  echo ""
  echo "Next steps:"
  echo "  1. Review changes: git diff"
  echo "  2. Build: ./gradlew :metadata-integration:java:acryl-spark-lineage:build"
  echo "  3. Test: ./gradlew :metadata-integration:java:acryl-spark-lineage:test"
  echo "  4. Update CLAUDE.md if needed"
else
  echo "⚠️  Upgrade completed with conflicts - manual review required"
  echo ""
  echo "Next steps:"
  echo "  1. Review conflicts in files listed above"
  echo "  2. Manually merge changes from:"
  echo "     - Backup: $BACKUP_DIR"
  echo "     - New upstream: $NEW_UPSTREAM_DIR"
  echo "  3. Regenerate patches: ./scripts/generate-patches.sh $NEW_VERSION"
  echo "  4. Build and test"
fi

echo ""
echo "Backup location: $BACKUP_DIR"
echo "========================================="