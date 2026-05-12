package com.linkedin.datahub.graphql.types.mappers;

import static org.testng.Assert.fail;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.testng.annotations.Test;

/**
 * Lint-style test that detects unsafe {@code SomeEnum.valueOf(pdlValue.toString())} and {@code
 * SomeEnum.valueOf(pdlValue.name())} calls in mapper code that reads PDL data into GraphQL types.
 *
 * <p>PDL enums use {@code $UNKNOWN} as a sentinel for forwards compatibility: when an older schema
 * reads an enum value added in a newer version, Pegasus deserializes it as {@code $UNKNOWN}. The
 * GraphQL-generated Java enums do not include {@code $UNKNOWN}, so a raw {@link Enum#valueOf} call
 * will throw {@link IllegalArgumentException}. Use {@link PdlEnumMapper#map} instead.
 *
 * <p>This test scans mapper files (files whose names end with {@code Mapper.java}) under {@code
 * datahub-graphql-core/src/main/java}. A GraphQL-generated enum is identified either by its import
 * ({@code import com.linkedin.datahub.graphql.generated.Foo}) or by a fully-qualified reference
 * ({@code com.linkedin.datahub.graphql.generated.Foo.valueOf(...)}) in the source code.
 *
 * <p>The file is read as a single string so that multi-line {@code valueOf()} calls (where the
 * arguments are wrapped to the next line) are detected. The regex uses {@code [^;]*?} (not {@code
 * .*?}) to constrain matches to a single statement, preventing false positives across semicolons.
 */
public class UnsafeEnumValueOfLintTest {

  private static final String GENERATED_PKG = "com.linkedin.datahub.graphql.generated.";

  /**
   * Matches {@code SomeEnum.valueOf(<expr>.toString())} or {@code .name())} within a single
   * statement. Uses {@code [^;]*?} instead of {@code .*?} so the match cannot cross a semicolon
   * boundary, preventing false positives when two unrelated calls appear in the same file. Group 1
   * captures the simple enum name (e.g. {@code MonitorState}).
   */
  private static final Pattern SIMPLE_VALUEOF_PATTERN =
      Pattern.compile("(\\w+)\\.valueOf\\([^;]*?\\.(?:toString|name)\\(\\)\\)", Pattern.DOTALL);

  /**
   * Matches the fully-qualified form {@code com.linkedin.datahub.graphql.generated.Foo.valueOf(
   * <expr>.toString())} or {@code .name())}. Group 1 captures the simple enum name. This catches
   * calls that bypass imports and would otherwise be invisible to the import-based check.
   */
  private static final Pattern QUALIFIED_VALUEOF_PATTERN =
      Pattern.compile(
          "com\\.linkedin\\.datahub\\.graphql\\.generated\\.(\\w+)"
              + "\\.valueOf\\([^;]*?\\.(?:toString|name)\\(\\)\\)",
          Pattern.DOTALL);

  /**
   * Pre-existing violations that should be migrated to PdlEnumMapper.map(). Do not add new entries
   * -- fix the code instead. Each entry here is a ticking time bomb that will crash if the
   * corresponding PDL enum gets a new value on a newer writer.
   */
  private static final Set<String> PREEXISTING_VIOLATIONS =
      Set.of(
          "java/com/linkedin/datahub/graphql/resolvers/connection/ConnectionMapper.java",
          "java/com/linkedin/datahub/graphql/resolvers/policy/mappers/PolicyInfoPolicyMapper.java",
          "java/com/linkedin/datahub/graphql/resolvers/settings/SettingsMapper.java",
          "java/com/linkedin/datahub/graphql/resolvers/test/BatchTestRunEventMapper.java",
          "java/com/linkedin/datahub/graphql/types/ai/mappers/DataHubAiConversationMapper.java",
          "java/com/linkedin/datahub/graphql/types/assertion/AssertionAssignmentRuleMapper.java",
          "java/com/linkedin/datahub/graphql/types/assertion/AssertionMapper.java",
          "java/com/linkedin/datahub/graphql/types/assertion/FieldAssertionMapper.java",
          "java/com/linkedin/datahub/graphql/types/assertion/FreshnessAssertionMapper.java",
          "java/com/linkedin/datahub/graphql/types/assertion/SqlAssertionMapper.java",
          "java/com/linkedin/datahub/graphql/types/assertion/VolumeAssertionMapper.java",
          "java/com/linkedin/datahub/graphql/types/auth/mappers/OAuthAuthorizationServerMapper.java",
          "java/com/linkedin/datahub/graphql/types/chart/mappers/ChartMapper.java",
          "java/com/linkedin/datahub/graphql/types/common/mappers/CostMapper.java",
          "java/com/linkedin/datahub/graphql/types/common/mappers/DataTransformLogicMapper.java",
          "java/com/linkedin/datahub/graphql/types/common/mappers/DisplayPropertiesMapper.java",
          "java/com/linkedin/datahub/graphql/types/common/mappers/OperationMapper.java",
          "java/com/linkedin/datahub/graphql/types/common/mappers/OriginMapper.java",
          "java/com/linkedin/datahub/graphql/types/common/mappers/OwnerMapper.java",
          "java/com/linkedin/datahub/graphql/types/common/mappers/ShareMapper.java",
          "java/com/linkedin/datahub/graphql/types/corpgroup/mappers/CorpGroupMapper.java",
          "java/com/linkedin/datahub/graphql/types/corpuser/mappers/CorpUserInvitationStatusMapper.java",
          "java/com/linkedin/datahub/graphql/types/corpuser/mappers/CorpUserMapper.java",
          "java/com/linkedin/datahub/graphql/types/dashboard/mappers/DashboardMapper.java",
          "java/com/linkedin/datahub/graphql/types/datacontract/DataContractMapper.java",
          "java/com/linkedin/datahub/graphql/types/dataplatform/mappers/DataPlatformInfoMapper.java",
          "java/com/linkedin/datahub/graphql/types/dataplatform/mappers/DataPlatformPropertiesMapper.java",
          "java/com/linkedin/datahub/graphql/types/dataprocessinst/mappers/DataProcessInstanceRunEventMapper.java",
          "java/com/linkedin/datahub/graphql/types/dataprocessinst/mappers/DataProcessInstanceRunResultMapper.java",
          "java/com/linkedin/datahub/graphql/types/dataset/mappers/AssertionRunEventMapper.java",
          "java/com/linkedin/datahub/graphql/types/dataset/mappers/DatasetFilterMapper.java",
          "java/com/linkedin/datahub/graphql/types/dataset/mappers/DatasetMapper.java",
          "java/com/linkedin/datahub/graphql/types/dataset/mappers/DatasetProfileMapper.java",
          "java/com/linkedin/datahub/graphql/types/dataset/mappers/VersionedDatasetMapper.java",
          "java/com/linkedin/datahub/graphql/types/ermodelrelationship/mappers/ERModelRelationMapper.java",
          "java/com/linkedin/datahub/graphql/types/file/DataHubFileMapper.java",
          "java/com/linkedin/datahub/graphql/types/form/FormMapper.java",
          "java/com/linkedin/datahub/graphql/types/incident/IncidentMapper.java",
          "java/com/linkedin/datahub/graphql/types/ingestion/IngestionSourceMapper.java",
          "java/com/linkedin/datahub/graphql/types/knowledge/DocumentMapper.java",
          "java/com/linkedin/datahub/graphql/types/mappers/UrnSearchAcrossLineageResultsMapper.java",
          "java/com/linkedin/datahub/graphql/types/mlmodel/mappers/IntendedUseMapper.java",
          "java/com/linkedin/datahub/graphql/types/mlmodel/mappers/MLFeatureMapper.java",
          "java/com/linkedin/datahub/graphql/types/mlmodel/mappers/MLFeaturePropertiesMapper.java",
          "java/com/linkedin/datahub/graphql/types/mlmodel/mappers/MLModelGroupMapper.java",
          "java/com/linkedin/datahub/graphql/types/mlmodel/mappers/MLModelMapper.java",
          "java/com/linkedin/datahub/graphql/types/mlmodel/mappers/MLPrimaryKeyMapper.java",
          "java/com/linkedin/datahub/graphql/types/mlmodel/mappers/MLPrimaryKeyPropertiesMapper.java",
          "java/com/linkedin/datahub/graphql/types/mlmodel/mappers/SourceCodeUrlMapper.java",
          "java/com/linkedin/datahub/graphql/types/module/PageModuleTypeMapper.java",
          "java/com/linkedin/datahub/graphql/types/module/PageModuleVisibilityMapper.java",
          "java/com/linkedin/datahub/graphql/types/monitor/MonitorMapper.java",
          "java/com/linkedin/datahub/graphql/types/notebook/mappers/NotebookMapper.java",
          "java/com/linkedin/datahub/graphql/types/notification/mappers/NotificationSettingMapMapper.java",
          "java/com/linkedin/datahub/graphql/types/notification/mappers/NotificationSettingsMapper.java",
          "java/com/linkedin/datahub/graphql/types/operations/OperationsAggregationMapper.java",
          "java/com/linkedin/datahub/graphql/types/policy/DataHubPolicyMapper.java",
          "java/com/linkedin/datahub/graphql/types/post/PostMapper.java",
          "java/com/linkedin/datahub/graphql/types/remoteexecutor/RemoteExecutorPoolMapper.java",
          "java/com/linkedin/datahub/graphql/types/service/mappers/ServiceMapper.java",
          "java/com/linkedin/datahub/graphql/types/structuredproperty/StructuredPropertyMapper.java",
          "java/com/linkedin/datahub/graphql/types/subscription/mappers/DataHubSubscriptionMapper.java",
          "java/com/linkedin/datahub/graphql/types/template/PageTemplateMapper.java",
          "java/com/linkedin/datahub/graphql/types/template/PageTemplateSurfaceMapper.java",
          "java/com/linkedin/datahub/graphql/types/template/PageTemplateVisibilityMapper.java",
          "java/com/linkedin/datahub/graphql/types/test/TestMapper.java",
          "java/com/linkedin/datahub/graphql/types/timeline/mappers/ChangeEventMapper.java",
          "java/com/linkedin/datahub/graphql/types/usage/UsageAggregationMapper.java",
          "java/com/linkedin/datahub/graphql/types/view/DataHubViewMapper.java");

  /**
   * Scans all *Mapper.java files for unsafe valueOf(toString()/name()) calls on GraphQL-generated
   * enums.
   *
   * <p>How the scan works, step by step:
   *
   * <ol>
   *   <li><b>Walk the source tree</b> — recursively visit every file under {@code
   *       datahub-graphql-core/src/main/java}.
   *   <li><b>Filter to mapper files only</b> — only files ending in {@code Mapper.java} are
   *       checked. Resolvers are skipped because they typically map in the safe direction (GraphQL
   *       input → PDL), not the dangerous direction (PDL → GraphQL output).
   *   <li><b>Skip pre-existing violations</b> — files listed in {@link #PREEXISTING_VIOLATIONS} are
   *       excluded so the test passes today; those files are tech debt to migrate later.
   *   <li><b>Read the whole file as a string</b> — so that multi-line {@code valueOf()} calls
   *       (where arguments wrap to the next line) are visible to the regex.
   *   <li><b>Collect GraphQL enum imports</b> — scan import statements for anything from {@code
   *       com.linkedin.datahub.graphql.generated.*}. These simple names are used to identify the
   *       import-based pattern.
   *   <li><b>Regex-match — import-based pattern</b> — find {@code <EnumName>.valueOf([^;]*?
   *       .toString()) } or {@code .name())} where {@code <EnumName>} matches a collected import.
   *       Uses {@code [^;]*?} to stay within a single statement. Uses {@link Pattern#DOTALL} so the
   *       match can cross newlines.
   *   <li><b>Regex-match — fully-qualified pattern</b> — independently find {@code
   *       com.linkedin.datahub.graphql.generated.<Enum>.valueOf(...)} calls. These don't need an
   *       import to be dangerous — the package prefix is proof enough.
   *   <li><b>Fail with actionable output</b> — if any violations are found, the test fails with the
   *       exact file, line number, and enum name, plus instructions to use {@link PdlEnumMapper}.
   * </ol>
   */
  @Test
  public void noUnsafeEnumValueOfCallsInMappers() throws IOException {
    Path srcRoot = findSourceRoot();
    if (srcRoot == null || !Files.isDirectory(srcRoot)) {
      return;
    }

    List<String> violations = new ArrayList<>();

    // Step 1: Walk the source tree
    Files.walkFileTree(
        srcRoot,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            // Step 2: Only inspect *Mapper.java files (the PDL → GraphQL read path)
            String fileName = file.getFileName().toString();
            if (!fileName.endsWith("Mapper.java")) {
              return FileVisitResult.CONTINUE;
            }

            // Step 3: Skip files with known pre-existing violations (tech debt)
            String relativePath = srcRoot.getParent().relativize(file).toString();
            if (PREEXISTING_VIOLATIONS.contains(relativePath)) {
              return FileVisitResult.CONTINUE;
            }

            // Step 4: Read the entire file as one string so multi-line valueOf()
            // calls (where arguments wrap to the next line) are visible to the regex.
            String content = Files.readString(file);

            // Step 5: Collect the simple names of all GraphQL-generated enum imports.
            // These are the types that DON'T have a $UNKNOWN constant and will crash
            // if valueOf() receives one.
            Set<String> generatedEnumImports = new HashSet<>();
            for (String line : content.split("\n")) {
              if (line.startsWith("import " + GENERATED_PKG) && !line.contains("*")) {
                String imported = line.replace("import ", "").replace(";", "").trim();
                String simpleName = imported.substring(imported.lastIndexOf('.') + 1);
                generatedEnumImports.add(simpleName);
              }
            }

            // Track violations by character offset to deduplicate across the two patterns.
            Set<Integer> reportedOffsets = new HashSet<>();

            // Step 6: Match the import-based pattern — SomeEnum.valueOf(<expr>.toString()/name())
            // where SomeEnum is one of the GraphQL-generated imports.
            if (!generatedEnumImports.isEmpty()) {
              Matcher m = SIMPLE_VALUEOF_PATTERN.matcher(content);
              while (m.find()) {
                String enumName = m.group(1);
                if (generatedEnumImports.contains(enumName)) {
                  reportedOffsets.add(m.start());
                  violations.add(formatViolation(content, m.start(), relativePath, enumName));
                }
              }
            }

            // Step 7: Match the fully-qualified pattern —
            // com.linkedin.datahub.graphql.generated.Foo.valueOf(<expr>.toString()/name()).
            // These don't need an import to be dangerous.
            Matcher qm = QUALIFIED_VALUEOF_PATTERN.matcher(content);
            while (qm.find()) {
              if (!reportedOffsets.contains(qm.start())) {
                violations.add(formatViolation(content, qm.start(), relativePath, qm.group(1)));
              }
            }

            return FileVisitResult.CONTINUE;
          }
        });

    // Step 8: Fail with actionable output listing every violation
    if (!violations.isEmpty()) {
      fail(
          "Found unsafe Enum.valueOf(pdlValue.toString()/name()) calls in mapper code.\n"
              + "PDL may deserialize unknown enum values as $UNKNOWN, which crashes valueOf().\n"
              + "Use PdlEnumMapper.map(EnumClass.class, pdlValue, defaultValue) instead.\n\n"
              + String.join("\n", violations)
              + "\n\n"
              + "Do NOT add new entries to PREEXISTING_VIOLATIONS — fix the code instead.\n");
    }
  }

  /** Convert a character offset in {@code content} to a 1-based line number. */
  private static int lineNumberAt(String content, int offset) {
    int line = 1;
    for (int i = 0; i < offset && i < content.length(); i++) {
      if (content.charAt(i) == '\n') {
        line++;
      }
    }
    return line;
  }

  private static String formatViolation(
      String content, int matchStart, String relativePath, String enumName) {
    return String.format(
        "  %s:%d — %s.valueOf(...toString()/name()) is unsafe for PDL enums. "
            + "Use PdlEnumMapper.map() instead.",
        relativePath, lineNumberAt(content, matchStart), enumName);
  }

  private static Path findSourceRoot() {
    Path candidate = Paths.get("datahub-graphql-core/src/main/java");
    if (Files.isDirectory(candidate)) {
      return candidate;
    }
    candidate = Paths.get("src/main/java");
    if (Files.isDirectory(candidate)) {
      return candidate;
    }
    return null;
  }
}
