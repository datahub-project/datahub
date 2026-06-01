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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.testng.annotations.Test;

/**
 * Detects unsafe GraphQL enum {@code valueOf(pdlValue.name()/toString())} calls in mapper code.
 *
 * <p>PDL enums can deserialize newer values as {@code $UNKNOWN}, but GraphQL-generated Java enums
 * do not include that sentinel. New mapper code should use {@link PdlEnumMapper#map} instead.
 */
public class UnsafeEnumValueOfLintTest {

  private static final String GENERATED_PKG = "com.linkedin.datahub.graphql.generated.";

  private static final Pattern SIMPLE_VALUEOF_PATTERN =
      Pattern.compile("(\\w+)\\.valueOf\\([^;]*?\\.(?:toString|name)\\(\\)\\)", Pattern.DOTALL);

  private static final Pattern QUALIFIED_VALUEOF_PATTERN =
      Pattern.compile(
          "com\\.linkedin\\.datahub\\.graphql\\.generated\\.(\\w+)"
              + "\\.valueOf\\([^;]*?\\.(?:toString|name)\\(\\)\\)",
          Pattern.DOTALL);

  private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");

  /**
   * Exact legacy unsafe call shapes allowed for now. The integer is the number of identical
   * occurrences currently present. This is deliberately narrower than file-level allowlisting so
   * new unsafe conversions added to files with existing debt still fail this test.
   */
  private static final Map<String, Integer> PREEXISTING_VIOLATIONS =
      Map.ofEntries(
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/resolvers/connection/ConnectionMapper.java",
                  "DataHubConnectionDetailsType",
                  "com.linkedin.datahub.graphql.generated.DataHubConnectionDetailsType.valueOf( gmsDetails.getType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/resolvers/policy/mappers/PolicyInfoPolicyMapper.java",
                  "PolicyMatchCondition",
                  "PolicyMatchCondition.valueOf(criterion.getCondition().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/AssertionMapper.java",
                  "AssertionActionType",
                  "AssertionActionType.valueOf(gmsAssertionAction.getType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/AssertionMapper.java",
                  "AssertionSourceType",
                  "AssertionSourceType.valueOf(gmsSource.getType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/AssertionMapper.java",
                  "AssertionStdAggregation",
                  "AssertionStdAggregation.valueOf(gmsDatasetAssertion.getAggregation().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/AssertionMapper.java",
                  "AssertionStdOperator",
                  "AssertionStdOperator.valueOf(gmsDatasetAssertion.getOperator().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/AssertionMapper.java",
                  "AssertionStdParameterType",
                  "AssertionStdParameterType.valueOf(param.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/AssertionMapper.java",
                  "AssertionType",
                  "AssertionType.valueOf(gmsAssertionInfo.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/AssertionMapper.java",
                  "DatasetAssertionScope",
                  "DatasetAssertionScope.valueOf(gmsDatasetAssertion.getScope().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/AssertionMapper.java",
                  "DateInterval",
                  "DateInterval.valueOf(gmsFixedIntervalSchedule.getUnit().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/AssertionMapper.java",
                  "SchemaAssertionCompatibility",
                  "SchemaAssertionCompatibility.valueOf(gmsSchemaAssertionInfo.getCompatibility().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/FieldAssertionMapper.java",
                  "AssertionStdOperator",
                  "AssertionStdOperator.valueOf(gmsFieldMetricAssertion.getOperator().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/FieldAssertionMapper.java",
                  "AssertionStdOperator",
                  "AssertionStdOperator.valueOf(gmsFieldValuesAssertion.getOperator().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/FieldAssertionMapper.java",
                  "FieldAssertionType",
                  "FieldAssertionType.valueOf(gmsFieldAssertionInfo.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/FieldAssertionMapper.java",
                  "FieldMetricType",
                  "FieldMetricType.valueOf(gmsFieldMetricAssertion.getMetric().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/FieldAssertionMapper.java",
                  "FieldTransformType",
                  "FieldTransformType.valueOf(gmsFieldTransform.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/FieldAssertionMapper.java",
                  "FieldValuesFailThresholdType",
                  "FieldValuesFailThresholdType.valueOf(gmsFieldValuesFailThreshold.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/FreshnessAssertionMapper.java",
                  "FreshnessAssertionScheduleType",
                  "FreshnessAssertionScheduleType.valueOf(gmsFreshnessAssertionSchedule.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/FreshnessAssertionMapper.java",
                  "FreshnessAssertionType",
                  "FreshnessAssertionType.valueOf(gmsFreshnessAssertionInfo.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/SqlAssertionMapper.java",
                  "AssertionStdOperator",
                  "AssertionStdOperator.valueOf(gmsSqlAssertionInfo.getOperator().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/SqlAssertionMapper.java",
                  "AssertionValueChangeType",
                  "AssertionValueChangeType.valueOf(gmsSqlAssertionInfo.getChangeType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/SqlAssertionMapper.java",
                  "SqlAssertionType",
                  "SqlAssertionType.valueOf(gmsSqlAssertionInfo.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/VolumeAssertionMapper.java",
                  "AssertionStdOperator",
                  "AssertionStdOperator.valueOf(gmsIncrementingSegmentRowCountChange.getOperator().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/VolumeAssertionMapper.java",
                  "AssertionStdOperator",
                  "AssertionStdOperator.valueOf(gmsIncrementingSegmentRowCountTotal.getOperator().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/VolumeAssertionMapper.java",
                  "AssertionStdOperator",
                  "AssertionStdOperator.valueOf(gmsRowCountChange.getOperator().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/VolumeAssertionMapper.java",
                  "AssertionStdOperator",
                  "AssertionStdOperator.valueOf(gmsRowCountTotal.getOperator().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/VolumeAssertionMapper.java",
                  "AssertionValueChangeType",
                  "AssertionValueChangeType.valueOf(gmsIncrementingSegmentRowCountChange.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/VolumeAssertionMapper.java",
                  "AssertionValueChangeType",
                  "AssertionValueChangeType.valueOf(gmsRowCountChange.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/VolumeAssertionMapper.java",
                  "IncrementingSegmentFieldTransformerType",
                  "IncrementingSegmentFieldTransformerType.valueOf(gmsTransformer.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/assertion/VolumeAssertionMapper.java",
                  "VolumeAssertionType",
                  "VolumeAssertionType.valueOf(gmsVolumeAssertionInfo.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/chart/mappers/ChartMapper.java",
                  "AccessLevel",
                  "AccessLevel.valueOf(info.getAccess().toString())"),
              2),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/chart/mappers/ChartMapper.java",
                  "ChartQueryType",
                  "ChartQueryType.valueOf(query.getType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/chart/mappers/ChartMapper.java",
                  "ChartType",
                  "ChartType.valueOf(info.getType().toString())"),
              2),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/common/mappers/CostMapper.java",
                  "CostType",
                  "CostType.valueOf(cost.getCostType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/common/mappers/DataTransformLogicMapper.java",
                  "QueryLanguage",
                  "QueryLanguage.valueOf(input.getQueryStatement().getLanguage().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/common/mappers/DisplayPropertiesMapper.java",
                  "IconLibrary",
                  "IconLibrary.valueOf(iconPropertiesInput.getIconLibrary().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/common/mappers/OperationMapper.java",
                  "OperationSourceType",
                  "OperationSourceType.valueOf(gmsProfile.getSourceType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/common/mappers/OperationMapper.java",
                  "OperationType",
                  "OperationType.valueOf(OperationType.class, gmsProfile.getOperationType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/common/mappers/OwnerMapper.java",
                  "OwnershipType",
                  "OwnershipType.valueOf(owner.getType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/corpgroup/mappers/CorpGroupMapper.java",
                  "OriginType",
                  "com.linkedin.datahub.graphql.generated.OriginType.valueOf( groupOrigin.getType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/dashboard/mappers/DashboardMapper.java",
                  "AccessLevel",
                  "AccessLevel.valueOf(info.getAccess().toString())"),
              2),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/datacontract/DataContractMapper.java",
                  "DataContractState",
                  "DataContractState.valueOf(status.getState().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/dataplatform/mappers/DataPlatformInfoMapper.java",
                  "PlatformType",
                  "PlatformType.valueOf(input.getType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/dataplatform/mappers/DataPlatformPropertiesMapper.java",
                  "PlatformType",
                  "PlatformType.valueOf(input.getType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/dataprocessinst/mappers/DataProcessInstanceRunEventMapper.java",
                  "DataProcessRunStatus",
                  "com.linkedin.datahub.graphql.generated.DataProcessRunStatus.valueOf( runEvent.getStatus().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/dataprocessinst/mappers/DataProcessInstanceRunResultMapper.java",
                  "DataProcessInstanceRunResultType",
                  "DataProcessInstanceRunResultType.valueOf(input.getType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/dataset/mappers/AssertionRunEventMapper.java",
                  "AssertionResultSeverity",
                  "AssertionResultSeverity.valueOf(gmsResult.getSeverity().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/dataset/mappers/AssertionRunEventMapper.java",
                  "AssertionResultType",
                  "AssertionResultType.valueOf(gmsResult.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/dataset/mappers/AssertionRunEventMapper.java",
                  "AssertionRunStatus",
                  "AssertionRunStatus.valueOf(gmsAssertionRunEvent.getStatus().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/dataset/mappers/AssertionRunEventMapper.java",
                  "PartitionType",
                  "PartitionType.valueOf(gmsPartitionSpec.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/dataset/mappers/DatasetFilterMapper.java",
                  "DatasetFilterType",
                  "DatasetFilterType.valueOf(input.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/dataset/mappers/DatasetMapper.java",
                  "FabricType",
                  "FabricType.valueOf(gmsKey.getOrigin().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/dataset/mappers/VersionedDatasetMapper.java",
                  "FabricType",
                  "FabricType.valueOf(gmsKey.getOrigin().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/ermodelrelationship/mappers/ERModelRelationMapper.java",
                  "ERModelRelationshipCardinality",
                  "ERModelRelationshipCardinality.valueOf( ermodelrelationProperties.getCardinality().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/file/DataHubFileMapper.java",
                  "UploadDownloadScenario",
                  "com.linkedin.datahub.graphql.generated.UploadDownloadScenario.valueOf( gmsFileInfo.getScenario().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/form/FormMapper.java",
                  "FormPromptType",
                  "FormPromptType.valueOf(gmsFormPrompt.getType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/form/FormMapper.java",
                  "FormType",
                  "FormType.valueOf(gmsFormInfo.getType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/incident/IncidentMapper.java",
                  "IncidentSourceType",
                  "IncidentSourceType.valueOf(incidentSource.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/incident/IncidentMapper.java",
                  "IncidentStage",
                  "IncidentStage.valueOf(incidentStatus.getStage().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/incident/IncidentMapper.java",
                  "IncidentState",
                  "IncidentState.valueOf(incidentStatus.getState().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/incident/IncidentMapper.java",
                  "IncidentType",
                  "IncidentType.valueOf(info.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/ingestion/IngestionSourceMapper.java",
                  "IngestionSourceSourceType",
                  "IngestionSourceSourceType.valueOf(source.getType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/knowledge/DocumentMapper.java",
                  "DocumentSourceType",
                  "com.linkedin.datahub.graphql.generated.DocumentSourceType.valueOf( source.getSourceType().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/knowledge/DocumentMapper.java",
                  "DocumentState",
                  "com.linkedin.datahub.graphql.generated.DocumentState.valueOf(status.getState().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/mappers/UrnSearchAcrossLineageResultsMapper.java",
                  "LineageSearchPath",
                  "LineageSearchPath.valueOf(input.getLineageSearchPath().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/mlmodel/mappers/IntendedUseMapper.java",
                  "IntendedUserType",
                  "IntendedUserType.valueOf(v.toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/mlmodel/mappers/MLFeatureMapper.java",
                  "MLFeatureDataType",
                  "MLFeatureDataType.valueOf(featureProperties.getDataType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/mlmodel/mappers/MLFeaturePropertiesMapper.java",
                  "MLFeatureDataType",
                  "MLFeatureDataType.valueOf(mlFeatureProperties.getDataType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/mlmodel/mappers/MLModelGroupMapper.java",
                  "FabricType",
                  "FabricType.valueOf(mlModelGroupKey.getOrigin().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/mlmodel/mappers/MLModelMapper.java",
                  "FabricType",
                  "FabricType.valueOf(mlModelKey.getOrigin().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/mlmodel/mappers/MLPrimaryKeyMapper.java",
                  "MLFeatureDataType",
                  "MLFeatureDataType.valueOf(primaryKeyProperties.getDataType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/mlmodel/mappers/MLPrimaryKeyPropertiesMapper.java",
                  "MLFeatureDataType",
                  "MLFeatureDataType.valueOf(mlPrimaryKeyProperties.getDataType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/mlmodel/mappers/SourceCodeUrlMapper.java",
                  "SourceCodeUrlType",
                  "SourceCodeUrlType.valueOf(input.getType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/module/PageModuleTypeMapper.java",
                  "DataHubPageModuleType",
                  "com.linkedin.datahub.graphql.generated.DataHubPageModuleType.valueOf(type.toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/module/PageModuleVisibilityMapper.java",
                  "PageModuleScope",
                  "PageModuleScope.valueOf(visibility.getScope().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/notebook/mappers/NotebookMapper.java",
                  "NotebookCellType",
                  "NotebookCellType.valueOf(pegasusCell.getType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/operations/OperationsAggregationMapper.java",
                  "WindowDuration",
                  "WindowDuration.valueOf(pdlUsageAggregation.getDuration().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/policy/DataHubPolicyMapper.java",
                  "PolicyMatchCondition",
                  "PolicyMatchCondition.valueOf(criterion.getCondition().name())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/post/PostMapper.java",
                  "MediaType",
                  "MediaType.valueOf(postMedia.getType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/post/PostMapper.java",
                  "PostContentType",
                  "PostContentType.valueOf(postContent.getType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/post/PostMapper.java",
                  "PostType",
                  "PostType.valueOf(postInfo.getType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/structuredproperty/StructuredPropertyMapper.java",
                  "PropertyCardinality",
                  "PropertyCardinality.valueOf(gmsDefinition.getCardinality().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/template/PageTemplateMapper.java",
                  "SummaryElementType",
                  "SummaryElementType.valueOf(el.getElementType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/template/PageTemplateSurfaceMapper.java",
                  "PageTemplateSurfaceType",
                  "PageTemplateSurfaceType.valueOf(surface.getSurfaceType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/template/PageTemplateVisibilityMapper.java",
                  "PageTemplateScope",
                  "PageTemplateScope.valueOf(visibility.getScope().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/timeline/mappers/ChangeEventMapper.java",
                  "ChangeCategoryType",
                  "ChangeCategoryType.valueOf(incomingChangeEvent.getCategory().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/timeline/mappers/ChangeEventMapper.java",
                  "ChangeOperationType",
                  "ChangeOperationType.valueOf(incomingChangeEvent.getOperation().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/usage/UsageAggregationMapper.java",
                  "WindowDuration",
                  "WindowDuration.valueOf(pdlUsageAggregation.getDuration().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/view/DataHubViewMapper.java",
                  "DataHubViewType",
                  "DataHubViewType.valueOf(viewInfo.getType().toString())"),
              1),
          Map.entry(
              violation(
                  "java/com/linkedin/datahub/graphql/types/view/DataHubViewMapper.java",
                  "FilterOperator",
                  "FilterOperator.valueOf(andFilter.getCondition().toString())"),
              1));

  @Test
  public void noUnsafeEnumValueOfCallsInMappers() throws IOException {
    Path srcRoot = findSourceRoot();
    if (srcRoot == null || !Files.isDirectory(srcRoot)) {
      return;
    }

    List<String> violations = new ArrayList<>();
    Map<String, Integer> observedCounts = new HashMap<>();

    Files.walkFileTree(
        srcRoot,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            if (!file.getFileName().toString().endsWith("Mapper.java")) {
              return FileVisitResult.CONTINUE;
            }

            String relativePath = srcRoot.getParent().relativize(file).toString();
            String content = Files.readString(file);
            Set<String> generatedEnumImports = getGeneratedEnumImports(content);
            Set<Integer> reportedSimpleNameOffsets = new HashSet<>();

            if (!generatedEnumImports.isEmpty()) {
              Matcher matcher = SIMPLE_VALUEOF_PATTERN.matcher(content);
              while (matcher.find()) {
                String enumName = matcher.group(1);
                if (generatedEnumImports.contains(enumName)) {
                  reportedSimpleNameOffsets.add(matcher.start());
                  addViolation(
                      violations,
                      observedCounts,
                      content,
                      matcher.start(),
                      relativePath,
                      enumName,
                      matcher.group(0));
                }
              }
            }

            Matcher qualifiedMatcher = QUALIFIED_VALUEOF_PATTERN.matcher(content);
            while (qualifiedMatcher.find()) {
              int simpleNameOffset = qualifiedMatcher.start() + GENERATED_PKG.length();
              if (!reportedSimpleNameOffsets.contains(simpleNameOffset)) {
                addViolation(
                    violations,
                    observedCounts,
                    content,
                    qualifiedMatcher.start(),
                    relativePath,
                    qualifiedMatcher.group(1),
                    qualifiedMatcher.group(0));
              }
            }

            return FileVisitResult.CONTINUE;
          }
        });

    if (!violations.isEmpty()) {
      fail(
          "Found unsafe Enum.valueOf(pdlValue.toString()/name()) calls in mapper code.\n"
              + "PDL may deserialize unknown enum values as $UNKNOWN, which crashes valueOf().\n"
              + "Use PdlEnumMapper.map(EnumClass.class, pdlValue, defaultValue) instead.\n\n"
              + String.join("\n", violations)
              + "\n\n"
              + "Do not add entries to PREEXISTING_VIOLATIONS; migrate new code to PdlEnumMapper.\n");
    }
  }

  private static Set<String> getGeneratedEnumImports(String content) {
    Set<String> generatedEnumImports = new HashSet<>();
    for (String line : content.split("\n")) {
      if (line.startsWith("import " + GENERATED_PKG) && !line.contains("*")) {
        String imported = line.replace("import ", "").replace(";", "").trim();
        generatedEnumImports.add(imported.substring(imported.lastIndexOf('.') + 1));
      }
    }
    return generatedEnumImports;
  }

  private static void addViolation(
      List<String> violations,
      Map<String, Integer> observedCounts,
      String content,
      int matchStart,
      String relativePath,
      String enumName,
      String rawCall) {
    String call = violation(relativePath, enumName, normalizeCall(rawCall));
    int observedCount = observedCounts.merge(call, 1, Integer::sum);
    int allowedCount = PREEXISTING_VIOLATIONS.getOrDefault(call, 0);
    if (observedCount > allowedCount) {
      violations.add(formatViolation(content, matchStart, relativePath, enumName));
    }
  }

  private static String violation(String relativePath, String enumName, String normalizedCall) {
    return relativePath + "\t" + enumName + "\t" + normalizedCall;
  }

  private static String normalizeCall(String call) {
    return WHITESPACE_PATTERN.matcher(call).replaceAll(" ").trim();
  }

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
        "  %s:%d - %s.valueOf(...toString()/name()) is unsafe for PDL enums. "
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
