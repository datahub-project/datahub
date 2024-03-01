package com.linkedin.metadata.service.util;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionStdAggregation;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.AssertionValueChangeType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FieldAssertionType;
import com.linkedin.assertion.FieldMetricAssertion;
import com.linkedin.assertion.FieldMetricType;
import com.linkedin.assertion.FieldTransform;
import com.linkedin.assertion.FieldTransformType;
import com.linkedin.assertion.FieldValuesAssertion;
import com.linkedin.assertion.FieldValuesFailThreshold;
import com.linkedin.assertion.FieldValuesFailThresholdType;
import com.linkedin.assertion.FixedIntervalSchedule;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.assertion.FreshnessCronSchedule;
import com.linkedin.assertion.IncrementingSegmentFieldTransformer;
import com.linkedin.assertion.IncrementingSegmentFieldTransformerType;
import com.linkedin.assertion.IncrementingSegmentRowCountChange;
import com.linkedin.assertion.IncrementingSegmentRowCountTotal;
import com.linkedin.assertion.IncrementingSegmentSpec;
import com.linkedin.assertion.RowCountChange;
import com.linkedin.assertion.RowCountTotal;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.SqlAssertionType;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.assertion.VolumeAssertionType;
import com.linkedin.common.FabricType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetFieldUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataset.DatasetFilter;
import com.linkedin.dataset.DatasetFilterType;
import com.linkedin.schema.SchemaFieldSpec;
import com.linkedin.timeseries.CalendarInterval;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AssertionUtilsTest {

  private static final Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:test");
  private static final DatasetUrn asserteeUrn =
      new DatasetUrn(new DataPlatformUrn("snowflake"), "test", FabricType.PROD);

  @Test
  private void testFreshnessAssertionDescription() {
    final AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FRESHNESS);

    final FreshnessAssertionInfo freshnessAssertionInfo = new FreshnessAssertionInfo();
    final FreshnessAssertionSchedule schedule =
        new FreshnessAssertionSchedule()
            .setType(com.linkedin.assertion.FreshnessAssertionScheduleType.CRON)
            .setCron(
                new FreshnessCronSchedule()
                    .setCron("* * * * *")
                    .setTimezone("America / Los Angeles"));

    freshnessAssertionInfo.setSchedule(schedule);
    freshnessAssertionInfo.setEntity(asserteeUrn);
    freshnessAssertionInfo.setFilter(
        new DatasetFilter().setType(DatasetFilterType.SQL).setSql("WHERE value > 5;"));
    freshnessAssertionInfo.setType(FreshnessAssertionType.DATASET_CHANGE);
    assertionInfo.setFreshnessAssertion(freshnessAssertionInfo);

    String result = AssertionUtils.buildAssertionDescription(assertionUrn, assertionInfo);
    Assert.assertEquals(result, "Dataset was updated since the previous check");

    final FreshnessAssertionSchedule fixedSchedule =
        new FreshnessAssertionSchedule()
            .setType(com.linkedin.assertion.FreshnessAssertionScheduleType.FIXED_INTERVAL)
            .setFixedInterval(
                new FixedIntervalSchedule().setMultiple(1).setUnit(CalendarInterval.DAY));
    freshnessAssertionInfo.setSchedule(fixedSchedule);

    result = AssertionUtils.buildAssertionDescription(assertionUrn, assertionInfo);
    Assert.assertEquals(result, "Dataset was updated in the past 1 days");
  }

  @Test
  private void testVolumeAssertionDescription() {
    final AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.VOLUME);

    final VolumeAssertionInfo volumeAssertionInfo =
        new VolumeAssertionInfo()
            .setEntity(asserteeUrn)
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL)
            .setRowCountTotal(
                new RowCountTotal()
                    .setOperator(AssertionStdOperator.BETWEEN)
                    .setParameters(
                        new AssertionStdParameters()
                            .setMinValue(
                                new AssertionStdParameter()
                                    .setValue("1")
                                    .setType(AssertionStdParameterType.NUMBER))
                            .setMaxValue(
                                new AssertionStdParameter()
                                    .setValue("1000")
                                    .setType(AssertionStdParameterType.NUMBER))));
    assertionInfo.setVolumeAssertion(volumeAssertionInfo);

    String result = AssertionUtils.buildAssertionDescription(assertionUrn, assertionInfo);
    Assert.assertEquals(result, "Row count is between 1 and 1000");

    volumeAssertionInfo.setType(VolumeAssertionType.ROW_COUNT_CHANGE);
    volumeAssertionInfo.setRowCountChange(
        new RowCountChange()
            .setType(AssertionValueChangeType.PERCENTAGE)
            .setOperator(AssertionStdOperator.GREATER_THAN)
            .setParameters(
                new AssertionStdParameters()
                    .setValue(
                        new AssertionStdParameter()
                            .setValue("5")
                            .setType(AssertionStdParameterType.NUMBER))));

    result = AssertionUtils.buildAssertionDescription(assertionUrn, assertionInfo);
    Assert.assertEquals(result, "Row count percentage change greater than 5");

    volumeAssertionInfo.setType(VolumeAssertionType.INCREMENTING_SEGMENT_ROW_COUNT_TOTAL);
    volumeAssertionInfo.setIncrementingSegmentRowCountTotal(
        new IncrementingSegmentRowCountTotal()
            .setSegment(
                new IncrementingSegmentSpec()
                    .setField(
                        new SchemaFieldSpec()
                            .setPath("path")
                            .setNativeType("NUMBER")
                            .setType("STRING"))
                    .setTransformer(
                        new IncrementingSegmentFieldTransformer()
                            .setType(IncrementingSegmentFieldTransformerType.CEILING)
                            .setNativeType("CEILING")))
            .setOperator(AssertionStdOperator.GREATER_THAN)
            .setParameters(
                new AssertionStdParameters()
                    .setValue(
                        new AssertionStdParameter()
                            .setValue("1000")
                            .setType(AssertionStdParameterType.NUMBER))));
    result = AssertionUtils.buildAssertionDescription(assertionUrn, assertionInfo);
    Assert.assertEquals(result, "Incremental row count greater than 1000");

    volumeAssertionInfo.setType(VolumeAssertionType.INCREMENTING_SEGMENT_ROW_COUNT_CHANGE);
    volumeAssertionInfo.setIncrementingSegmentRowCountChange(
        new IncrementingSegmentRowCountChange()
            .setType(AssertionValueChangeType.PERCENTAGE)
            .setSegment(
                new IncrementingSegmentSpec()
                    .setField(
                        new SchemaFieldSpec()
                            .setPath("path")
                            .setNativeType("NUMBER")
                            .setType("STRING"))
                    .setTransformer(
                        new IncrementingSegmentFieldTransformer()
                            .setType(IncrementingSegmentFieldTransformerType.CEILING)
                            .setNativeType("CEILING")))
            .setOperator(AssertionStdOperator.GREATER_THAN)
            .setParameters(
                new AssertionStdParameters()
                    .setValue(
                        new AssertionStdParameter()
                            .setValue("4")
                            .setType(AssertionStdParameterType.NUMBER))));
    result = AssertionUtils.buildAssertionDescription(assertionUrn, assertionInfo);
    Assert.assertEquals(result, "Incremental row count percentage change greater than 4");
  }

  @Test
  private void testFieldAssertionDescription() {
    final AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FIELD);

    FieldAssertionInfo fieldAssertionInfo =
        new FieldAssertionInfo()
            .setEntity(asserteeUrn)
            .setType(FieldAssertionType.FIELD_METRIC)
            .setFieldMetricAssertion(
                new FieldMetricAssertion()
                    .setField(
                        new SchemaFieldSpec()
                            .setPath("path")
                            .setNativeType("NUMBER")
                            .setType("STRING"))
                    .setMetric(FieldMetricType.MEDIAN)
                    .setOperator(AssertionStdOperator.GREATER_THAN)
                    .setParameters(
                        new AssertionStdParameters()
                            .setValue(
                                new AssertionStdParameter()
                                    .setValue("4")
                                    .setType(AssertionStdParameterType.NUMBER))));
    assertionInfo.setFieldAssertion(fieldAssertionInfo);

    String result = AssertionUtils.buildAssertionDescription(assertionUrn, assertionInfo);
    Assert.assertEquals(result, "*Median* of column 'path' (NUMBER) greater than 4");

    fieldAssertionInfo.setType(FieldAssertionType.FIELD_VALUES);
    fieldAssertionInfo.setFieldValuesAssertion(
        new FieldValuesAssertion()
            .setField(
                new SchemaFieldSpec().setPath("path").setNativeType("NUMBER").setType("STRING"))
            .setExcludeNulls(true)
            .setFailThreshold(
                new FieldValuesFailThreshold()
                    .setValue(10)
                    .setType(FieldValuesFailThresholdType.PERCENTAGE))
            .setOperator(AssertionStdOperator.GREATER_THAN)
            .setParameters(
                new AssertionStdParameters()
                    .setValue(
                        new AssertionStdParameter()
                            .setValue("4")
                            .setType(AssertionStdParameterType.NUMBER)))
            .setTransform(new FieldTransform().setType(FieldTransformType.LENGTH)));

    result = AssertionUtils.buildAssertionDescription(assertionUrn, assertionInfo);
    Assert.assertEquals(result, "Column 'path' (NUMBER) greater than 4");
  }

  @Test
  private void testSQLAssertionDescription() {
    final AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.SQL);

    SqlAssertionInfo sqlAssertionInfo =
        new SqlAssertionInfo()
            .setEntity(asserteeUrn)
            .setType(SqlAssertionType.METRIC)
            .setStatement("SELECT COUNT(*) FROM foo.bar.baz")
            .setOperator(AssertionStdOperator.GREATER_THAN)
            .setParameters(
                new AssertionStdParameters()
                    .setValue(
                        new AssertionStdParameter()
                            .setType(AssertionStdParameterType.NUMBER)
                            .setValue(("5"))));
    assertionInfo.setSqlAssertion(sqlAssertionInfo);

    String result = AssertionUtils.buildAssertionDescription(assertionUrn, assertionInfo);
    Assert.assertEquals(result, "SELECT COUNT(*) FROM foo.bar.baz greater than 5");

    sqlAssertionInfo
        .setType(SqlAssertionType.METRIC_CHANGE)
        .setStatement("SELECT COUNT(*) FROM foo.bar.baz")
        .setChangeType(AssertionValueChangeType.ABSOLUTE)
        .setOperator(AssertionStdOperator.GREATER_THAN)
        .setParameters(
            new AssertionStdParameters()
                .setValue(
                    new AssertionStdParameter()
                        .setType(AssertionStdParameterType.NUMBER)
                        .setValue(("5"))));

    result = AssertionUtils.buildAssertionDescription(assertionUrn, assertionInfo);
    Assert.assertEquals(result, "SELECT COUNT(*) FROM foo.bar.baz change greater than 5");

    sqlAssertionInfo.setChangeType(AssertionValueChangeType.PERCENTAGE);

    result = AssertionUtils.buildAssertionDescription(assertionUrn, assertionInfo);
    Assert.assertEquals(
        result, "SELECT COUNT(*) FROM foo.bar.baz percentage change greater than 5");
  }

  @Test
  private void testDatasetAssertionDescription() {
    final AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.DATASET);

    // Native dataset assertion
    DatasetAssertionInfo datasetAssertionInfo =
        new DatasetAssertionInfo()
            .setDataset(asserteeUrn)
            .setScope(DatasetAssertionScope.DATASET_ROWS)
            .setOperator(AssertionStdOperator._NATIVE_)
            .setAggregation(AssertionStdAggregation._NATIVE_)
            .setNativeType("Native description");
    assertionInfo.setDatasetAssertion(datasetAssertionInfo);

    String result = AssertionUtils.buildAssertionDescription(assertionUrn, assertionInfo);
    Assert.assertEquals(result, "Native description");

    // Test a native assertion without a specific type
    DatasetAssertionInfo columnDatasetAssertionInfo =
        new DatasetAssertionInfo()
            .setScope(DatasetAssertionScope.DATASET_COLUMN)
            .setAggregation(AssertionStdAggregation.IDENTITY)
            .setOperator(AssertionStdOperator.NOT_NULL)
            .setFields(new UrnArray(new DatasetFieldUrn(asserteeUrn, "id")));
    assertionInfo.setDatasetAssertion(columnDatasetAssertionInfo);

    result = AssertionUtils.buildAssertionDescription(assertionUrn, assertionInfo);
    Assert.assertEquals(result, "Column id is not null");

    DatasetAssertionInfo rowsDatasetAssertionInfo =
        new DatasetAssertionInfo()
            .setScope(DatasetAssertionScope.DATASET_ROWS)
            .setAggregation(AssertionStdAggregation.ROW_COUNT)
            .setOperator(AssertionStdOperator.BETWEEN)
            .setParameters(
                new AssertionStdParameters()
                    .setMinValue(
                        new AssertionStdParameter()
                            .setValue("40000")
                            .setType(AssertionStdParameterType.NUMBER))
                    .setMaxValue(
                        new AssertionStdParameter()
                            .setValue("50000")
                            .setType(AssertionStdParameterType.NUMBER)));
    assertionInfo.setDatasetAssertion(rowsDatasetAssertionInfo);

    result = AssertionUtils.buildAssertionDescription(assertionUrn, assertionInfo);
    Assert.assertEquals(result, "Row count is between 40000 and 50000");
  }
}
