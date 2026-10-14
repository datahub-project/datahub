package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.DatasetProfile;
import com.linkedin.datahub.graphql.generated.PartitionType;
import com.linkedin.datahub.graphql.types.common.mappers.util.TimeWindowUtils;
import com.linkedin.dataset.DatasetFieldProfile;
import com.linkedin.dataset.DatasetFieldProfileArray;
import com.linkedin.dataset.Quantile;
import com.linkedin.dataset.QuantileArray;
import com.linkedin.dataset.ValueFrequency;
import com.linkedin.dataset.ValueFrequencyArray;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.timeseries.PartitionSpec;
import com.linkedin.timeseries.TimeWindow;
import com.linkedin.timeseries.TimeWindowSize;
import java.util.ArrayList;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DatasetProfileMapperTest {
  @Test
  public void testMapperFullProfile() {
    final com.linkedin.dataset.DatasetProfile input = new com.linkedin.dataset.DatasetProfile();
    input.setTimestampMillis(1L);
    input.setRowCount(10L);
    input.setColumnCount(45L);
    input.setSizeInBytes(15L);

    ValueFrequency valueFrequency = new ValueFrequency();
    valueFrequency.setValue("2");
    valueFrequency.setFrequency(10L);

    Quantile quantile25 = new Quantile();
    quantile25.setQuantile("0.25");
    quantile25.setValue("1");

    Quantile quantile75 = new Quantile();
    quantile75.setQuantile("0.75");
    quantile75.setValue("5");

    input.setFieldProfiles(
        new DatasetFieldProfileArray(
            ImmutableList.of(
                new DatasetFieldProfile()
                    .setFieldPath("/field1")
                    .setMax("1")
                    .setMean("2")
                    .setStdev("3")
                    .setMedian("4")
                    .setMin("5")
                    .setNullCount(20L)
                    .setNullProportion(20.5f)
                    .setUniqueCount(30L)
                    .setUniqueProportion(30.5f)
                    .setSampleValues(new StringArray(ImmutableList.of("val1", "val2")))
                    .setQuantiles(new QuantileArray(ImmutableList.of(quantile25, quantile75)))
                    .setDistinctValueFrequencies(
                        new ValueFrequencyArray(ImmutableList.of(valueFrequency)),
                        SetMode.IGNORE_NULL),
                new DatasetFieldProfile()
                    .setFieldPath("/field2")
                    .setMax("2")
                    .setMean("3")
                    .setStdev("4")
                    .setMedian("5")
                    .setMin("6")
                    .setNullCount(30L)
                    .setNullProportion(30.5f)
                    .setUniqueCount(40L)
                    .setUniqueProportion(40.5f)
                    .setSampleValues(new StringArray(ImmutableList.of("val3", "val4")))
                    .setQuantiles(new QuantileArray(ImmutableList.of(quantile25, quantile75)))
                    .setDistinctValueFrequencies(
                        new ValueFrequencyArray(ImmutableList.of(valueFrequency)),
                        SetMode.IGNORE_NULL))));
    final EnvelopedAspect inputAspect =
        new EnvelopedAspect().setAspect(GenericRecordUtils.serializeAspect(input));
    final DatasetProfile actual = DatasetProfileMapper.map(null, inputAspect);
    final DatasetProfile expected = new DatasetProfile();
    expected.setTimestampMillis(1L);
    expected.setRowCount(10L);
    expected.setColumnCount(45L);
    expected.setSizeInBytes(15L);
    expected.setFieldProfiles(
        new ArrayList<>(
            ImmutableList.of(
                new com.linkedin.datahub.graphql.generated.DatasetFieldProfile(
                    "/field1",
                    30L,
                    30.5f,
                    20L,
                    20.5f,
                    "5",
                    "1",
                    "2",
                    "4",
                    "3",
                    new ArrayList<>(ImmutableList.of("val1", "val2")),
                    new ArrayList<com.linkedin.datahub.graphql.generated.Quantile>(
                        ImmutableList.of(
                            new com.linkedin.datahub.graphql.generated.Quantile("0.25", "1"),
                            new com.linkedin.datahub.graphql.generated.Quantile("0.75", "5"))),
                    new ArrayList<com.linkedin.datahub.graphql.generated.ValueFrequency>(
                        ImmutableList.of(
                            new com.linkedin.datahub.graphql.generated.ValueFrequency("2", 10L)))),
                new com.linkedin.datahub.graphql.generated.DatasetFieldProfile(
                    "/field2",
                    40L,
                    40.5f,
                    30L,
                    30.5f,
                    "6",
                    "2",
                    "3",
                    "5",
                    "4",
                    new ArrayList<>(ImmutableList.of("val3", "val4")),
                    new ArrayList<com.linkedin.datahub.graphql.generated.Quantile>(
                        ImmutableList.of(
                            new com.linkedin.datahub.graphql.generated.Quantile("0.25", "1"),
                            new com.linkedin.datahub.graphql.generated.Quantile("0.75", "5"))),
                    new ArrayList<com.linkedin.datahub.graphql.generated.ValueFrequency>(
                        ImmutableList.of(
                            new com.linkedin.datahub.graphql.generated.ValueFrequency(
                                "2", 10L)))))));
    Assert.assertEquals(actual.getTimestampMillis(), expected.getTimestampMillis());
    Assert.assertEquals(actual.getRowCount(), expected.getRowCount());
    Assert.assertEquals(actual.getColumnCount(), expected.getColumnCount());
    Assert.assertEquals(actual.getSizeInBytes(), expected.getSizeInBytes());

    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getFieldPath(),
        expected.getFieldProfiles().get(0).getFieldPath());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getMax(), expected.getFieldProfiles().get(0).getMax());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getMean(), expected.getFieldProfiles().get(0).getMean());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getMedian(),
        expected.getFieldProfiles().get(0).getMedian());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getNullCount(),
        expected.getFieldProfiles().get(0).getNullCount());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getNullProportion(),
        expected.getFieldProfiles().get(0).getNullProportion());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getStdev(), expected.getFieldProfiles().get(0).getStdev());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getUniqueCount(),
        expected.getFieldProfiles().get(0).getUniqueCount());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getUniqueProportion(),
        expected.getFieldProfiles().get(0).getUniqueProportion());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getSampleValues(),
        expected.getFieldProfiles().get(0).getSampleValues());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getQuantiles().get(0).getQuantile(),
        expected.getFieldProfiles().get(0).getQuantiles().get(0).getQuantile());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getQuantiles().get(0).getValue(),
        expected.getFieldProfiles().get(0).getQuantiles().get(0).getValue());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getQuantiles().get(1).getQuantile(),
        expected.getFieldProfiles().get(0).getQuantiles().get(1).getQuantile());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getQuantiles().get(1).getValue(),
        expected.getFieldProfiles().get(0).getQuantiles().get(1).getValue());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getDistinctValueFrequencies().get(0).getValue(),
        expected.getFieldProfiles().get(0).getDistinctValueFrequencies().get(0).getValue());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getDistinctValueFrequencies().get(0).getFrequency(),
        expected.getFieldProfiles().get(0).getDistinctValueFrequencies().get(0).getFrequency());

    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getFieldPath(),
        expected.getFieldProfiles().get(1).getFieldPath());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getMax(), expected.getFieldProfiles().get(1).getMax());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getMean(), expected.getFieldProfiles().get(1).getMean());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getMedian(),
        expected.getFieldProfiles().get(1).getMedian());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getNullCount(),
        expected.getFieldProfiles().get(1).getNullCount());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getNullProportion(),
        expected.getFieldProfiles().get(1).getNullProportion());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getStdev(), expected.getFieldProfiles().get(1).getStdev());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getUniqueCount(),
        expected.getFieldProfiles().get(1).getUniqueCount());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getUniqueProportion(),
        expected.getFieldProfiles().get(1).getUniqueProportion());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getSampleValues(),
        expected.getFieldProfiles().get(1).getSampleValues());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getQuantiles().get(0).getQuantile(),
        expected.getFieldProfiles().get(1).getQuantiles().get(0).getQuantile());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getQuantiles().get(0).getValue(),
        expected.getFieldProfiles().get(0).getQuantiles().get(0).getValue());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getQuantiles().get(1).getQuantile(),
        expected.getFieldProfiles().get(1).getQuantiles().get(1).getQuantile());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getQuantiles().get(1).getValue(),
        expected.getFieldProfiles().get(1).getQuantiles().get(1).getValue());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getDistinctValueFrequencies().get(0).getValue(),
        expected.getFieldProfiles().get(1).getDistinctValueFrequencies().get(0).getValue());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getDistinctValueFrequencies().get(0).getFrequency(),
        expected.getFieldProfiles().get(1).getDistinctValueFrequencies().get(0).getFrequency());
  }

  @Test
  public void testMapperPartialProfile() {
    final com.linkedin.dataset.DatasetProfile input = new com.linkedin.dataset.DatasetProfile();
    input.setTimestampMillis(1L);
    input.setRowCount(10L);
    input.setColumnCount(45L);
    input.setFieldProfiles(
        new DatasetFieldProfileArray(
            ImmutableList.of(
                new DatasetFieldProfile()
                    .setFieldPath("/field1")
                    .setUniqueCount(30L)
                    .setUniqueProportion(30.5f),
                new DatasetFieldProfile()
                    .setFieldPath("/field2")
                    .setMax("2")
                    .setMean("3")
                    .setStdev("4")
                    .setMedian("5")
                    .setMin("6")
                    .setUniqueCount(40L)
                    .setUniqueProportion(40.5f))));
    final EnvelopedAspect inputAspect =
        new EnvelopedAspect().setAspect(GenericRecordUtils.serializeAspect(input));
    final DatasetProfile actual = DatasetProfileMapper.map(null, inputAspect);
    final DatasetProfile expected = new DatasetProfile();
    expected.setTimestampMillis(1L);
    expected.setRowCount(10L);
    expected.setColumnCount(45L);
    expected.setFieldProfiles(
        new ArrayList<>(
            ImmutableList.of(
                new com.linkedin.datahub.graphql.generated.DatasetFieldProfile(
                    "/field1", 30L, 30.5f, null, null, null, null, null, null, null, null, null,
                    null),
                new com.linkedin.datahub.graphql.generated.DatasetFieldProfile(
                    "/field2", 40L, 40.5f, null, null, "6", "2", "3", "5", "4", null, null,
                    null))));
    Assert.assertEquals(actual.getTimestampMillis(), expected.getTimestampMillis());
    Assert.assertEquals(actual.getRowCount(), expected.getRowCount());
    Assert.assertEquals(actual.getColumnCount(), expected.getColumnCount());
    Assert.assertEquals(actual.getSizeInBytes(), expected.getSizeInBytes());

    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getFieldPath(),
        expected.getFieldProfiles().get(0).getFieldPath());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getMax(), expected.getFieldProfiles().get(0).getMax());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getMean(), expected.getFieldProfiles().get(0).getMean());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getMedian(),
        expected.getFieldProfiles().get(0).getMedian());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getNullCount(),
        expected.getFieldProfiles().get(0).getNullCount());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getNullProportion(),
        expected.getFieldProfiles().get(0).getNullProportion());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getStdev(), expected.getFieldProfiles().get(0).getStdev());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getUniqueCount(),
        expected.getFieldProfiles().get(0).getUniqueCount());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getUniqueProportion(),
        expected.getFieldProfiles().get(0).getUniqueProportion());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getSampleValues(),
        expected.getFieldProfiles().get(0).getSampleValues());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getQuantiles(),
        expected.getFieldProfiles().get(0).getQuantiles());
    Assert.assertEquals(
        actual.getFieldProfiles().get(0).getDistinctValueFrequencies(),
        expected.getFieldProfiles().get(0).getDistinctValueFrequencies());

    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getFieldPath(),
        expected.getFieldProfiles().get(1).getFieldPath());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getMax(), expected.getFieldProfiles().get(1).getMax());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getMean(), expected.getFieldProfiles().get(1).getMean());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getMedian(),
        expected.getFieldProfiles().get(1).getMedian());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getNullCount(),
        expected.getFieldProfiles().get(1).getNullCount());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getNullProportion(),
        expected.getFieldProfiles().get(1).getNullProportion());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getStdev(), expected.getFieldProfiles().get(1).getStdev());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getUniqueCount(),
        expected.getFieldProfiles().get(1).getUniqueCount());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getUniqueProportion(),
        expected.getFieldProfiles().get(1).getUniqueProportion());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getSampleValues(),
        expected.getFieldProfiles().get(1).getSampleValues());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getQuantiles(),
        expected.getFieldProfiles().get(1).getQuantiles());
    Assert.assertEquals(
        actual.getFieldProfiles().get(1).getDistinctValueFrequencies(),
        expected.getFieldProfiles().get(1).getDistinctValueFrequencies());
  }

  @Test
  public void testMapperSamplePartitionSpec() {
    final com.linkedin.dataset.DatasetProfile input = new com.linkedin.dataset.DatasetProfile();
    input.setTimestampMillis(1L);
    input.setPartitionSpec(
        new PartitionSpec()
            .setType(com.linkedin.timeseries.PartitionType.QUERY)
            .setPartition("SAMPLE"));

    final DatasetProfile actual = mapProfile(input);

    Assert.assertEquals(actual.getPartitionSpec().getType(), PartitionType.QUERY);
    Assert.assertEquals(actual.getPartitionSpec().getPartition(), "SAMPLE");
    Assert.assertNull(actual.getPartitionSpec().getTimePartition());
  }

  @Test
  public void testMapperPartitionIdPartitionSpec() {
    final com.linkedin.dataset.DatasetProfile input = new com.linkedin.dataset.DatasetProfile();
    input.setTimestampMillis(1L);
    input.setPartitionSpec(new PartitionSpec().setPartition("ds=2024-01-01"));

    final DatasetProfile actual = mapProfile(input);

    Assert.assertEquals(actual.getPartitionSpec().getType(), PartitionType.PARTITION);
    Assert.assertEquals(actual.getPartitionSpec().getPartition(), "ds=2024-01-01");
    Assert.assertNull(actual.getPartitionSpec().getTimePartition());
  }

  @Test
  public void testMapperNoPartitionSpec() {
    final com.linkedin.dataset.DatasetProfile input = new com.linkedin.dataset.DatasetProfile();
    input.setTimestampMillis(1L);
    input.setRowCount(10L);

    final DatasetProfile actual = mapProfile(input);

    Assert.assertNull(actual.getPartitionSpec());
  }

  @Test
  public void testMapperTimePartitionWithFixedLength() {
    final com.linkedin.dataset.DatasetProfile input = new com.linkedin.dataset.DatasetProfile();
    input.setTimestampMillis(1L);
    input.setPartitionSpec(
        new PartitionSpec()
            .setPartition("ds=2024-01-01")
            .setTimePartition(
                new TimeWindow()
                    .setStartTimeMillis(1704067200000L)
                    .setLength(new TimeWindowSize().setUnit(CalendarInterval.DAY).setMultiple(2))));

    final DatasetProfile actual = mapProfile(input);

    Assert.assertEquals(actual.getPartitionSpec().getPartition(), "ds=2024-01-01");
    Assert.assertEquals(
        actual.getPartitionSpec().getTimePartition().getStartTimeMillis(), (Long) 1704067200000L);
    Assert.assertEquals(
        actual.getPartitionSpec().getTimePartition().getDurationMillis(),
        (Long) (2L * 24 * 60 * 60 * 1000));
  }

  @Test
  public void testMapperTimePartitionWithCalendarLength() {
    final com.linkedin.dataset.DatasetProfile input = new com.linkedin.dataset.DatasetProfile();
    input.setTimestampMillis(1L);
    final TimeWindow timePartition =
        new TimeWindow()
            .setStartTimeMillis(1704067200000L)
            .setLength(new TimeWindowSize().setUnit(CalendarInterval.MONTH));
    input.setPartitionSpec(
        new PartitionSpec().setPartition("month=2024-01").setTimePartition(timePartition));

    final DatasetProfile actual = mapProfile(input);

    Assert.assertEquals(actual.getPartitionSpec().getPartition(), "month=2024-01");
    Assert.assertEquals(
        actual.getPartitionSpec().getTimePartition().getStartTimeMillis(), (Long) 1704067200000L);
    Assert.assertEquals(
        actual.getPartitionSpec().getTimePartition().getDurationMillis(),
        (Long) (TimeWindowUtils.getEndTimeMillis(timePartition) - 1704067200000L));
  }

  @Test
  public void testMapperTimePartitionWithoutLengthIsOmitted() {
    final com.linkedin.dataset.DatasetProfile input = new com.linkedin.dataset.DatasetProfile();
    input.setTimestampMillis(1L);
    input.setPartitionSpec(
        new PartitionSpec()
            .setPartition("ds=2024-01-01")
            .setTimePartition(new TimeWindow().setStartTimeMillis(1704067200000L)));

    final DatasetProfile actual = mapProfile(input);

    Assert.assertEquals(actual.getPartitionSpec().getType(), PartitionType.PARTITION);
    Assert.assertEquals(actual.getPartitionSpec().getPartition(), "ds=2024-01-01");
    Assert.assertNull(actual.getPartitionSpec().getTimePartition());
  }

  private static DatasetProfile mapProfile(final com.linkedin.dataset.DatasetProfile input) {
    final EnvelopedAspect inputAspect =
        new EnvelopedAspect().setAspect(GenericRecordUtils.serializeAspect(input));
    return DatasetProfileMapper.map(null, inputAspect);
  }
}
