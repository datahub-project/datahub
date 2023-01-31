package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.DatasetProfile;
import com.linkedin.dataset.DatasetFieldProfile;
import com.linkedin.dataset.DatasetFieldProfileArray;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.utils.GenericRecordUtils;
import java.util.ArrayList;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DatasetProfileMapperTest {
  @Test
  public void testMapperFullProfile() throws Exception {
    final com.linkedin.dataset.DatasetProfile input = new com.linkedin.dataset.DatasetProfile();
    input.setTimestampMillis(1L);
    input.setRowCount(10L);
    input.setColumnCount(45L);
    input.setSizeInBytes(15L);
    input.setFieldProfiles(new DatasetFieldProfileArray(ImmutableList.of(
        new DatasetFieldProfile().setFieldPath("/field1")
            .setMax("1")
            .setMean("2")
            .setStdev("3")
            .setMedian("4")
            .setMin("5")
            .setNullCount(20L)
            .setNullProportion(20.5f)
            .setUniqueCount(30L)
            .setUniqueProportion(30.5f)
            .setSampleValues(new StringArray(ImmutableList.of("val1", "val2"))),
        new DatasetFieldProfile().setFieldPath("/field2")
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
    )));
    final EnvelopedAspect inputAspect = new EnvelopedAspect()
        .setAspect(GenericRecordUtils.serializeAspect(input));
    final DatasetProfile actual = DatasetProfileMapper.map(inputAspect);
    final DatasetProfile expected = new DatasetProfile();
    expected.setTimestampMillis(1L);
    expected.setRowCount(10L);
    expected.setColumnCount(45L);
    expected.setSizeInBytes(15L);
    expected.setFieldProfiles(new ArrayList<>(
        ImmutableList.of(
            new com.linkedin.datahub.graphql.generated.DatasetFieldProfile("/field1", 30L, 30.5f, 20L, 20.5f, "5", "1", "2", "4", "3", new ArrayList<>(ImmutableList.of("val1", "val2"))),
            new com.linkedin.datahub.graphql.generated.DatasetFieldProfile("/field2", 40L, 40.5f, 30L, 30.5f, "6", "2", "3", "5", "4", new ArrayList<>(ImmutableList.of("val3", "val4")))
        )
    ));

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMapperPartialProfile() throws Exception {
  }
}
