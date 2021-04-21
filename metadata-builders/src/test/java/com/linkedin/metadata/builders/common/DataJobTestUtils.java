package com.linkedin.metadata.builders.common;

import com.linkedin.metadata.aspect.DataJobAspect;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.urn.DatasetUrn;

import static com.linkedin.metadata.testing.Owners.*;
import static com.linkedin.metadata.testing.Urns.*;

public class DataJobTestUtils {

  private DataJobTestUtils() {
    // Util class should not have public constructor
  }

  public static DataJobInputOutput makeDataJobInputOutput() {
    DatasetUrn input1 = makeDatasetUrn("input1");
    DatasetUrn input2 = makeDatasetUrn("input2");
    DatasetUrn output1 = makeDatasetUrn("output1");
    DatasetUrn output2 = makeDatasetUrn("output2");

    return new DataJobInputOutput().setInputDatasets(new DatasetUrnArray(input1, input2))
        .setOutputDatasets(new DatasetUrnArray(output1, output2));
  }

  public static DataJobAspect makeDataJobInputOutputAspect() {
    DataJobAspect aspect = new DataJobAspect();
    aspect.setDataJobInputOutput(makeDataJobInputOutput());
    return aspect;
  }

  public static DataJobAspect makeOwnershipAspect() {
    DataJobAspect aspect = new DataJobAspect();
    aspect.setOwnership(makeOwnership("fooUser"));
    return aspect;
  }

  public static DataJobAspect makeDataJobInfoAspect() {
    DataJobInfo dataJobInfo = new DataJobInfo();
    dataJobInfo.setName("You had one Job");
    dataJobInfo.setDescription("A Job for one");
    DataJobAspect aspect = new DataJobAspect();
    aspect.setDataJobInfo(dataJobInfo);
    return aspect;
  }
}
