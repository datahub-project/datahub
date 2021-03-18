package com.linkedin.metadata.builders.common;

import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.urn.DatasetUrn;

import static com.linkedin.metadata.testing.Owners.*;
import static com.linkedin.metadata.testing.Urns.*;
import static com.linkedin.metadata.utils.AuditStamps.*;


public class DataJobTestUtils {

  private DataJobTestUtils() {
    // Util class should not have public constructor
  }

  public static DataJobInputOutput makeDataJobInputOutput() {
    DatasetUrn input1 = makeDatasetUrn("input1");
    DatasetUrn input2 = makeDatasetUrn("input2");
    DatasetUrn output1 = makeDatasetUrn("output1");
    DatasetUrn output2 = makeDatasetUrn("output2");

    return new DataJobInputOutput()
        .setInputDatasets(new DatasetUrnArray(input1, input2))
        .setOutputDatasets(new DatasetUrnArray(output1, output2));
  }
}