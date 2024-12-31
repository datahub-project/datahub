package com.linkedin.common.urn;

import com.linkedin.common.FabricType;
import java.net.URISyntaxException;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

public class DatasetFieldUrnTest {

  private static final String PLATFORM = "fooPlatform";
  private static final String DATASET_NAME = "fooName";
  private static final String FIELD_NAME = "fooField";
  private static final FabricType FABRIC_TYPE = FabricType.PROD;

  @Test
  public void testSerialization() throws URISyntaxException {
    final String datasetFieldString =
        String.format(
            "urn:li:datasetField:(urn:li:dataset:(urn:li:dataPlatform:%s,%s,%s),%s)",
            PLATFORM, DATASET_NAME, FABRIC_TYPE, FIELD_NAME);

    final DatasetFieldUrn datasetFieldUrn = DatasetFieldUrn.deserialize(datasetFieldString);
    final DatasetUrn datasetUrn = datasetFieldUrn.getDatasetEntity();

    Assertions.assertThat(datasetFieldUrn.getFieldPathEntity()).isEqualTo(FIELD_NAME);
    Assertions.assertThat(datasetUrn.getDatasetNameEntity()).isEqualTo(DATASET_NAME);
    Assertions.assertThat(datasetUrn.getPlatformEntity().getPlatformNameEntity())
        .isEqualTo(PLATFORM);
    Assertions.assertThat(datasetUrn.getOriginEntity()).isEqualTo(FabricType.PROD);
    Assertions.assertThat(datasetFieldUrn.toString())
        .isEqualTo(datasetFieldString)
        .describedAs(
            "serialization followed by deserialization should produce the same urn string");
  }

  @Test
  public void testCreateUrn() {
    final DatasetFieldUrn datasetFieldUrn =
        new DatasetFieldUrn(PLATFORM, DATASET_NAME, FABRIC_TYPE, FIELD_NAME);

    final DatasetUrn datasetUrn = datasetFieldUrn.getDatasetEntity();

    Assertions.assertThat(datasetFieldUrn.getFieldPathEntity()).isEqualTo(FIELD_NAME);
    Assertions.assertThat(datasetUrn.getDatasetNameEntity()).isEqualTo(DATASET_NAME);
    Assertions.assertThat(datasetUrn.getPlatformEntity().getPlatformNameEntity())
        .isEqualTo(PLATFORM);
    Assertions.assertThat(datasetUrn.getOriginEntity()).isEqualTo(FabricType.PROD);
  }

  @Test
  public void testUrnConstructors() {
    final DatasetFieldUrn datasetFieldUrn1 =
        new DatasetFieldUrn(PLATFORM, DATASET_NAME, FABRIC_TYPE, FIELD_NAME);
    final DatasetUrn datasetUrn = datasetFieldUrn1.getDatasetEntity();
    final DatasetFieldUrn datasetFieldUrn2 = new DatasetFieldUrn(datasetUrn, FIELD_NAME);

    Assertions.assertThat(datasetFieldUrn1).isEqualTo(datasetFieldUrn2);
  }
}
