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

  @Test
  public void testDatasetNameWhitespaceTrimming() throws URISyntaxException {
    // This test verifies the sanitizeName() logic correctly removes
    // ONLY leading and trailing whitespace from the dataset name.
    //
    // Example:
    // "   fooName   "  →  "fooName"
    //
    // This ensures that padded names coming from DB2 or other systems
    // are normalized before being stored or serialized into URNs.

    final String datasetNameWithSpaces = "   fooName   "; // leading + trailing spaces
    final String trimmed = "fooName"; // expected result after trimming

    // Construct a URN that contains a dataset name with padding.
    final String rawUrn =
        String.format(
            "urn:li:dataset:(urn:li:dataPlatform:%s,%s,%s)",
            PLATFORM, datasetNameWithSpaces, FABRIC_TYPE);

    // Deserialize the URN; this triggers sanitizeName() internally.
    DatasetUrn datasetUrn = DatasetUrn.deserialize(rawUrn);

    // Validate trimming applied correctly.
    Assertions.assertThat(datasetUrn.getDatasetNameEntity())
        .isEqualTo(trimmed)
        .describedAs("Dataset name should have leading and trailing spaces removed");

    // Validate that serialization also uses the trimmed value.
    final String expectedSerialized =
        String.format(
            "urn:li:dataset:(urn:li:dataPlatform:%s,%s,%s)", PLATFORM, trimmed, FABRIC_TYPE);

    Assertions.assertThat(datasetUrn.toString())
        .isEqualTo(expectedSerialized)
        .describedAs("Serialized URN should contain the trimmed dataset name");
  }

  @Test
  public void testInternalWhitespaceIsPreserved() throws URISyntaxException {
    // This test ensures sanitizeName() does NOT modify internal whitespace.
    //
    // Example:
    // "foo   bar   baz"  →  "foo   bar   baz"  (internal spaces preserved)
    //
    // This is important because internal spaces may be meaningful,
    // and our trimming logic should only remove padding on the left/right,
    // not collapse or alter spacing inside the dataset name.

    final String datasetName = "foo   bar   baz"; // internal spaces only (3 spaces between words)

    // Serialize a URN using a name that contains internal whitespace.
    final String rawUrn =
        String.format(
            "urn:li:dataset:(urn:li:dataPlatform:%s,%s,%s)", PLATFORM, datasetName, FABRIC_TYPE);

    // Deserialize the URN.
    DatasetUrn datasetUrn = DatasetUrn.deserialize(rawUrn);

    // Verify internal whitespace was preserved exactly.
    Assertions.assertThat(datasetUrn.getDatasetNameEntity())
        .isEqualTo(datasetName)
        .describedAs("Internal whitespace should be preserved unchanged");
  }
}
