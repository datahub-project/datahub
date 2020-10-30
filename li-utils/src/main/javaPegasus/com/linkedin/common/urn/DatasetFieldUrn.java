package com.linkedin.common.urn;

import com.linkedin.common.FabricType;
import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Standardized dataset field information identifier
 */
public class DatasetFieldUrn extends Urn {

  // uniquely identifies urn's key type
  public static final String ENTITY_TYPE = "datasetField";

  // urn pattern
  private static final Pattern DATASET_FIELD_URN_PATTERN = Pattern.compile(
      "urn:li:datasetField:\\(urn:li:dataset:\\(urn:li:dataPlatform:(?<dataPlatform>.+),(?<datasetName>.+),(?<fabric>.+)\\),(?<fieldPath>.+)\\)");

  /**
   * Dataset urn of the datasetFieldUrn
   */
  private final DatasetUrn _dataset;

  /**
   * Field of datasetFieldUrn
   */
  private final String _fieldPath;

  static {
    Custom.initializeCustomClass(DatasetUrn.class);
    Custom.registerCoercer(new DirectCoercer<DatasetFieldUrn>() {

      @Override
      public String coerceInput(DatasetFieldUrn object) throws ClassCastException {
        return object.toString();
      }

      @Override
      public DatasetFieldUrn coerceOutput(Object object) throws TemplateOutputCastException {
        if (object instanceof String) {
          try {
            return DatasetFieldUrn.deserialize(((String) object));
          } catch (URISyntaxException e) {
            throw new TemplateOutputCastException((("Deserializing output '" + object) + "' failed"), e);
          }
        }
        throw new TemplateOutputCastException((("Output '" + object) + ("' is not a String, and cannot be coerced to "
            + DatasetFieldUrn.class.getName())));
      }
    }, DatasetFieldUrn.class);
  }

  /**
   * Creates a new instance of a {@link DatasetFieldUrn }.
   *
   * @param dataset Dataset that this dataset field belongs to.
   * @param fieldPath Dataset field path or column name
   */
  public DatasetFieldUrn(DatasetUrn dataset, String fieldPath) {
    this(dataset.getPlatformEntity().getPlatformNameEntity(), dataset.getDatasetNameEntity(), dataset.getOriginEntity(),
        fieldPath);
  }

  public DatasetFieldUrn(String dataPlatform, String datasetName, FabricType fabricType, String fieldPath) {
    super(ENTITY_TYPE, String.format("(urn:li:dataset:(urn:li:dataPlatform:%s,%s,%s),%s)", dataPlatform, datasetName,
        fabricType.name(), fieldPath));
    this._dataset = new DatasetUrn(new DataPlatformUrn(dataPlatform), datasetName, fabricType);
    this._fieldPath = fieldPath;
  }

  public DatasetUrn getDatasetEntity() {
    return _dataset;
  }

  public String getFieldPathEntity() {
    return _fieldPath;
  }

  /**
   * Creates an instance of a DatasetFieldUrn from a raw urn string.
   * @param rawUrn The raw urn input to convert to a full DatasetFieldUrn instance.
   * @return {@link DatasetFieldUrn} dataset Field Urn
   */
  public static DatasetFieldUrn deserialize(String rawUrn) throws URISyntaxException {
    final Matcher matcher = DATASET_FIELD_URN_PATTERN.matcher(rawUrn);
    if (matcher.matches()) {
      final String dataPlatform = matcher.group("dataPlatform");
      final String datasetName = matcher.group("datasetName");
      final String fabric = matcher.group("fabric");
      final String fieldName = matcher.group("fieldPath");
      return new DatasetFieldUrn(dataPlatform, datasetName, FabricType.valueOf(fabric), fieldName);
    }
    throw new URISyntaxException(rawUrn,
        String.format("urn does match dataset field urn pattern %s", DATASET_FIELD_URN_PATTERN.toString()));
  }
}
