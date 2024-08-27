package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class MLFeatureUrn extends Urn {

  public static final String ENTITY_TYPE = "mlFeature";

  private final String _mlFeatureNamespace;

  private final String _mlFeatureName;

  public MLFeatureUrn(String mlFeatureNamespace, String mlFeatureName) {
    super(ENTITY_TYPE, TupleKey.create(mlFeatureNamespace, mlFeatureName));
    this._mlFeatureNamespace = mlFeatureNamespace;
    this._mlFeatureName = mlFeatureName;
  }

  public String getMlFeatureNameEntity() {
    return _mlFeatureName;
  }

  public String getMlFeatureNamespaceEntity() {
    return _mlFeatureNamespace;
  }

  public static MLFeatureUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static MLFeatureUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'mlFeature'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 2) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new MLFeatureUrn(
              (String) key.getAs(0, String.class), (String) key.getAs(1, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  static {
    Custom.registerCoercer(
        new DirectCoercer<MLFeatureUrn>() {
          public Object coerceInput(MLFeatureUrn object) throws ClassCastException {
            return object.toString();
          }

          public MLFeatureUrn coerceOutput(Object object) throws TemplateOutputCastException {
            try {
              return MLFeatureUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        MLFeatureUrn.class);
  }
}
