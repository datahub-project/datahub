package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;


public final class MLPrimaryKeyUrn extends Urn {

  public static final String ENTITY_TYPE = "mlPrimaryKey";

  private final String _mlPrimaryKeyNamespace;

  private final String _mlPrimaryKeyName;

  public MLPrimaryKeyUrn(String mlPrimaryKeyNamespace, String mlPrimaryKeyName) {
    super(ENTITY_TYPE, TupleKey.create(mlPrimaryKeyNamespace, mlPrimaryKeyName));
    this._mlPrimaryKeyNamespace = mlPrimaryKeyNamespace;
    this._mlPrimaryKeyName = mlPrimaryKeyName;
  }

  public String getMlPrimaryKeyNameEntity() {
    return _mlPrimaryKeyName;
  }

  public String getMlPrimaryKeyNamespaceEntity() {
    return _mlPrimaryKeyNamespace;
  }

  public static MLPrimaryKeyUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static MLPrimaryKeyUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'mlPrimaryKey'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 2) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new MLPrimaryKeyUrn((String) key.getAs(0, String.class), (String) key.getAs(1, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  static {
    Custom.registerCoercer(new DirectCoercer<MLPrimaryKeyUrn>() {
      public Object coerceInput(MLPrimaryKeyUrn object) throws ClassCastException {
        return object.toString();
      }

      public MLPrimaryKeyUrn coerceOutput(Object object) throws TemplateOutputCastException {
        try {
          return MLPrimaryKeyUrn.createFromString((String) object);
        } catch (URISyntaxException e) {
          throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
        }
      }
    }, MLPrimaryKeyUrn.class);
  }
}
