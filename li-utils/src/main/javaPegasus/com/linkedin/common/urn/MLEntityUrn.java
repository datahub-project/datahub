package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;


public final class MLEntityUrn extends Urn {

  public static final String ENTITY_TYPE = "mlEntity";

  private final String _mlEntityNamespace;

  private final String _mlEntityName;

  public MLEntityUrn(String mlEntityNamespace, String mlEntityName) {
    super(ENTITY_TYPE, TupleKey.create(mlEntityNamespace, mlEntityName));
    this._mlEntityNamespace = mlEntityNamespace;
    this._mlEntityName = mlEntityName;
  }

  public String getMlEntityNameEntity() {
    return _mlEntityName;
  }

  public String getMlEntityNamespaceEntity() {
    return _mlEntityNamespace;
  }

  public static MLEntityUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static MLEntityUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'mlEntity'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 2) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new MLEntityUrn((String) key.getAs(0, String.class), (String) key.getAs(1, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  static {
    Custom.registerCoercer(new DirectCoercer<MLEntityUrn>() {
      public Object coerceInput(MLEntityUrn object) throws ClassCastException {
        return object.toString();
      }

      public MLEntityUrn coerceOutput(Object object) throws TemplateOutputCastException {
        try {
          return MLEntityUrn.createFromString((String) object);
        } catch (URISyntaxException e) {
          throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
        }
      }
    }, MLEntityUrn.class);
  }
}
