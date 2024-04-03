package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class FabricUrn extends Urn {

  public static final String ENTITY_TYPE = "fabric";

  private final String _name;

  public FabricUrn(String name) {
    super(ENTITY_TYPE, TupleKey.create(name));
    this._name = name;
  }

  public String getNameEntity() {
    return _name;
  }

  public static FabricUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static FabricUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'fabric'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 1) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new FabricUrn((String) key.getAs(0, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  static {
    Custom.registerCoercer(
        new DirectCoercer<FabricUrn>() {
          public Object coerceInput(FabricUrn object) throws ClassCastException {
            return object.toString();
          }

          public FabricUrn coerceOutput(Object object) throws TemplateOutputCastException {
            try {
              return FabricUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        FabricUrn.class);
  }
}
