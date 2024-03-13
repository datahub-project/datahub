package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public class ERModelRelationUrn extends Urn {
  public static final String ENTITY_TYPE = "erModelRelationship";

  private final String _ermodelrelationId;

  public ERModelRelationUrn(String ermodelrelationId) {
    super(ENTITY_TYPE, TupleKey.create(ermodelrelationId));
    this._ermodelrelationId = ermodelrelationId;
  }

  public String getERModelRelationIdEntity() {
    return _ermodelrelationId;
  }

  public static ERModelRelationUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static ERModelRelationUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(
          urn.toString(), "Urn entity type should be 'erModelRelationship'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 1) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new ERModelRelationUrn((String) key.getAs(0, String.class));
        } catch (Exception var3) {
          throw new URISyntaxException(
              urn.toString(), "Invalid URN Parameter: '" + var3.getMessage());
        }
      }
    }
  }

  public static ERModelRelationUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.initializeCustomClass(ERModelRelationUrn.class);
    Custom.registerCoercer(
        new DirectCoercer<ERModelRelationUrn>() {
          public Object coerceInput(ERModelRelationUrn object) throws ClassCastException {
            return object.toString();
          }

          public ERModelRelationUrn coerceOutput(Object object) throws TemplateOutputCastException {
            try {
              return ERModelRelationUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        ERModelRelationUrn.class);
  }
}
