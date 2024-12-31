package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class BusinessAttributeUrn extends Urn {

  public static final String ENTITY_TYPE = "businessAttribute";

  private final String _name;

  public BusinessAttributeUrn(String name) {
    super(ENTITY_TYPE, TupleKey.create(name));
    this._name = name;
  }

  public String getName() {
    return _name;
  }

  public static BusinessAttributeUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static BusinessAttributeUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(
          urn.toString(), "Urn entity type should be '" + urn.getEntityType() + "'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 1) {
        throw new URISyntaxException(
            urn.toString(), "Invalid number of keys: found " + key.size() + " expected 1.");
      } else {
        try {
          return new BusinessAttributeUrn((String) key.getAs(0, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  public static BusinessAttributeUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(
        new DirectCoercer<BusinessAttributeUrn>() {
          public Object coerceInput(BusinessAttributeUrn object) throws ClassCastException {
            return object.toString();
          }

          public BusinessAttributeUrn coerceOutput(Object object)
              throws TemplateOutputCastException {
            try {
              return BusinessAttributeUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        BusinessAttributeUrn.class);
  }
}
