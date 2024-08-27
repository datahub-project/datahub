package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class TagUrn extends Urn {

  public static final String ENTITY_TYPE = "tag";

  private final String _name;

  public TagUrn(String name) {
    super(ENTITY_TYPE, TupleKey.create(name));
    this._name = name;
  }

  public String getName() {
    return _name;
  }

  public static TagUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static TagUrn createFromUrn(Urn urn) throws URISyntaxException {
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
          return new TagUrn((String) key.getAs(0, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  public static TagUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(
        new DirectCoercer<TagUrn>() {
          public Object coerceInput(TagUrn object) throws ClassCastException {
            return object.toString();
          }

          public TagUrn coerceOutput(Object object) throws TemplateOutputCastException {
            try {
              return TagUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        TagUrn.class);
  }
}
