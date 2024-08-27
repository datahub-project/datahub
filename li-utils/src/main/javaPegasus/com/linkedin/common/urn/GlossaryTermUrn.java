package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class GlossaryTermUrn extends Urn {

  public static final String ENTITY_TYPE = "glossaryTerm";

  private final String _name;

  public GlossaryTermUrn(String name) {
    super(ENTITY_TYPE, TupleKey.create(name));
    this._name = name;
  }

  public String getNameEntity() {
    return _name;
  }

  public static GlossaryTermUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static GlossaryTermUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'glossaryTerm'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 1) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new GlossaryTermUrn((String) key.getAs(0, String.class));
        } catch (Exception var3) {
          throw new URISyntaxException(
              urn.toString(), "Invalid URN Parameter: '" + var3.getMessage());
        }
      }
    }
  }

  public static GlossaryTermUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(
        new DirectCoercer<GlossaryTermUrn>() {
          public Object coerceInput(GlossaryTermUrn object) throws ClassCastException {
            return object.toString();
          }

          public GlossaryTermUrn coerceOutput(Object object) throws TemplateOutputCastException {
            try {
              return GlossaryTermUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        GlossaryTermUrn.class);
  }
}
