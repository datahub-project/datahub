package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class CorpuserUrn extends Urn {

  public static final String ENTITY_TYPE = "corpuser";

  private final String _username;

  public CorpuserUrn(String username) {
    super(ENTITY_TYPE, TupleKey.create(username));
    this._username = username;
  }

  public String getUsernameEntity() {
    return _username;
  }

  public static CorpuserUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static CorpuserUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'corpuser'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 1) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new CorpuserUrn((String) key.getAs(0, String.class));
        } catch (Exception var3) {
          throw new URISyntaxException(
              urn.toString(), "Invalid URN Parameter: '" + var3.getMessage());
        }
      }
    }
  }

  public static CorpuserUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(
        new DirectCoercer<CorpuserUrn>() {
          public Object coerceInput(CorpuserUrn object) throws ClassCastException {
            return object.toString();
          }

          public CorpuserUrn coerceOutput(Object object) throws TemplateOutputCastException {
            try {
              return CorpuserUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        CorpuserUrn.class);
  }
}
