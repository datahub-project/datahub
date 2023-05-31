package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;


public class JoinUrn extends Urn {
  public static final String ENTITY_TYPE = "join";

  private final String _joinId;

  public JoinUrn(String joinId) {
    super(ENTITY_TYPE, TupleKey.create(joinId));
    this._joinId = joinId;
  }

  public String getJoinIdEntity() {
    return _joinId;
  }

  public static JoinUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static JoinUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'join'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 1) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new JoinUrn((String) key.getAs(0, String.class));
        } catch (Exception var3) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + var3.getMessage());
        }
      }
    }
  }

  public static JoinUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.initializeCustomClass(JoinUrn.class);
    Custom.registerCoercer(new DirectCoercer<JoinUrn>() {
      public Object coerceInput(JoinUrn object) throws ClassCastException {
        return object.toString();
      }

      public JoinUrn coerceOutput(Object object) throws TemplateOutputCastException {
        try {
          return JoinUrn.createFromString((String) object);
        } catch (URISyntaxException e) {
          throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
        }
      }
    }, JoinUrn.class);
  }
}
