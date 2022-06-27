package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;


public final class CorpRoleUrn extends Urn {

  public static final String ENTITY_TYPE = "corpRole";

  private final String _roleName;

  public CorpRoleUrn(String roleName) {
    super(ENTITY_TYPE, TupleKey.createWithOneKeyPart(roleName));
    this._roleName = roleName;
  }

  private CorpRoleUrn(TupleKey entityKey, String roleName) {
    super("li", "corpRole", entityKey);
    this._roleName = roleName;
  }

  public String getRoleNameEntity() {
    return _roleName;
  }

  public static CorpRoleUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  private static CorpRoleUrn decodeUrn(String roleName) throws Exception {
    return new CorpRoleUrn(TupleKey.create(new Object[]{roleName}), roleName);
  }

  public static CorpRoleUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'corpRole'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 1) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return decodeUrn((String)key.getAs(0, String.class));
        } catch (Exception var3) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + var3.getMessage());
        }
      }
    }
  }

  public static CorpRoleUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(new DirectCoercer<CorpRoleUrn>() {
      public Object coerceInput(CorpRoleUrn object) throws ClassCastException {
        return object.toString();
      }

      public CorpRoleUrn coerceOutput(Object object) throws TemplateOutputCastException {
        try {
          return CorpRoleUrn.createFromString((String) object);
        } catch (URISyntaxException e) {
          throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
        }
      }
    }, CorpRoleUrn.class);
  }
}
