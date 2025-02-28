package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class DataHubRoleUrn extends Urn {

  public static final String ENTITY_TYPE = "dataHubRole";
  private final String _roleName;

  public DataHubRoleUrn(String roleName) {
    super(ENTITY_TYPE, TupleKey.createWithOneKeyPart(roleName));
    this._roleName = roleName;
  }

  public String getRoleName() {
    return _roleName;
  }

  public static DataHubRoleUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static DataHubRoleUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(
          urn.toString(), String.format("Urn entity type should be '%s'.", ENTITY_TYPE));
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 1) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new DataHubRoleUrn((String) key.getAs(0, String.class));
        } catch (Exception var3) {
          throw new URISyntaxException(
              urn.toString(), "Invalid URN Parameter: '" + var3.getMessage());
        }
      }
    }
  }

  public static DataHubRoleUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(
        new DirectCoercer<DataHubRoleUrn>() {
          public Object coerceInput(DataHubRoleUrn object) throws ClassCastException {
            return object.toString();
          }

          public DataHubRoleUrn coerceOutput(Object object) throws TemplateOutputCastException {
            try {
              return DataHubRoleUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        DataHubRoleUrn.class);
  }
}
