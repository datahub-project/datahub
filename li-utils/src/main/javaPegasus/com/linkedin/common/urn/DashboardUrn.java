package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class DashboardUrn extends Urn {

  public static final String ENTITY_TYPE = "dashboard";

  private final String _dashboardTool;
  private final String _dashboardId;

  public DashboardUrn(String dashboardTool, String dashboardId) {
    super(ENTITY_TYPE, TupleKey.create(dashboardTool, dashboardId));
    this._dashboardTool = dashboardTool;
    this._dashboardId = dashboardId;
  }

  public String getDashboardToolEntity() {
    return _dashboardTool;
  }

  public String getDashboardIdEntity() {
    return _dashboardId;
  }

  public static DashboardUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static DashboardUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'dashboard'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 2) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new DashboardUrn(
              (String) key.getAs(0, String.class), (String) key.getAs(1, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  public static DashboardUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(
        new DirectCoercer<DashboardUrn>() {
          public Object coerceInput(DashboardUrn object) throws ClassCastException {
            return object.toString();
          }

          public DashboardUrn coerceOutput(Object object) throws TemplateOutputCastException {
            try {
              return DashboardUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        DashboardUrn.class);
  }
}
