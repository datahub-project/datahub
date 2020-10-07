package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;


public final class DashboardUrn extends Urn {

  public static final String ENTITY_TYPE = "dashboard";

  private static final String CONTENT_FORMAT = "(%s,%s)";

  private final String dashboardToolEntity;

  private final String dashboardIdEntity;

  public DashboardUrn(String dashboardTool, String dashboardId) {
    super(ENTITY_TYPE, String.format(CONTENT_FORMAT, dashboardTool, dashboardId));
    this.dashboardToolEntity = dashboardTool;
    this.dashboardIdEntity = dashboardId;
  }

  public String getDashboardToolEntity() {
    return dashboardToolEntity;
  }

  public String getDashboardIdEntity() {
    return dashboardIdEntity;
  }

  public static DashboardUrn createFromString(String rawUrn) throws URISyntaxException {
    Urn urn = new Urn(rawUrn);
    validateUrn(urn, ENTITY_TYPE);
    String[] urnParts = urn.getContent().split(",");
    return new DashboardUrn(urnParts[0], urnParts[1]);
  }

  public static DashboardUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(new DirectCoercer<DashboardUrn>() {
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
    }, DashboardUrn.class);
  }
}
