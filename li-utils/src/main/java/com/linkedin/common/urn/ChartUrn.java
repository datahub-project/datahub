package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;


public final class ChartUrn extends Urn {

  public static final String ENTITY_TYPE = "chart";

  private static final String CONTENT_FORMAT = "(%s,%s)";

  private final String dashboardToolEntity;

  private final String chartIdEntity;

  public ChartUrn(String dashboardTool, String chartId) {
    super(ENTITY_TYPE, String.format(CONTENT_FORMAT, dashboardTool, chartId));
    this.dashboardToolEntity = dashboardTool;
    this.chartIdEntity = chartId;
  }

  public String getDashboardToolEntity() {
    return dashboardToolEntity;
  }

  public String getChartIdEntity() {
    return chartIdEntity;
  }

  public static ChartUrn createFromString(String rawUrn) throws URISyntaxException {
    Urn urn = new Urn(rawUrn);
    validateUrn(urn, ENTITY_TYPE);
    String[] urnParts = urn.getContent().split(",");
    return new ChartUrn(urnParts[0], urnParts[1]);
  }

  public static ChartUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(new DirectCoercer<ChartUrn>() {
      public Object coerceInput(ChartUrn object) throws ClassCastException {
        return object.toString();
      }

      public ChartUrn coerceOutput(Object object) throws TemplateOutputCastException {
        try {
          return ChartUrn.createFromString((String) object);
        } catch (URISyntaxException e) {
          throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
        }
      }
    }, ChartUrn.class);
  }
}
