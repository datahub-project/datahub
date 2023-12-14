package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class ChartUrn extends Urn {

  public static final String ENTITY_TYPE = "chart";

  private final String _dashboardTool;
  private final String _chartId;

  public ChartUrn(String dashboardTool, String chartId) {
    super(ENTITY_TYPE, TupleKey.create(dashboardTool, chartId));
    this._dashboardTool = dashboardTool;
    this._chartId = chartId;
  }

  public String getDashboardToolEntity() {
    return _dashboardTool;
  }

  public String getChartIdEntity() {
    return _chartId;
  }

  public static ChartUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static ChartUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'chart'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 2) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new ChartUrn(
              (String) key.getAs(0, String.class), (String) key.getAs(1, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  public static ChartUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(
        new DirectCoercer<ChartUrn>() {
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
        },
        ChartUrn.class);
  }
}
