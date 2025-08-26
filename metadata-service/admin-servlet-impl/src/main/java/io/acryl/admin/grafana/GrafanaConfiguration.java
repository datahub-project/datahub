package io.acryl.admin.grafana;

import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource(
    value = "classpath:/grafana-application.yml",
    factory = YamlPropertySourceFactory.class)
public class GrafanaConfiguration {
  public static final String GRAFANA_SERVLET_NAME = "grafanaServlet";

  @Value("${grafana.uri}")
  private String uri;

  @Value("${grafana.token}")
  private String token;

  @Value("${grafana.orgId}")
  private String orgId;

  @Value("${grafana.namespace}")
  private String namespace;

  @Value("${grafana.datasource}")
  private String datasource;

  @Value("${grafana.logging}")
  private String logging;

  @Value("${grafana.dashboards}")
  private String dashboards;

  @Bean("grafanaDashboards")
  public Map<String, String> grafanaDashboards() {
    Optional<Map<String, String>> parseOpt =
        Optional.ofNullable(
            new Gson().fromJson(dashboards, new TypeToken<Map<String, String>>() {}.getType()));
    return parseOpt.orElse(Map.of());
  }

  @Bean("grafanaAllow")
  public Set<String> grafanaAllow(@Qualifier("grafanaDashboards") Map<String, String> dashboards) {
    return ImmutableSet.<String>builder()
        .addAll(
            dashboards.values().stream()
                .map(url -> url.split("[?]", 2)[0])
                .collect(Collectors.toList()))
        .add("/public")
        .add("/api/ds")
        .add("/api/dashboards")
        .add("/api/prometheus")
        .add("/api/frontend-metrics")
        .add("/config")
        .add("/rules")
        .build();
  }

  /**
   * These are used to block off parts of the grafana ui
   *
   * @return list of blocked url paths
   */
  @Bean("grafanaDeny")
  public Set<String> grafanaDeny() {
    return ImmutableSet.<String>builder()
        .add("/public/build/AlertRuleListIndex")
        .add("/api/alert-notifiers")
        .add("/api/alertmanager")
        .add("/api/playlists")
        .build();
  }

  /**
   * These url paths redirect to the default dashboard if accessed
   *
   * @return set of url paths
   */
  @Bean("grafanaRedirectDefault")
  public Set<String> grafanaRedirectDefault() {
    return ImmutableSet.<String>builder().add("/alerting/list").build();
  }

  /**
   * These parameters are forced on every call with the given values. Currently, we enforce the org
   * id, namespace, and kiosk mode (escape is possible)
   *
   * @return list of parameters, multi-parameters are allow per http spec
   */
  @Bean("grafanaRequiredParameters")
  public List<Map.Entry<String, String[]>> grafanaRequiredParameters() {
    return List.of(
        Map.entry("orgId", new String[] {orgId}),
        Map.entry("var-namespace", new String[] {namespace}),
        Map.entry("kiosk", new String[] {""}),
        Map.entry("var-datasource", new String[] {datasource}));
  }

  /**
   * If a /default dashboard is designated it is used as a forced redirect in a few cases where we
   * need to protect the grafana endpoints. Otherwise, it is useful in case we want to define a per
   * instance/client default dashboard.
   *
   * @param grafanaDashboards the configured dashboards
   * @return configuration object
   */
  @Bean("grafanaConfig")
  public Config grafanaConfig(
      @Qualifier("grafanaDashboards") Map<String, String> grafanaDashboards) {
    return Config.builder()
        .grafanaUri(URI.create(uri))
        .grafanaToken(token)
        .orgId(orgId)
        .namespace(namespace)
        .defaultDashboard(grafanaDashboards.getOrDefault("/default", "/"))
        .logging(logging)
        .build();
  }

  @Builder
  @Getter
  public static class Config {
    private URI grafanaUri;
    private String grafanaToken;
    private String orgId;
    private String namespace;
    private String defaultDashboard;
    private String logging;
  }
}
