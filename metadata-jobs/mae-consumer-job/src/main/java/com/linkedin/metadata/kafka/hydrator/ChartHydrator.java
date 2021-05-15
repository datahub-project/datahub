package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.chart.ChartInfo;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.metadata.aspect.ChartAspect;
import com.linkedin.metadata.dao.RestliRemoteDAO;
import com.linkedin.metadata.snapshot.ChartSnapshot;
import com.linkedin.restli.client.Client;
import java.net.URISyntaxException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ChartHydrator implements Hydrator {
  private final Client _restliClient;
  private final RestliRemoteDAO<ChartSnapshot, ChartAspect, ChartUrn> _remoteDAO;

  private static final String DASHBOARD_TOOL = "dashboardTool";
  private static final String TITLE = "title";

  public ChartHydrator(Client restliClient) {
    _restliClient = restliClient;
    _remoteDAO = new RestliRemoteDAO<>(ChartSnapshot.class, ChartAspect.class, _restliClient);
  }

  @Override
  public Optional<ObjectNode> getHydratedEntity(String urn) {
    ChartUrn chartUrn;
    try {
      chartUrn = ChartUrn.createFromString(urn);
    } catch (URISyntaxException e) {
      log.info("Invalid Chart URN: {}", urn);
      return Optional.empty();
    }

    ObjectNode jsonObject = HydratorFactory.OBJECT_MAPPER.createObjectNode();
    jsonObject.put(DASHBOARD_TOOL, chartUrn.getDashboardToolEntity());

    _remoteDAO.get(ChartInfo.class, chartUrn).ifPresent(chartInfo -> jsonObject.put(TITLE, chartInfo.getTitle()));

    return Optional.of(jsonObject);
  }
}
