package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.metadata.aspect.DashboardAspect;
import com.linkedin.metadata.dao.RestliRemoteDAO;
import com.linkedin.metadata.snapshot.DashboardSnapshot;
import com.linkedin.restli.client.Client;
import java.net.URISyntaxException;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class DashboardHydrator implements Hydrator {
  private final Client _restliClient;
  private final RestliRemoteDAO<DashboardSnapshot, DashboardAspect, DashboardUrn> _remoteDAO;

  private static final String DASHBOARD_TOOL = "dashboardTool";
  private static final String TITLE = "title";

  public DashboardHydrator(@Nonnull Client restliClient) {
    _restliClient = restliClient;
    _remoteDAO = new RestliRemoteDAO<>(DashboardSnapshot.class, DashboardAspect.class, _restliClient);
  }

  @Override
  public Optional<ObjectNode> getHydratedEntity(String urn) {
    DashboardUrn dashboardUrn;
    try {
      dashboardUrn = DashboardUrn.createFromString(urn);
    } catch (URISyntaxException e) {
      log.info("Invalid Dashboard URN: {}", urn);
      return Optional.empty();
    }

    ObjectNode jsonObject = HydratorFactory.OBJECT_MAPPER.createObjectNode();
    jsonObject.put(DASHBOARD_TOOL, dashboardUrn.getDashboardToolEntity());

    _remoteDAO.get(DashboardInfo.class, dashboardUrn)
        .ifPresent(dashboardInfo -> jsonObject.put(TITLE, dashboardInfo.getTitle()));

    return Optional.of(jsonObject);
  }
}
