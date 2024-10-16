package com.linkedin.datahub.upgrade.common.steps;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class GMSQualificationStep implements UpgradeStep {

  private final Map<String, String> configToMatch;

  private static String convertStreamToString(InputStream is) {

    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    StringBuilder sb = new StringBuilder();

    String line = null;
    try {
      while ((line = reader.readLine()) != null) {
        sb.append(line + "\n");
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        is.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return sb.toString();
  }

  @Override
  public String id() {
    return "GMSQualificationStep";
  }

  @Override
  public int retryCount() {
    return 2;
  }

  private boolean isEligible(ObjectNode configJson) {
    for (String key : configToMatch.keySet()) {
      if (!configJson.has(key) || !configJson.get(key).asText().equals(configToMatch.get(key))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      String gmsHost =
          System.getenv("DATAHUB_GMS_HOST") == null
              ? "localhost"
              : System.getenv("DATAHUB_GMS_HOST");
      String gmsPort =
          System.getenv("DATAHUB_GMS_PORT") == null ? "8080" : System.getenv("DATAHUB_GMS_PORT");
      String gmsProtocol =
          System.getenv("DATAHUB_GMS_PROTOCOL") == null
              ? "http"
              : System.getenv("DATAHUB_GMS_PROTOCOL");
      try {
        String spec = String.format("%s://%s:%s/config", gmsProtocol, gmsHost, gmsPort);

        URLConnection gmsConnection = new URL(spec).openConnection();
        InputStream response = gmsConnection.getInputStream();
        String responseString = convertStreamToString(response);

        ObjectMapper mapper = new ObjectMapper();
        int maxSize =
            Integer.parseInt(
                System.getenv()
                    .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
        mapper
            .getFactory()
            .setStreamReadConstraints(
                StreamReadConstraints.builder().maxStringLength(maxSize).build());
        JsonNode configJson = mapper.readTree(responseString);
        if (isEligible((ObjectNode) configJson)) {
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
        } else {
          context
              .report()
              .addLine(
                  String.format(
                      "Failed to qualify GMS. It is not running on the latest version."
                          + "Re-run GMS on the latest datahub release"));
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }
      } catch (Exception e) {
        e.printStackTrace();
        context
            .report()
            .addLine(
                String.format(
                    "ERROR: Cannot connect to GMS"
                        + "at %s://host %s port %s. Make sure GMS is on the latest version "
                        + "and is running at that host before starting the migration.",
                    gmsProtocol, gmsHost, gmsPort));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }
}
