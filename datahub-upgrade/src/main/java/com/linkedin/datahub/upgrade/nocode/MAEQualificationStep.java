package com.linkedin.datahub.upgrade.nocode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.function.Function;


public class MAEQualificationStep implements UpgradeStep {

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

  MAEQualificationStep() { }

  @Override
  public String id() {
    return "MAEQualificationStep";
  }

  @Override
  public int retryCount() {
    return 2;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      String maeHost = System.getenv("DATAHUB_MAE_CONSUMER_HOST") == null ? "localhost" : System.getenv("DATAHUB_MAE_CONSUMER_HOST");
      String maePort = System.getenv("DATAHUB_MAE_CONSUMER_PORT") == null ? "9091" : System.getenv("DATAHUB_MAE_CONSUMER_PORT");
      try {
        String spec = String.format("http://%s:%s/config", maeHost, maePort);

        URLConnection gmsConnection = new URL(spec).openConnection();
        InputStream response = gmsConnection.getInputStream();
        String responseString = convertStreamToString(response);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode configJson = mapper.readTree(responseString);
        if (configJson.get("noCode").asBoolean()) {
          context.report().addLine("MAE Consumer is running and up to date. Proceeding with upgrade...");
          return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
        } else {
          context.report().addLine(String.format("Failed to qualify MAE Consumer. It is not running on the latest version."
              + "Re-run MAE Consumer on the latest datahub release"));
          return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
        }
      } catch (Exception e) {
        e.printStackTrace();
        context.report().addLine(String.format(
            "ERROR: Cannot connect to MAE Consumer"
                + "at host %s port %s. Make sure MAE Consumer is on the latest version "
                + "and is running at that host before starting the migration.",
            maeHost, maePort));
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
    };
  }
}
