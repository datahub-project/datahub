package com.linkedin.datahub.upgrade.commonsteps;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.function.Function;


public class GMSDisableWriteModeStep implements UpgradeStep {

  GMSDisableWriteModeStep() { }

  @Override
  public String id() {
    return "GMSEnableWriteModeStep";
  }

  @Override
  public int retryCount() {
    return 2;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      String gmsHost = System.getenv("DATAHUB_GMS_HOST") == null ? "localhost" : System.getenv("DATAHUB_GMS_HOST");
      String gmsPort = System.getenv("DATAHUB_GMS_PORT") == null ? "8080" : System.getenv("DATAHUB_GMS_PORT");
      try {
        String spec = String.format("http://%s:%s/entities?action=disableWrite", gmsHost, gmsPort);

        HttpURLConnection gmsConnection =  (HttpURLConnection) new URL(spec).openConnection();
        gmsConnection.setRequestMethod("POST");
        gmsConnection.connect();

        if (gmsConnection.getResponseCode() != 200) {
          System.out.printf("Failed to turn write mode off in GMS, received %i", gmsConnection.getResponseCode());
        } else {
          System.out.printf("Disabled write mode in GMS");
        }
      } catch (Exception e) {
        e.printStackTrace();
        System.out.printf("Failed to turn write mode off in GMS");
      }
      return new DefaultUpgradeStepResult(
          id(),
          UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
