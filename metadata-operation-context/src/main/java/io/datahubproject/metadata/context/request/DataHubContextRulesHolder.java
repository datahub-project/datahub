package io.datahubproject.metadata.context.request;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Holds the active {@link DataHubContextParsePolicy} for the JVM. GMS sets this from {@code
 * application.yaml} at startup; defaults match historical hardcoded {@code skill}/{@code caller}
 * behavior until then.
 */
@ThreadSafe
public final class DataHubContextRulesHolder {

  private static volatile DataHubContextParsePolicy policy = DataHubContextParsePolicy.defaults();

  private DataHubContextRulesHolder() {}

  public static DataHubContextParsePolicy get() {
    return policy;
  }

  public static void setPolicy(DataHubContextParsePolicy newPolicy) {
    policy = newPolicy != null ? newPolicy : DataHubContextParsePolicy.defaults();
  }
}
