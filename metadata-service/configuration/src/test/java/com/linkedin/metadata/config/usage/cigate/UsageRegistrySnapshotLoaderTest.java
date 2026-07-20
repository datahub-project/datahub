package com.linkedin.metadata.config.usage.cigate;

import com.linkedin.metadata.config.usage.cigate.graphql.GraphqlExemptionSnapshot;
import com.linkedin.metadata.config.usage.cigate.graphql.GraphqlUsageSurface;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UsageRegistrySnapshotLoaderTest {

  @Test
  public void testGraphqlExemptionSnapshotsMergeByKey() {
    GraphqlExemptionSnapshot oss =
        new GraphqlExemptionSnapshot(
            List.of(
                new GraphqlExemptionSnapshot.Exemption(
                    "legacyOp",
                    GraphqlUsageSurface.GraphqlSurfaceKind.CLIENT_OPERATION,
                    "grandfathered")));
    GraphqlExemptionSnapshot supplemental =
        new GraphqlExemptionSnapshot(
            List.of(
                new GraphqlExemptionSnapshot.Exemption(
                    "internalQuery",
                    GraphqlUsageSurface.GraphqlSurfaceKind.QUERY_ROOT_FIELD,
                    "internal")));
    GraphqlExemptionSnapshot merged = oss.merge(supplemental);
    Assert.assertEquals(merged.exemptions().size(), 2);
  }

  @Test
  public void testOpenApiExemptionSnapshotsMergeBySourceFile() {
    HandlerExemptionSnapshot oss =
        new HandlerExemptionSnapshot(
            List.of(new HandlerExemptionSnapshot.Exemption("oss/Demo.java", 10, "admin")));
    HandlerExemptionSnapshot supplemental =
        new HandlerExemptionSnapshot(
            List.of(new HandlerExemptionSnapshot.Exemption("extra/Handler.java", 5, "internal")));
    HandlerExemptionSnapshot merged = oss.merge(supplemental);
    Assert.assertEquals(merged.exemptions().size(), 2);
  }
}
