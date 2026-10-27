package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContextWithOperationContext;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

public class LabelUtilsTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)");
  private static final Urn TAG_URN = UrnUtils.getUrn("urn:li:tag:test-tag");

  @Test
  public void testIsAuthorizedToUpdateGlobalTags() {
    QueryContext context = getMockAllowContext();
    assertTrue(LabelUtils.isAuthorizedToUpdateTags(context, DATASET_URN, null, List.of(TAG_URN)));
    assertTrue(
        LabelUtils.isAuthorizedToUpdateTags(context, DATASET_URN, null, Collections.emptyList()));
  }

  @Test
  public void testIsAuthorizedToUpdateSchemaFieldTags() {
    QueryContext context = getMockAllowContext();
    assertTrue(
        LabelUtils.isAuthorizedToUpdateTags(context, DATASET_URN, "field1", List.of(TAG_URN)));
  }

  @Test
  public void testIsAuthorizedToUpdateTagsDenied() {
    QueryContext context = getMockDenyContextWithOperationContext();
    assertFalse(LabelUtils.isAuthorizedToUpdateTags(context, DATASET_URN, null, List.of(TAG_URN)));
    assertFalse(
        LabelUtils.isAuthorizedToUpdateTags(context, DATASET_URN, "field1", List.of(TAG_URN)));
  }
}
