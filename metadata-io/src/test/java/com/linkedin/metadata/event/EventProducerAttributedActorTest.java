package com.linkedin.metadata.event;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.entity.ebean.batch.AttributedActorHeaders;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import org.testng.annotations.Test;

public class EventProducerAttributedActorTest {
  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)");
  private static final String ALICE = "urn:li:corpuser:alice";

  private final EventProducer producer = mock(EventProducer.class, CALLS_REAL_METHODS);

  private static OperationContext aliceContext() {
    return TestOperationContexts.userContextNoSearchAuthorization(UrnUtils.getUrn(ALICE));
  }

  private static String header(MetadataChangeProposal mcp) {
    return mcp.getHeaders().get(AttributedActorHeaders.ATTRIBUTED_ACTOR);
  }

  @Test
  public void userSessionStampsActor() {
    MetadataChangeProposal mcp = new MetadataChangeProposal();

    producer.produceMetadataChangeProposal(aliceContext(), DATASET, mcp);

    assertEquals(header(mcp), ALICE);
  }

  @Test
  public void overwritesClientSuppliedHeader() {
    MetadataChangeProposal mcp =
        new MetadataChangeProposal()
            .setHeaders(
                new StringMap(
                    Map.of(AttributedActorHeaders.ATTRIBUTED_ACTOR, "urn:li:corpuser:mallory")));

    producer.produceMetadataChangeProposal(aliceContext(), DATASET, mcp);

    assertEquals(header(mcp), ALICE);
  }

  @Test
  public void systemSessionDoesNotStamp() {
    MetadataChangeProposal mcp = new MetadataChangeProposal();

    producer.produceMetadataChangeProposal(
        TestOperationContexts.systemContextNoSearchAuthorization(), DATASET, mcp);

    assertFalse(
        mcp.hasHeaders() && mcp.getHeaders().containsKey(AttributedActorHeaders.ATTRIBUTED_ACTOR));
  }

  @Test
  public void systemSessionKeepsExistingHeader() {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    AttributedActorHeaders.stamp(mcp, ALICE);

    producer.produceMetadataChangeProposal(
        TestOperationContexts.systemContextNoSearchAuthorization(), DATASET, mcp);

    assertTrue(mcp.hasHeaders());
    assertEquals(header(mcp), ALICE);
  }
}
