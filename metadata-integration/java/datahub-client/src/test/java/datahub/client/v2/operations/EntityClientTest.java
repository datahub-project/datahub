package datahub.client.v2.operations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.config.DataHubClientConfigV2;
import datahub.client.v2.entity.Container;
import datahub.event.MetadataChangeProposalWrapper;
import java.util.concurrent.Future;
import org.junit.Test;
import org.mockito.InOrder;

public class EntityClientTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testUpsertWritesDataPlatformInstanceFirstAndWaitsForIt() throws Exception {
    RestEmitter emitter = mock(RestEmitter.class);
    Future<MetadataWriteResponse> dpiFuture = mock(Future.class);
    Future<MetadataWriteResponse> otherFuture = mock(Future.class);
    MetadataWriteResponse ok = MetadataWriteResponse.builder().success(true).build();
    when(dpiFuture.get()).thenReturn(ok);
    when(otherFuture.get()).thenReturn(ok);
    when(emitter.emit(any(MetadataChangeProposalWrapper.class)))
        .thenAnswer(
            inv ->
                isDpi(inv.getArgument(0, MetadataChangeProposalWrapper.class))
                    ? dpiFuture
                    : otherFuture);

    EntityClient client =
        new EntityClient(
            emitter,
            DataHubClientConfigV2.builder().server("http://localhost:8080").build(),
            mock(DataHubClientV2.class));
    Container container =
        Container.builder()
            .platform("snowflake")
            .platformInstance("my-instance")
            .displayName("My Database")
            .build();

    client.upsert(container);

    InOrder order = inOrder(emitter, dpiFuture);
    order.verify(emitter).emit(argThat(EntityClientTest::isDpi));
    order.verify(dpiFuture).get();
    order.verify(emitter).emit(argThat((MetadataChangeProposalWrapper mcp) -> !isDpi(mcp)));
  }

  private static boolean isDpi(MetadataChangeProposalWrapper mcp) {
    return "dataPlatformInstance".equals(mcp.getAspectName());
  }
}
