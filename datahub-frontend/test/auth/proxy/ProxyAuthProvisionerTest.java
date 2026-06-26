package auth.proxy;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class ProxyAuthProvisionerTest {

  @Mock private SystemEntityClient systemEntityClient;
  @Mock private OperationContext systemOperationContext;

  private ProxyAuthProvisioner provisioner;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    provisioner = new ProxyAuthProvisioner(systemEntityClient, systemOperationContext);
  }

  @Test
  public void testProvisionNewUser() throws Exception {
    CorpuserUrn urn = new CorpuserUrn("newuser");

    when(systemEntityClient.get(eq(systemOperationContext), eq(urn)))
        .thenThrow(new RemoteInvocationException("not found"));

    provisioner.provisionUser(urn, "New User");

    verify(systemEntityClient).update(eq(systemOperationContext), any(Entity.class));
    verify(systemEntityClient)
        .ingestProposal(eq(systemOperationContext), any(MetadataChangeProposal.class));
  }

  @Test
  public void testExistingUserWithAspectsOnlyActivates() throws Exception {
    CorpuserUrn urn = new CorpuserUrn("existinguser");

    CorpUserSnapshot snapshot = new CorpUserSnapshot();
    snapshot.setUrn(urn);
    CorpUserAspectArray aspects = new CorpUserAspectArray();
    aspects.add(CorpUserAspect.create(new com.linkedin.identity.CorpUserInfo().setActive(true)));
    aspects.add(CorpUserAspect.create(new com.linkedin.identity.CorpUserEditableInfo()));
    snapshot.setAspects(aspects);

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));

    when(systemEntityClient.get(eq(systemOperationContext), eq(urn))).thenReturn(entity);

    provisioner.provisionUser(urn, "Existing User");

    verify(systemEntityClient, never()).update(any(), any(Entity.class));
    verify(systemEntityClient)
        .ingestProposal(eq(systemOperationContext), any(MetadataChangeProposal.class));
  }

  @Test
  public void testProvisionFailureDoesNotThrow() throws Exception {
    CorpuserUrn urn = new CorpuserUrn("failuser");

    when(systemEntityClient.get(eq(systemOperationContext), eq(urn)))
        .thenThrow(new RemoteInvocationException("not found"));
    doThrow(new RemoteInvocationException("update failed"))
        .when(systemEntityClient)
        .update(any(), any(Entity.class));

    provisioner.provisionUser(urn, "Fail User");

    verify(systemEntityClient).update(eq(systemOperationContext), any(Entity.class));
    verify(systemEntityClient, never()).ingestProposal(any(), any(MetadataChangeProposal.class));
  }
}
