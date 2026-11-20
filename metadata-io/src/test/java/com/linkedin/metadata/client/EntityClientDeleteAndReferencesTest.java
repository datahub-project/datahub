package com.linkedin.metadata.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** {@link EntityClient#deleteEntityThenReferences}'s own implementation. */
public class EntityClientDeleteAndReferencesTest {

  private static final Urn URN = UrnUtils.getUrn("urn:li:tag:deleteAndReferences");

  private EntityClient entityClient;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    entityClient =
        mock(EntityClient.class, withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS));
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  /** The entity is deleted before returning; the references only when the caller runs them. */
  @Test
  public void testTheEntityIsDeletedAndTheReferencesWaitForTheCaller() throws Exception {
    EntityClient.ReferencesCleanup references =
        entityClient.deleteEntityThenReferences(opContext, URN);

    verify(entityClient).deleteEntity(opContext, URN);
    verify(entityClient, never()).deleteEntityReferences(any(), any());

    references.run();

    InOrder inOrder = inOrder(entityClient);
    inOrder.verify(entityClient).deleteEntity(opContext, URN);
    inOrder.verify(entityClient).deleteEntityReferences(opContext, URN);
  }

  /** A reference failure reaches the caller as {@link EntityClient#deleteEntityReferences}'s. */
  @Test
  public void testAReferenceFailureIsThrownByTheCleanup() throws Exception {
    RemoteInvocationException failure = new RemoteInvocationException("references");
    doThrow(failure).when(entityClient).deleteEntityReferences(opContext, URN);

    EntityClient.ReferencesCleanup references =
        entityClient.deleteEntityThenReferences(opContext, URN);

    assertSame(expectThrows(RemoteInvocationException.class, references::run), failure);
  }

  /** A failed entity delete is thrown; the references are left alone. */
  @Test
  public void testAFailedDeleteIsThrownAndTheReferencesAreLeftAlone() throws Exception {
    doThrow(new IllegalStateException("delete")).when(entityClient).deleteEntity(opContext, URN);

    assertThrows(
        IllegalStateException.class, () -> entityClient.deleteEntityThenReferences(opContext, URN));

    verify(entityClient, never()).deleteEntityReferences(any(), any());
  }
}
