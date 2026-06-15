package com.linkedin.datahub.graphql.exception;

import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.entity.validation.ValidationException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class IngestProposalExceptionUtilsTest {

  @Test
  public void testMapsAuthorizationValidationTo403() {
    ValidationExceptionCollection collection = ValidationExceptionCollection.newCollection();
    collection.addAuthException(
        mockItem(), "Unauthorized to modify logicalParent on entity: urn:li:dataset:test");

    ValidationException validationException = new ValidationException(collection);
    RuntimeException graphQLException =
        IngestProposalExceptionUtils.toGraphQLException(validationException);

    assertTrue(graphQLException instanceof AuthorizationException);
    assertEquals(
        ((AuthorizationException) graphQLException).errorCode(),
        DataHubGraphQLErrorCode.UNAUTHORIZED);
  }

  @Test
  public void testMapsNonAuthValidationTo400() {
    ValidationExceptionCollection collection = ValidationExceptionCollection.newCollection();
    collection.addException(mockItem(), "Invalid aspect payload");

    ValidationException validationException = new ValidationException(collection);
    RuntimeException graphQLException =
        IngestProposalExceptionUtils.toGraphQLException(validationException);

    assertTrue(
        graphQLException instanceof com.linkedin.datahub.graphql.exception.ValidationException);
    assertEquals(graphQLException.getMessage(), "Invalid aspect payload");
  }

  @Test
  public void testMapsWrappedValidationException() {
    ValidationExceptionCollection collection = ValidationExceptionCollection.newCollection();
    collection.addAuthException(mockItem(), "Unauthorized to modify dataProductProperties.assets");

    ValidationException validationException = new ValidationException(collection);
    RuntimeException graphQLException =
        IngestProposalExceptionUtils.toGraphQLException(
            "Failed to ingest proposal",
            new RuntimeException("ingest failed", validationException));

    assertTrue(graphQLException instanceof AuthorizationException);
  }

  private static BatchItem mockItem() {
    BatchItem item = Mockito.mock(BatchItem.class);
    when(item.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(item.getUrn())
        .thenReturn(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)"));
    when(item.getAspectName()).thenReturn("logicalParent");
    return item;
  }
}
