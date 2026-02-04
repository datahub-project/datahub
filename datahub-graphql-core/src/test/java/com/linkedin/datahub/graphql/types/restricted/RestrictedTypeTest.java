package com.linkedin.datahub.graphql.types.restricted;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Restricted;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RestrictedTypeTest {

  private RestrictedType restrictedType;
  private QueryContext mockContext;

  @BeforeMethod
  public void setup() {
    restrictedType = new RestrictedType();
    mockContext = mock(QueryContext.class);
    OperationContext mockOperationContext = mock(OperationContext.class);
    when(mockContext.getOperationContext()).thenReturn(mockOperationContext);
  }

  @Test
  public void testType() {
    assertEquals(restrictedType.type(), EntityType.RESTRICTED);
  }

  @Test
  public void testObjectClass() {
    assertEquals(restrictedType.objectClass(), Restricted.class);
  }

  @Test
  public void testGetKeyProvider() {
    Entity mockEntity = mock(Entity.class);
    when(mockEntity.getUrn()).thenReturn("urn:li:restricted:test123");

    String result = restrictedType.getKeyProvider().apply(mockEntity);
    assertEquals(result, "urn:li:restricted:test123");
  }

  @Test
  public void testBatchLoad() throws Exception {
    List<String> urns =
        Arrays.asList("urn:li:restricted:encrypted1", "urn:li:restricted:encrypted2");

    List<DataFetcherResult<Restricted>> results = restrictedType.batchLoad(urns, mockContext);

    assertEquals(results.size(), 2);

    // First result
    DataFetcherResult<Restricted> result1 = results.get(0);
    assertNotNull(result1);
    assertNotNull(result1.getData());
    assertEquals(result1.getData().getUrn(), "urn:li:restricted:encrypted1");
    assertEquals(result1.getData().getType(), EntityType.RESTRICTED);

    // Second result
    DataFetcherResult<Restricted> result2 = results.get(1);
    assertNotNull(result2);
    assertNotNull(result2.getData());
    assertEquals(result2.getData().getUrn(), "urn:li:restricted:encrypted2");
    assertEquals(result2.getData().getType(), EntityType.RESTRICTED);
  }

  @Test
  public void testBatchLoadWithoutAuthorization() throws Exception {
    List<String> urns =
        Arrays.asList(
            "urn:li:restricted:test1", "urn:li:restricted:test2", "urn:li:restricted:test3");

    List<DataFetcherResult<Restricted>> results =
        restrictedType.batchLoadWithoutAuthorization(urns, mockContext);

    assertEquals(results.size(), 3);

    for (int i = 0; i < urns.size(); i++) {
      DataFetcherResult<Restricted> result = results.get(i);
      assertNotNull(result);
      assertNotNull(result.getData());
      assertEquals(result.getData().getUrn(), urns.get(i));
      assertEquals(result.getData().getType(), EntityType.RESTRICTED);
    }
  }

  @Test
  public void testBatchLoadEmptyList() throws Exception {
    List<String> urns = Arrays.asList();

    List<DataFetcherResult<Restricted>> results = restrictedType.batchLoad(urns, mockContext);

    assertEquals(results.size(), 0);
  }

  @Test
  public void testBatchLoadPreservesOriginalUrn() throws Exception {
    // Encrypted URN should be preserved, not re-encrypted
    String encryptedUrn = "urn:li:restricted:v2:abc123xyz789";
    List<String> urns = Arrays.asList(encryptedUrn);

    List<DataFetcherResult<Restricted>> results = restrictedType.batchLoad(urns, mockContext);

    assertEquals(results.size(), 1);
    assertEquals(results.get(0).getData().getUrn(), encryptedUrn);
  }
}
