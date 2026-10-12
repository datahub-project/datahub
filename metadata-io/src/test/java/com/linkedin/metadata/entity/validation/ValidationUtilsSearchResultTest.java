package com.linkedin.metadata.entity.validation;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ValidationUtilsSearchResultTest {

  private static final Urn URN_A =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,a,PROD)");
  private static final Urn URN_B =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,b,PROD)");

  private EntityService<?> entityService;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    entityService = mock(EntityService.class);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void validateSearchResultDropsAllGhostHitsButKeepsNumEntities() {
    SearchResult input =
        new SearchResult()
            .setFrom(0)
            .setPageSize(10)
            .setNumEntities(2)
            .setMetadata(new SearchResultMetadata())
            .setEntities(
                new SearchEntityArray(
                    new SearchEntity().setEntity(URN_A), new SearchEntity().setEntity(URN_B)));

    when(entityService.exists(eq(opContext), anyCollection(), anyBoolean())).thenReturn(Set.of());

    SearchResult validated = ValidationUtils.validateSearchResult(opContext, input, entityService);

    assertEquals(validated.getNumEntities().intValue(), 2);
    assertTrue(validated.getEntities().isEmpty());
  }

  @Test
  public void validateSearchResultKeepsExistingHits() {
    SearchResult input =
        new SearchResult()
            .setFrom(0)
            .setPageSize(10)
            .setNumEntities(2)
            .setMetadata(new SearchResultMetadata())
            .setEntities(
                new SearchEntityArray(
                    new SearchEntity().setEntity(URN_A), new SearchEntity().setEntity(URN_B)));

    when(entityService.exists(eq(opContext), anyCollection(), anyBoolean()))
        .thenReturn(Set.of(URN_A));

    SearchResult validated = ValidationUtils.validateSearchResult(opContext, input, entityService);

    assertEquals(validated.getNumEntities().intValue(), 2);
    assertEquals(validated.getEntities().size(), 1);
    assertEquals(validated.getEntities().get(0).getEntity(), URN_A);
  }
}
