package com.linkedin.datahub.upgrade.propagate;

import static com.linkedin.datahub.upgrade.propagate.PropagateTerms.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TermFetcherTest {
  private static final Urn ALLOWED_TERM_1 =
      UrnUtils.getUrn("urn:li:glossaryTerm:d206c230-93b9-4cff-8389-f459c03b32cb");
  private static final Urn ALLOWED_TERM_2 =
      UrnUtils.getUrn("urn:li:glossaryTerm:d206c230-93b9-4cff-8389-f459c03b32cc");
  private static final Urn NOT_ALLOWED_TERM_1 =
      UrnUtils.getUrn("urn:li:glossaryTerm:d206c230-93b9-4cff-8389-f459c03b32cd");
  private static final Urn NOT_ALLOWED_TERM_2 =
      UrnUtils.getUrn("urn:li:glossaryTerm:d206c230-93b9-4cff-8389-f459c03b32ce");
  private static final Urn ALLOWED_NODE_1 =
      UrnUtils.getUrn("urn:li:glossaryNode:d206c230-93b9-4cff-8389-f459c03b32cb");
  private static final Urn ALLOWED_NODE_2 =
      UrnUtils.getUrn("urn:li:glossaryNode:d206c230-93b9-4cff-8389-f459c03b32cc");
  private static final Urn NOT_ALLOWED_NODE_1 =
      UrnUtils.getUrn("urn:li:glossaryNode:d206c230-93b9-4cff-8389-f459c03b32cd");
  private static final Urn NOT_ALLOWED_NODE_2 =
      UrnUtils.getUrn("urn:li:glossaryNode:d206c230-93b9-4cff-8389-f459c03b32ce");
  private static final String SCROLL_ID = "test123";

  private OperationContext opContext;

  @BeforeTest
  public void setup() {
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testFetchAllowedTerms() throws URISyntaxException {
    final EntityService<?> entityService = mock(EntityService.class);
    final EntitySearchService entitySearchService = mock(EntitySearchService.class);
    configureEntityServiceMock(opContext, entityService);
    configureEntitySearchServiceMock(entitySearchService);
    Set<String> allowedGlossaryNodes = Set.of(ALLOWED_NODE_1.toString(), ALLOWED_NODE_2.toString());

    TermFetcher termFetcher =
        new TermFetcher(
            mock(OperationContext.class), entityService, entitySearchService, allowedGlossaryNodes);
    Set<Urn> fetchedAllowedNodes = termFetcher.fetchAllowedTerms(opContext);
    assertEquals(Set.of(ALLOWED_TERM_1, ALLOWED_TERM_2), fetchedAllowedNodes);
  }

  private static EntityResponse createEntityResponse(Urn entityUrn, Urn nodeUrn)
      throws URISyntaxException {
    return new EntityResponse()
        .setUrn(entityUrn)
        .setAspects(
            new EnvelopedAspectMap(
                Map.of(
                    Constants.GLOSSARY_TERM_INFO_ASPECT_NAME,
                    new EnvelopedAspect()
                        .setValue(
                            new Aspect(
                                new GlossaryTermInfo()
                                    .setParentNode(GlossaryNodeUrn.createFromUrn(nodeUrn))
                                    .data())))));
  }

  private static void configureEntityServiceMock(
      @Nonnull OperationContext opContext, final EntityService<?> mockEntityService)
      throws URISyntaxException {

    Mockito.when(
            mockEntityService.getEntitiesV2(
                opContext,
                Constants.GLOSSARY_TERM_ENTITY_NAME,
                Set.of(ALLOWED_TERM_1, NOT_ALLOWED_TERM_1),
                Set.of(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)))
        .thenReturn(
            Map.of(
                ALLOWED_TERM_1, createEntityResponse(ALLOWED_TERM_1, ALLOWED_NODE_1),
                NOT_ALLOWED_TERM_1, createEntityResponse(NOT_ALLOWED_TERM_1, NOT_ALLOWED_NODE_1)));
    Mockito.when(
            mockEntityService.getEntitiesV2(
                opContext,
                Constants.GLOSSARY_TERM_ENTITY_NAME,
                Set.of(ALLOWED_TERM_2, NOT_ALLOWED_TERM_2),
                Set.of(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)))
        .thenReturn(
            Map.of(
                ALLOWED_TERM_2, createEntityResponse(ALLOWED_TERM_2, ALLOWED_NODE_2),
                NOT_ALLOWED_TERM_2, createEntityResponse(NOT_ALLOWED_TERM_2, NOT_ALLOWED_NODE_2)));
  }

  private static void configureEntitySearchServiceMock(
      final EntitySearchService mockSearchService) {
    SearchEntity datasetSearchEntry = new SearchEntity();
    datasetSearchEntry.setEntity(ALLOWED_TERM_1);
    SearchEntity datasetSearchEntry2 = new SearchEntity();
    datasetSearchEntry2.setEntity(NOT_ALLOWED_TERM_1);
    SearchEntityArray datasetSearchArray = new SearchEntityArray();
    datasetSearchArray.add(datasetSearchEntry);
    datasetSearchArray.add(datasetSearchEntry2);
    ScrollResult scrollResult = new ScrollResult();
    scrollResult.setEntities(datasetSearchArray);
    scrollResult.setScrollId(SCROLL_ID);

    Mockito.when(
            mockSearchService.scroll(
                Mockito.any(OperationContext.class),
                Mockito.eq(Collections.singletonList(Constants.GLOSSARY_TERM_ENTITY_NAME)),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(1000),
                Mockito.eq(null),
                Mockito.eq(ELASTIC_TIMEOUT),
                Mockito.eq(null)))
        .thenReturn(scrollResult);

    SearchEntity datasetSearchEntry3 = new SearchEntity();
    datasetSearchEntry3.setEntity(NOT_ALLOWED_TERM_2);
    SearchEntity datasetSearchEntry4 = new SearchEntity();
    datasetSearchEntry4.setEntity(ALLOWED_TERM_2);
    SearchEntityArray datasetSearchArray2 = new SearchEntityArray();
    datasetSearchArray2.add(datasetSearchEntry3);
    datasetSearchArray2.add(datasetSearchEntry4);
    ScrollResult scrollResult2 = new ScrollResult();
    scrollResult2.setEntities(datasetSearchArray2);
    // Null scroll ID

    Mockito.when(
            mockSearchService.scroll(
                Mockito.any(OperationContext.class),
                Mockito.eq(Collections.singletonList(Constants.GLOSSARY_TERM_ENTITY_NAME)),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(1000),
                Mockito.eq(SCROLL_ID),
                Mockito.eq(ELASTIC_TIMEOUT),
                Mockito.eq(null)))
        .thenReturn(scrollResult2);
  }
}
