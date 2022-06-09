package com.linkedin.datahub.graphql.resolvers.glossary;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTermUrnArray;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.RelatedTermsInput;
import com.linkedin.datahub.graphql.generated.TermRelationshipType;
import com.linkedin.glossary.GlossaryRelatedTerms;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class RemoveRelatedTermsResolverTest {

  private static final String TEST_ENTITY_URN = "urn:li:glossaryTerm:test-id-0";
  private static final String TEST_TERM_1_URN = "urn:li:glossaryTerm:test-id-1";
  private static final String TEST_TERM_2_URN = "urn:li:glossaryTerm:test-id-2";

  @Test
  public void testGetSuccessIsA() throws Exception {
    GlossaryTermUrn term1Urn = GlossaryTermUrn.createFromString(TEST_TERM_1_URN);
    GlossaryTermUrn term2Urn = GlossaryTermUrn.createFromString(TEST_TERM_2_URN);
    final GlossaryRelatedTerms relatedTerms = new GlossaryRelatedTerms();
    relatedTerms.setIsRelatedTerms(new GlossaryTermUrnArray(Arrays.asList(term1Urn, term2Urn)));
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.getAspect(
            Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
            Mockito.eq(Constants.GLOSSARY_RELATED_TERM_ASPECT_NAME),
            Mockito.eq(0L)))
        .thenReturn(relatedTerms);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);

    RemoveRelatedTermsResolver resolver = new RemoveRelatedTermsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    RelatedTermsInput input = new RelatedTermsInput(TEST_ENTITY_URN, ImmutableList.of(
        TEST_TERM_1_URN
    ), TermRelationshipType.isA);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
    Mockito.verify(mockService, Mockito.times(1)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(AuditStamp.class)
    );
    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_ENTITY_URN))
    );
  }

  @Test
  public void testGetSuccessHasA() throws Exception {
    GlossaryTermUrn term1Urn = GlossaryTermUrn.createFromString(TEST_TERM_1_URN);
    GlossaryTermUrn term2Urn = GlossaryTermUrn.createFromString(TEST_TERM_2_URN);
    final GlossaryRelatedTerms relatedTerms = new GlossaryRelatedTerms();
    relatedTerms.setHasRelatedTerms(new GlossaryTermUrnArray(Arrays.asList(term1Urn, term2Urn)));
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.getAspect(
            Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
            Mockito.eq(Constants.GLOSSARY_RELATED_TERM_ASPECT_NAME),
            Mockito.eq(0L)))
        .thenReturn(relatedTerms);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);

    RemoveRelatedTermsResolver resolver = new RemoveRelatedTermsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    RelatedTermsInput input = new RelatedTermsInput(TEST_ENTITY_URN, ImmutableList.of(
        TEST_TERM_1_URN
    ), TermRelationshipType.hasA);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
    Mockito.verify(mockService, Mockito.times(1)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(AuditStamp.class)
    );
    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_ENTITY_URN))
    );
  }

  @Test
  public void testFailAspectDoesNotExist() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.getAspect(
            Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
            Mockito.eq(Constants.GLOSSARY_RELATED_TERM_ASPECT_NAME),
            Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);

    RemoveRelatedTermsResolver resolver = new RemoveRelatedTermsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    RelatedTermsInput input = new RelatedTermsInput(TEST_ENTITY_URN, ImmutableList.of(
        TEST_TERM_1_URN
    ), TermRelationshipType.hasA);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());
    Mockito.verify(mockService, Mockito.times(0)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(AuditStamp.class)
    );
  }

  @Test
  public void testFailNoPermissions() throws Exception {
    GlossaryTermUrn term1Urn = GlossaryTermUrn.createFromString(TEST_TERM_1_URN);
    GlossaryTermUrn term2Urn = GlossaryTermUrn.createFromString(TEST_TERM_2_URN);
    final GlossaryRelatedTerms relatedTerms = new GlossaryRelatedTerms();
    relatedTerms.setIsRelatedTerms(new GlossaryTermUrnArray(Arrays.asList(term1Urn, term2Urn)));
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.getAspect(
            Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
            Mockito.eq(Constants.GLOSSARY_RELATED_TERM_ASPECT_NAME),
            Mockito.eq(0L)))
        .thenReturn(relatedTerms);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);

    RemoveRelatedTermsResolver resolver = new RemoveRelatedTermsResolver(mockService);

    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    RelatedTermsInput input = new RelatedTermsInput(TEST_ENTITY_URN, ImmutableList.of(
        TEST_TERM_1_URN
    ), TermRelationshipType.isA);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());
    Mockito.verify(mockService, Mockito.times(0)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(AuditStamp.class)
    );
    Mockito.verify(mockService, Mockito.times(0)).exists(
        Mockito.eq(Urn.createFromString(TEST_ENTITY_URN))
    );
  }
}
