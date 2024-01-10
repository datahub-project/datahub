package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.SEARCHABLE_ENTITY_TYPES;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.combineFilters;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.FormFilter;
import com.linkedin.datahub.graphql.generated.SearchForEntitiesByFormInput;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.form.FormActorAssignment;
import com.linkedin.form.FormInfo;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.FormService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SearchForEntitiesByFormResolverTest {

  private static final Urn TEST_FORM_URN = UrnUtils.getUrn("urn:li:form:1");
  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:admin");

  @Test
  public static void testSearchByFormSuccess() throws Exception {
    final Filter filter = generateCompleteOrIncompleteFilter();
    EntityClient mockClient = initMockEntityClient(filter, 0, 10);
    FormService mockFormService = initMockFormService(TEST_FORM_URN);

    final SearchForEntitiesByFormResolver resolver =
        new SearchForEntitiesByFormResolver(mockClient, mockFormService);
    // if they only provide formUrn in input, search for all entities with that form urn
    final FormFilter formFilter = new FormFilter();
    formFilter.setFormUrn(TEST_FORM_URN.toString());
    final SearchForEntitiesByFormInput testInput =
        new SearchForEntitiesByFormInput(null, 0, 10, null, null, formFilter);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();
    verifyMockEntityClient(mockClient, filter, 0, 10);
  }

  @Test
  public static void testSearchByFormCompleted() throws Exception {
    final Filter filter = generateFormCompletionFilter("completedForms");
    EntityClient mockClient = initMockEntityClient(filter, 0, 10);
    FormService mockFormService = initMockFormService(TEST_FORM_URN);

    final SearchForEntitiesByFormResolver resolver =
        new SearchForEntitiesByFormResolver(mockClient, mockFormService);
    final FormFilter formFilter = new FormFilter();
    formFilter.setFormUrn(TEST_FORM_URN.toString());
    formFilter.setIsFormComplete(true);
    final SearchForEntitiesByFormInput testInput =
        new SearchForEntitiesByFormInput(null, 0, 10, null, null, formFilter);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();
    verifyMockEntityClient(mockClient, filter, 0, 10);
  }

  @Test
  public static void testSearchByFormIncomplete() throws Exception {
    final Filter filter = generateFormCompletionFilter("incompleteForms");
    EntityClient mockClient = initMockEntityClient(filter, 0, 10);
    FormService mockFormService = initMockFormService(TEST_FORM_URN);

    final SearchForEntitiesByFormResolver resolver =
        new SearchForEntitiesByFormResolver(mockClient, mockFormService);
    final FormFilter formFilter = new FormFilter();
    formFilter.setFormUrn(TEST_FORM_URN.toString());
    formFilter.setIsFormComplete(false);
    final SearchForEntitiesByFormInput testInput =
        new SearchForEntitiesByFormInput(null, 0, 10, null, null, formFilter);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();
    verifyMockEntityClient(mockClient, filter, 0, 10);
  }

  @Test
  public static void testSearchByFormByAssignedActor() throws Exception {
    final Filter filter = generateFormCompletionFilter("incompleteForms");
    final Filter assignedActorFilter = generateAssignedActorFilter();
    final Filter finalFilter = combineFilters(assignedActorFilter, filter);

    EntityClient mockClient = initMockEntityClient(finalFilter, 0, 10);
    FormService mockFormService = initMockFormService(TEST_FORM_URN);

    final SearchForEntitiesByFormResolver resolver =
        new SearchForEntitiesByFormResolver(mockClient, mockFormService);
    final FormFilter formFilter = new FormFilter();
    formFilter.setFormUrn(TEST_FORM_URN.toString());
    formFilter.setIsFormComplete(false);
    formFilter.setAssignedActor(TEST_ACTOR_URN.toString());
    final SearchForEntitiesByFormInput testInput =
        new SearchForEntitiesByFormInput(null, 0, 10, null, null, formFilter);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();
    verifyMockEntityClient(mockClient, finalFilter, 0, 10);
  }

  @Test
  public static void testSearchThrowsError() throws Exception {
    final Filter filter = generateCompleteOrIncompleteFilter();
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.searchAcrossEntities(
                Mockito.eq(
                    SEARCHABLE_ENTITY_TYPES.stream()
                        .map(EntityTypeMapper::getName)
                        .collect(Collectors.toList())),
                Mockito.eq("*"),
                Mockito.eq(filter),
                Mockito.eq(0),
                Mockito.eq(10),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.any(Authentication.class)))
        .thenThrow(new RemoteInvocationException());
    FormService mockFormService = initMockFormService(TEST_FORM_URN);

    final SearchForEntitiesByFormResolver resolver =
        new SearchForEntitiesByFormResolver(mockClient, mockFormService);
    final FormFilter formFilter = new FormFilter();
    formFilter.setFormUrn(TEST_FORM_URN.toString());
    final SearchForEntitiesByFormInput testInput =
        new SearchForEntitiesByFormInput(null, 0, 10, null, null, formFilter);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assert.assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private static Filter generateCompleteOrIncompleteFilter() {
    final CriterionArray completedFormsAndArray =
        new CriterionArray(
            new Criterion()
                .setField("completedForms")
                .setValue(TEST_FORM_URN.toString())
                .setCondition(Condition.EQUAL)
                .setNegated(false));
    final CriterionArray incompleteFormsAndArray =
        new CriterionArray(
            new Criterion()
                .setField("incompleteForms")
                .setValue(TEST_FORM_URN.toString())
                .setCondition(Condition.EQUAL)
                .setNegated(false));
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(completedFormsAndArray),
                new ConjunctiveCriterion().setAnd(incompleteFormsAndArray)));
  }

  private static Filter generateFormCompletionFilter(final String field) {
    final CriterionArray criterionArray =
        new CriterionArray(
            new Criterion()
                .setField(field)
                .setValue(TEST_FORM_URN.toString())
                .setCondition(Condition.EQUAL)
                .setNegated(false));
    return new Filter()
        .setOr(new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(criterionArray)));
  }

  private static Filter generateAssignedActorFilter() {
    final CriterionArray criterionArray =
        new CriterionArray(
            new Criterion()
                .setField("owners")
                .setValue(TEST_ACTOR_URN.toString())
                .setCondition(Condition.EQUAL));
    return new Filter()
        .setOr(new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(criterionArray)));
  }

  private static EntityClient initMockEntityClient(Filter filter, int start, int limit)
      throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    Mockito.when(
            client.searchAcrossEntities(
                Mockito.eq(
                    SEARCHABLE_ENTITY_TYPES.stream()
                        .map(EntityTypeMapper::getName)
                        .collect(Collectors.toList())),
                Mockito.eq("*"),
                Mockito.eq(filter),
                Mockito.eq(start),
                Mockito.eq(limit),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.any(Authentication.class)))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));
    return client;
  }

  private static FormService initMockFormService(Urn formUrn) throws Exception {

    FormService service = Mockito.mock(FormService.class);
    Mockito.when(service.getFormInfo(Mockito.eq(formUrn), Mockito.any(Authentication.class)))
        .thenReturn(new FormInfo().setActors(new FormActorAssignment().setOwners(true)));
    return service;
  }

  private static void verifyMockEntityClient(
      EntityClient mockClient, Filter filter, int start, int limit) throws Exception {
    Mockito.verify(mockClient, Mockito.times(1))
        .searchAcrossEntities(
            Mockito.eq(
                SEARCHABLE_ENTITY_TYPES.stream()
                    .map(EntityTypeMapper::getName)
                    .collect(Collectors.toList())),
            Mockito.eq("*"),
            Mockito.eq(filter),
            Mockito.eq(start),
            Mockito.eq(limit),
            Mockito.eq(null),
            Mockito.eq(null),
            Mockito.any(Authentication.class));
  }

  private SearchForEntitiesByFormResolverTest() {}
}
