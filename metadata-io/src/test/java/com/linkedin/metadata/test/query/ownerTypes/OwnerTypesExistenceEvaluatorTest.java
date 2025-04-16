package com.linkedin.metadata.test.query.ownerTypes;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.test.definition.ValidationResult;
import com.linkedin.metadata.test.query.TestQuery;
import com.linkedin.metadata.test.query.TestQueryResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OwnerTypesExistenceEvaluatorTest {
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,TestDataset,PROD)");
  private static final Urn USER_URN = UrnUtils.getUrn("urn:li:corpuser:datahub");

  private static final String TECHNICAL_OWNER_TYPE =
      "urn:li:ownershipType:__system__technical_owner";
  private static final String BUSINESS_OWNER_TYPE = "urn:li:ownershipType:__system__business_owner";

  private static final String OWNER_TYPE_EXISTS_QUERY = "ownerTypesList";

  private OwnerTypesExistenceEvaluator evaluator;
  private EntityService<?> entityService;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    entityService = mock(EntityService.class);
    evaluator = new OwnerTypesExistenceEvaluator(entityService);
    opContext = mock(OperationContext.class);
  }

  @Test
  public void testIsEligibleTrueForOwnerTypesExistenceQuery() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQuery()).thenReturn(OWNER_TYPE_EXISTS_QUERY);
    when(query.getQueryParts()).thenReturn(ImmutableList.of(OWNER_TYPE_EXISTS_QUERY));

    assertTrue(evaluator.isEligible("dataset", query));
  }

  @Test
  public void testIsEligibleFalseForNonOwnerTypesExistenceQuery() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQuery()).thenReturn("someOtherQuery");
    when(query.getQueryParts()).thenReturn(ImmutableList.of("someOtherQuery"));

    assertFalse(evaluator.isEligible("dataset", query));
  }

  @Test
  public void testIsEligibleFalseForEmptyQueryParts() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQuery()).thenReturn(OWNER_TYPE_EXISTS_QUERY);
    when(query.getQueryParts()).thenReturn(Collections.emptyList());

    assertFalse(evaluator.isEligible("dataset", query));
  }

  @Test
  public void testValidateQueryValid() {
    TestQuery query = mock(TestQuery.class);
    when(query.getQuery()).thenReturn(OWNER_TYPE_EXISTS_QUERY);
    when(query.getQueryParts()).thenReturn(ImmutableList.of(OWNER_TYPE_EXISTS_QUERY));

    ValidationResult result = evaluator.validateQuery("dataset", query);
    assertTrue(result.isValid());
  }

  @Test
  public void testEvaluateWithOwnerTypesMap() throws URISyntaxException {
    Set<Urn> urns = new HashSet<>(Collections.singletonList(TEST_DATASET_URN));
    TestQuery query = mock(TestQuery.class);
    when(query.getQuery()).thenReturn(OWNER_TYPE_EXISTS_QUERY);
    when(query.getQueryParts()).thenReturn(ImmutableList.of(OWNER_TYPE_EXISTS_QUERY));
    Set<TestQuery> queries = new HashSet<>(Collections.singletonList(query));

    // Create ownership with ownerTypes map
    Ownership ownership = new Ownership();
    ownership.setOwners(new OwnerArray());
    UrnArrayMap ownerTypesMap = new UrnArrayMap();
    UrnArray userArray = new UrnArray();
    userArray.add(USER_URN);
    ownerTypesMap.put(TECHNICAL_OWNER_TYPE, userArray);
    ownerTypesMap.put(BUSINESS_OWNER_TYPE, userArray);
    ownership.setOwnerTypes(ownerTypesMap);

    Map<Urn, EntityResponse> mockResponses = new HashMap<>();
    mockResponses.put(
        TEST_DATASET_URN,
        new EntityResponse()
            .setUrn(TEST_DATASET_URN)
            .setEntityName(DATASET_ENTITY_NAME)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        OWNERSHIP_ASPECT_NAME,
                        new EnvelopedAspect().setValue(new Aspect(ownership.data()))))));

    when(entityService.getEntitiesV2(
            eq(opContext),
            eq(DATASET_ENTITY_NAME),
            eq(Collections.singleton(TEST_DATASET_URN)),
            eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(mockResponses);

    Map<Urn, Map<TestQuery, TestQueryResponse>> results =
        evaluator.evaluate(opContext, DATASET_ENTITY_NAME, urns, queries);

    TestQueryResponse response = results.get(TEST_DATASET_URN).get(query);
    assertEquals(response.getValues().size(), 2);
    assertTrue(response.getValues().contains(TECHNICAL_OWNER_TYPE));
    assertTrue(response.getValues().contains(BUSINESS_OWNER_TYPE));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testEvaluateWithNestedOwners() throws URISyntaxException {
    Set<Urn> urns = new HashSet<>(Collections.singletonList(TEST_DATASET_URN));
    TestQuery query = mock(TestQuery.class);
    when(query.getQuery()).thenReturn(OWNER_TYPE_EXISTS_QUERY);
    when(query.getQueryParts()).thenReturn(ImmutableList.of(OWNER_TYPE_EXISTS_QUERY));
    Set<TestQuery> queries = new HashSet<>(Collections.singletonList(query));

    // Create ownership with nested owners array
    Ownership ownership = new Ownership();
    OwnerArray ownerArray = new OwnerArray();

    // Owner with typeUrn
    Owner owner1 = new Owner();
    owner1.setType(OwnershipType.BUSINESS_OWNER);
    owner1.setTypeUrn(UrnUtils.getUrn(BUSINESS_OWNER_TYPE));

    // Owner with just type (no typeUrn)
    Owner owner2 = new Owner();
    owner2.setType(OwnershipType.TECHNICAL_OWNER);

    ownerArray.add(owner1);
    ownerArray.add(owner2);
    ownership.setOwners(ownerArray);

    Map<Urn, EntityResponse> mockResponses = new HashMap<>();
    mockResponses.put(
        TEST_DATASET_URN,
        new EntityResponse()
            .setUrn(TEST_DATASET_URN)
            .setEntityName(DATASET_ENTITY_NAME)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        OWNERSHIP_ASPECT_NAME,
                        new EnvelopedAspect().setValue(new Aspect(ownership.data()))))));

    when(entityService.getEntitiesV2(
            eq(opContext),
            eq(DATASET_ENTITY_NAME),
            eq(Collections.singleton(TEST_DATASET_URN)),
            eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(mockResponses);

    Map<Urn, Map<TestQuery, TestQueryResponse>> results =
        evaluator.evaluate(opContext, DATASET_ENTITY_NAME, urns, queries);

    TestQueryResponse response = results.get(TEST_DATASET_URN).get(query);
    assertEquals(response.getValues().size(), 2);
    assertTrue(response.getValues().contains(BUSINESS_OWNER_TYPE));
    assertTrue(response.getValues().contains(TECHNICAL_OWNER_TYPE));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testEvaluateWithBothOwnerTypesAndNestedOwners() throws URISyntaxException {
    Set<Urn> urns = new HashSet<>(Collections.singletonList(TEST_DATASET_URN));
    TestQuery query = mock(TestQuery.class);
    when(query.getQuery()).thenReturn(OWNER_TYPE_EXISTS_QUERY);
    when(query.getQueryParts()).thenReturn(ImmutableList.of(OWNER_TYPE_EXISTS_QUERY));
    Set<TestQuery> queries = new HashSet<>(Collections.singletonList(query));

    // Create ownership with both ownerTypes map and nested owners array
    Ownership ownership = new Ownership();

    // Add ownerTypes map
    UrnArrayMap ownerTypesMap = new UrnArrayMap();
    UrnArray userArray = new UrnArray();
    userArray.add(USER_URN);
    ownerTypesMap.put(TECHNICAL_OWNER_TYPE, userArray);
    ownership.setOwnerTypes(ownerTypesMap);

    // Add nested owners
    OwnerArray ownerArray = new OwnerArray();
    Owner owner = new Owner();
    owner.setType(OwnershipType.BUSINESS_OWNER);
    owner.setTypeUrn(UrnUtils.getUrn(BUSINESS_OWNER_TYPE));
    ownerArray.add(owner);
    ownership.setOwners(ownerArray);

    Map<Urn, EntityResponse> mockResponses = new HashMap<>();
    mockResponses.put(
        TEST_DATASET_URN,
        new EntityResponse()
            .setUrn(TEST_DATASET_URN)
            .setEntityName(DATASET_ENTITY_NAME)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        OWNERSHIP_ASPECT_NAME,
                        new EnvelopedAspect().setValue(new Aspect(ownership.data()))))));

    when(entityService.getEntitiesV2(
            eq(opContext),
            eq(DATASET_ENTITY_NAME),
            eq(Collections.singleton(TEST_DATASET_URN)),
            eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(mockResponses);

    Map<Urn, Map<TestQuery, TestQueryResponse>> results =
        evaluator.evaluate(opContext, DATASET_ENTITY_NAME, urns, queries);

    TestQueryResponse response = results.get(TEST_DATASET_URN).get(query);
    assertEquals(response.getValues().size(), 2);
    assertTrue(response.getValues().contains(TECHNICAL_OWNER_TYPE));
    assertTrue(response.getValues().contains(BUSINESS_OWNER_TYPE));
  }

  @Test
  public void testEvaluateWithNoOwnership() throws URISyntaxException {
    Set<Urn> urns = new HashSet<>(Collections.singletonList(TEST_DATASET_URN));
    TestQuery query = mock(TestQuery.class);
    when(query.getQuery()).thenReturn(OWNER_TYPE_EXISTS_QUERY);
    when(query.getQueryParts()).thenReturn(ImmutableList.of(OWNER_TYPE_EXISTS_QUERY));
    Set<TestQuery> queries = new HashSet<>(Collections.singletonList(query));

    // Create entity response with no ownership aspect
    Map<Urn, EntityResponse> mockResponses = new HashMap<>();
    mockResponses.put(
        TEST_DATASET_URN,
        new EntityResponse()
            .setUrn(TEST_DATASET_URN)
            .setEntityName(DATASET_ENTITY_NAME)
            .setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    when(entityService.getEntitiesV2(
            eq(opContext),
            eq(DATASET_ENTITY_NAME),
            eq(Collections.singleton(TEST_DATASET_URN)),
            eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(mockResponses);

    Map<Urn, Map<TestQuery, TestQueryResponse>> results =
        evaluator.evaluate(opContext, DATASET_ENTITY_NAME, urns, queries);

    TestQueryResponse response = results.get(TEST_DATASET_URN).get(query);
    assertTrue(response.getValues().isEmpty());
  }

  @Test
  public void testEvaluateWithNullOwnership() throws URISyntaxException {
    Set<Urn> urns = new HashSet<>(Collections.singletonList(TEST_DATASET_URN));
    TestQuery query = mock(TestQuery.class);
    when(query.getQuery()).thenReturn(OWNER_TYPE_EXISTS_QUERY);
    when(query.getQueryParts()).thenReturn(ImmutableList.of(OWNER_TYPE_EXISTS_QUERY));
    Set<TestQuery> queries = new HashSet<>(Collections.singletonList(query));

    // Create entity response with null ownership aspect
    Map<Urn, EntityResponse> mockResponses = new HashMap<>();
    mockResponses.put(
        TEST_DATASET_URN,
        new EntityResponse()
            .setUrn(TEST_DATASET_URN)
            .setEntityName(DATASET_ENTITY_NAME)
            .setAspects(new EnvelopedAspectMap()));

    when(entityService.getEntitiesV2(
            eq(opContext),
            eq(DATASET_ENTITY_NAME),
            eq(Collections.singleton(TEST_DATASET_URN)),
            eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(mockResponses);

    Map<Urn, Map<TestQuery, TestQueryResponse>> results =
        evaluator.evaluate(opContext, DATASET_ENTITY_NAME, urns, queries);

    TestQueryResponse response = results.get(TEST_DATASET_URN).get(query);
    assertTrue(response.getValues().isEmpty());
  }

  @Test
  public void testEvaluateWithEmptyOwnership() throws URISyntaxException {
    Set<Urn> urns = new HashSet<>(Collections.singletonList(TEST_DATASET_URN));
    TestQuery query = mock(TestQuery.class);
    when(query.getQuery()).thenReturn(OWNER_TYPE_EXISTS_QUERY);
    when(query.getQueryParts()).thenReturn(ImmutableList.of(OWNER_TYPE_EXISTS_QUERY));
    Set<TestQuery> queries = new HashSet<>(Collections.singletonList(query));

    // Create ownership with no data
    Ownership ownership = new Ownership();
    ownership.setOwners(new OwnerArray());

    Map<Urn, EntityResponse> mockResponses = new HashMap<>();
    mockResponses.put(
        TEST_DATASET_URN,
        new EntityResponse()
            .setUrn(TEST_DATASET_URN)
            .setEntityName(DATASET_ENTITY_NAME)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        OWNERSHIP_ASPECT_NAME,
                        new EnvelopedAspect().setValue(new Aspect(ownership.data()))))));

    when(entityService.getEntitiesV2(
            eq(opContext),
            eq(DATASET_ENTITY_NAME),
            eq(Collections.singleton(TEST_DATASET_URN)),
            eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(mockResponses);

    Map<Urn, Map<TestQuery, TestQueryResponse>> results =
        evaluator.evaluate(opContext, DATASET_ENTITY_NAME, urns, queries);

    TestQueryResponse response = results.get(TEST_DATASET_URN).get(query);
    assertTrue(response.getValues().isEmpty());
  }
}
