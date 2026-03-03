package com.linkedin.metadata.aspect.consistency.check.assertion;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.assertion.SchemaAssertionInfo;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AssertionTypeMismatchCheckTest {

  @Mock private EntityService<?> mockEntityService;

  private EntityRegistry entityRegistry;
  private OperationContext opContext;
  private CheckContext checkContext;
  private AssertionTypeMismatchCheck check;

  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test123");
  private static final Urn TEST_ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    entityRegistry = TestOperationContexts.defaultEntityRegistry();
    opContext = TestOperationContexts.systemContextNoSearchAuthorization(entityRegistry);
    checkContext =
        CheckContext.builder().operationContext(opContext).entityService(mockEntityService).build();
    check = new AssertionTypeMismatchCheck(entityRegistry);
  }

  @Test
  public void testGetName() {
    assertEquals(check.getName(), "Assertion Type Mismatch");
  }

  @Test
  public void testGetDescription() {
    assertTrue(check.getDescription().contains("sub-property"));
  }

  @Test
  public void testGetId() {
    assertEquals(check.getId(), "assertion-type-mismatch");
  }

  @Test
  public void testValidFreshnessAssertion_NoIssue() {
    EntityResponse response =
        createAssertionWithTypeAndSubProperty(
            TEST_ASSERTION_URN, TEST_ENTITY_URN, AssertionType.FRESHNESS);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(TEST_ASSERTION_URN, response));

    assertTrue(issues.isEmpty());
  }

  @Test
  public void testValidDatasetAssertion_NoIssue() {
    EntityResponse response =
        createAssertionWithTypeAndSubProperty(
            TEST_ASSERTION_URN, TEST_ENTITY_URN, AssertionType.DATASET);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(TEST_ASSERTION_URN, response));

    assertTrue(issues.isEmpty());
  }

  @Test
  public void testValidVolumeAssertion_NoIssue() {
    EntityResponse response =
        createAssertionWithTypeAndSubProperty(
            TEST_ASSERTION_URN, TEST_ENTITY_URN, AssertionType.VOLUME);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(TEST_ASSERTION_URN, response));

    assertTrue(issues.isEmpty());
  }

  @Test
  public void testValidSqlAssertion_NoIssue() {
    EntityResponse response =
        createAssertionWithTypeAndSubProperty(
            TEST_ASSERTION_URN, TEST_ENTITY_URN, AssertionType.SQL);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(TEST_ASSERTION_URN, response));

    assertTrue(issues.isEmpty());
  }

  @Test
  public void testValidFieldAssertion_NoIssue() {
    EntityResponse response =
        createAssertionWithTypeAndSubProperty(
            TEST_ASSERTION_URN, TEST_ENTITY_URN, AssertionType.FIELD);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(TEST_ASSERTION_URN, response));

    assertTrue(issues.isEmpty());
  }

  @Test
  public void testValidSchemaAssertion_NoIssue() {
    EntityResponse response =
        createAssertionWithTypeAndSubProperty(
            TEST_ASSERTION_URN, TEST_ENTITY_URN, AssertionType.DATA_SCHEMA);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(TEST_ASSERTION_URN, response));

    assertTrue(issues.isEmpty());
  }

  @Test
  public void testValidCustomAssertion_NoIssue() {
    EntityResponse response =
        createAssertionWithTypeAndSubProperty(
            TEST_ASSERTION_URN, TEST_ENTITY_URN, AssertionType.CUSTOM);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(TEST_ASSERTION_URN, response));

    assertTrue(issues.isEmpty());
  }

  @Test
  public void testFreshnessTypeMissingSubProperty_ReturnsHardDeleteIssue() {
    EntityResponse response =
        createAssertionWithTypeMismatch(TEST_ASSERTION_URN, AssertionType.FRESHNESS);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(TEST_ASSERTION_URN, response));

    assertEquals(issues.size(), 1);
    ConsistencyIssue issue = issues.get(0);
    assertEquals(issue.getEntityUrn(), TEST_ASSERTION_URN);
    assertEquals(issue.getFixType(), ConsistencyFixType.HARD_DELETE);
    assertTrue(issue.getDescription().contains("FRESHNESS"));
    assertTrue(issue.getDescription().contains("freshnessAssertion"));
    assertNotNull(issue.getHardDeleteUrns());
    assertTrue(issue.getHardDeleteUrns().contains(TEST_ASSERTION_URN));
  }

  @Test
  public void testDatasetTypeMissingSubProperty_ReturnsHardDeleteIssue() {
    EntityResponse response =
        createAssertionWithTypeMismatch(TEST_ASSERTION_URN, AssertionType.DATASET);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(TEST_ASSERTION_URN, response));

    assertEquals(issues.size(), 1);
    ConsistencyIssue issue = issues.get(0);
    assertEquals(issue.getFixType(), ConsistencyFixType.HARD_DELETE);
    assertTrue(issue.getDescription().contains("DATASET"));
    assertTrue(issue.getDescription().contains("datasetAssertion"));
  }

  @Test
  public void testVolumeTypeMissingSubProperty_ReturnsHardDeleteIssue() {
    EntityResponse response =
        createAssertionWithTypeMismatch(TEST_ASSERTION_URN, AssertionType.VOLUME);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(TEST_ASSERTION_URN, response));

    assertEquals(issues.size(), 1);
    assertTrue(issues.get(0).getDescription().contains("volumeAssertion"));
  }

  @Test
  public void testSqlTypeMissingSubProperty_ReturnsHardDeleteIssue() {
    EntityResponse response =
        createAssertionWithTypeMismatch(TEST_ASSERTION_URN, AssertionType.SQL);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(TEST_ASSERTION_URN, response));

    assertEquals(issues.size(), 1);
    assertTrue(issues.get(0).getDescription().contains("sqlAssertion"));
  }

  @Test
  public void testFieldTypeMissingSubProperty_ReturnsHardDeleteIssue() {
    EntityResponse response =
        createAssertionWithTypeMismatch(TEST_ASSERTION_URN, AssertionType.FIELD);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(TEST_ASSERTION_URN, response));

    assertEquals(issues.size(), 1);
    assertTrue(issues.get(0).getDescription().contains("fieldAssertion"));
  }

  @Test
  public void testSchemaTypeMissingSubProperty_ReturnsHardDeleteIssue() {
    EntityResponse response =
        createAssertionWithTypeMismatch(TEST_ASSERTION_URN, AssertionType.DATA_SCHEMA);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(TEST_ASSERTION_URN, response));

    assertEquals(issues.size(), 1);
    assertTrue(issues.get(0).getDescription().contains("schemaAssertion"));
  }

  @Test
  public void testCustomTypeMissingSubProperty_ReturnsHardDeleteIssue() {
    EntityResponse response =
        createAssertionWithTypeMismatch(TEST_ASSERTION_URN, AssertionType.CUSTOM);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(TEST_ASSERTION_URN, response));

    assertEquals(issues.size(), 1);
    assertTrue(issues.get(0).getDescription().contains("customAssertion"));
  }

  @Test
  public void testAssertionWithNoType_NoIssue() {
    EntityResponse response = createAssertionWithoutType(TEST_ASSERTION_URN);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(TEST_ASSERTION_URN, response));

    assertTrue(issues.isEmpty());
  }

  @Test
  public void testMultipleAssertions_MixedResults() {
    Urn validUrn = UrnUtils.getUrn("urn:li:assertion:valid");
    Urn invalidUrn = UrnUtils.getUrn("urn:li:assertion:invalid");

    EntityResponse validResponse =
        createAssertionWithTypeAndSubProperty(validUrn, TEST_ENTITY_URN, AssertionType.FRESHNESS);
    EntityResponse invalidResponse =
        createAssertionWithTypeMismatch(invalidUrn, AssertionType.FRESHNESS);

    Map<Urn, EntityResponse> responses =
        Map.of(validUrn, validResponse, invalidUrn, invalidResponse);

    List<ConsistencyIssue> issues = check.check(checkContext, responses);

    assertEquals(issues.size(), 1);
    assertEquals(issues.get(0).getEntityUrn(), invalidUrn);
  }

  @Test
  public void testSkipsSoftDeletedAssertions() {
    EntityResponse response =
        createSoftDeletedAssertionWithTypeMismatch(TEST_ASSERTION_URN, AssertionType.FRESHNESS);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(TEST_ASSERTION_URN, response));

    assertTrue(issues.isEmpty());
  }

  private EntityResponse createAssertionWithTypeAndSubProperty(
      Urn assertionUrn, Urn entityUrn, AssertionType type) {
    EntityResponse response = new EntityResponse();
    response.setUrn(assertionUrn);
    response.setEntityName(ASSERTION_ENTITY_NAME);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(false);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);

    EnvelopedAspect infoAspect = new EnvelopedAspect();
    AssertionInfo assertionInfo = createValidAssertionInfo(entityUrn, type);
    infoAspect.setValue(new Aspect(assertionInfo.data()));
    aspects.put(ASSERTION_INFO_ASPECT_NAME, infoAspect);

    response.setAspects(aspects);
    return response;
  }

  private AssertionInfo createValidAssertionInfo(Urn entityUrn, AssertionType type) {
    AssertionInfo info = new AssertionInfo();
    info.setType(type);

    switch (type) {
      case FRESHNESS:
        FreshnessAssertionInfo freshnessInfo = new FreshnessAssertionInfo();
        freshnessInfo.setType(FreshnessAssertionType.DATASET_CHANGE);
        freshnessInfo.setEntity(entityUrn);
        info.setFreshnessAssertion(freshnessInfo);
        break;
      case DATASET:
        DatasetAssertionInfo datasetInfo = new DatasetAssertionInfo();
        datasetInfo.setDataset(entityUrn);
        datasetInfo.setScope(DatasetAssertionScope.DATASET_ROWS);
        info.setDatasetAssertion(datasetInfo);
        break;
      case VOLUME:
        VolumeAssertionInfo volumeInfo = new VolumeAssertionInfo();
        volumeInfo.setEntity(entityUrn);
        info.setVolumeAssertion(volumeInfo);
        break;
      case SQL:
        SqlAssertionInfo sqlInfo = new SqlAssertionInfo();
        sqlInfo.setEntity(entityUrn);
        sqlInfo.setStatement("SELECT 1");
        info.setSqlAssertion(sqlInfo);
        break;
      case FIELD:
        FieldAssertionInfo fieldInfo = new FieldAssertionInfo();
        fieldInfo.setEntity(entityUrn);
        info.setFieldAssertion(fieldInfo);
        break;
      case DATA_SCHEMA:
        SchemaAssertionInfo schemaInfo = new SchemaAssertionInfo();
        schemaInfo.setEntity(entityUrn);
        info.setSchemaAssertion(schemaInfo);
        break;
      case CUSTOM:
        CustomAssertionInfo customInfo = new CustomAssertionInfo();
        customInfo.setEntity(entityUrn);
        customInfo.setType("testCustomType");
        info.setCustomAssertion(customInfo);
        break;
      default:
        break;
    }

    return info;
  }

  private EntityResponse createAssertionWithTypeMismatch(Urn assertionUrn, AssertionType type) {
    EntityResponse response = new EntityResponse();
    response.setUrn(assertionUrn);
    response.setEntityName(ASSERTION_ENTITY_NAME);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(false);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);

    EnvelopedAspect infoAspect = new EnvelopedAspect();
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(type);
    infoAspect.setValue(new Aspect(assertionInfo.data()));
    aspects.put(ASSERTION_INFO_ASPECT_NAME, infoAspect);

    response.setAspects(aspects);
    return response;
  }

  private EntityResponse createAssertionWithoutType(Urn assertionUrn) {
    EntityResponse response = new EntityResponse();
    response.setUrn(assertionUrn);
    response.setEntityName(ASSERTION_ENTITY_NAME);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(false);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);

    EnvelopedAspect infoAspect = new EnvelopedAspect();
    AssertionInfo assertionInfo = new AssertionInfo();
    infoAspect.setValue(new Aspect(assertionInfo.data()));
    aspects.put(ASSERTION_INFO_ASPECT_NAME, infoAspect);

    response.setAspects(aspects);
    return response;
  }

  private EntityResponse createSoftDeletedAssertionWithTypeMismatch(
      Urn assertionUrn, AssertionType type) {
    EntityResponse response = createAssertionWithTypeMismatch(assertionUrn, type);
    Status softDeleted = new Status().setRemoved(true);
    response.getAspects().get(STATUS_ASPECT_NAME).setValue(new Aspect(softDeleted.data()));
    return response;
  }
}
