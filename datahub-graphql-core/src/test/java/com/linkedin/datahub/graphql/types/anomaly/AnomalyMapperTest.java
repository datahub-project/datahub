package com.linkedin.datahub.graphql.types.anomaly;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.anomaly.AnomalyInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.Anomaly;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import org.testng.annotations.Test;

public class AnomalyMapperTest {

  private static final Urn TEST_ANOMALY_URN;
  private static final Urn TEST_USER_URN;
  private static final Urn TEST_ASSERTION_URN;
  private static final Urn TEST_DATASET_URN;

  static {
    TEST_ANOMALY_URN = UrnUtils.getUrn("urn:li:anomaly:test");
    TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");
    TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
    TEST_DATASET_URN = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");
  }

  @Test
  public void testMapWithNullableFields() {
    // Setup
    AnomalyInfo info = mockAnomalyInfo();
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(TEST_ANOMALY_URN);
    entityResponse.setAspects(new EnvelopedAspectMap());
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(info.data()));
    entityResponse.getAspects().put(Constants.ANOMALY_INFO_ASPECT_NAME, envelopedAspect);
    info.setDescription("Test description");
    info.setSeverity(5);
    info.getSource().setSourceUrn(TEST_ASSERTION_URN);
    info.getReview().setMessage("Test source message");

    // Exercise
    Anomaly anomaly = AnomalyMapper.map(null, entityResponse);

    // Verify
    assertEquals(anomaly.getDescription(), info.getDescription());
    assertEquals((int) anomaly.getSeverity(), (int) info.getSeverity());
    assertEquals(anomaly.getSource().getSource().getUrn(), TEST_ASSERTION_URN.toString());
    assertEquals(anomaly.getReview().getMessage(), info.getReview().getMessage());
    verifyRequiredFields(anomaly, info);
  }

  @Test
  public void testMapWithoutNullableFields() {
    // Setup
    AnomalyInfo info = mockAnomalyInfo();
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(TEST_ANOMALY_URN);
    entityResponse.setAspects(new EnvelopedAspectMap());
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(info.data()));
    entityResponse.getAspects().put(Constants.ANOMALY_INFO_ASPECT_NAME, envelopedAspect);

    // Exercise
    Anomaly anomaly = AnomalyMapper.map(null, entityResponse);

    // Verify
    assertNull(anomaly.getDescription());
    assertNull(anomaly.getSeverity());
    assertNull(anomaly.getReview().getMessage());
    verifyRequiredFields(anomaly, info);
  }

  private AnomalyInfo mockAnomalyInfo() {
    AnomalyInfo info = new AnomalyInfo();
    info.setType(com.linkedin.anomaly.AnomalyType.DATASET_COLUMN);
    info.setEntity(TEST_DATASET_URN);
    info.setStatus(createStatus());
    info.setReview(createReview());
    info.setSource(createSource());
    info.setCreated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));
    return info;
  }

  private com.linkedin.anomaly.AnomalyStatus createStatus() {
    com.linkedin.anomaly.AnomalyStatus status = new com.linkedin.anomaly.AnomalyStatus();
    status.setState(com.linkedin.anomaly.AnomalyState.ACTIVE);
    status.setLastUpdated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));
    return status;
  }

  private com.linkedin.anomaly.AnomalyReview createReview() {
    com.linkedin.anomaly.AnomalyReview review = new com.linkedin.anomaly.AnomalyReview();
    review.setState(com.linkedin.anomaly.AnomalyReviewState.PENDING);
    review.setLastUpdated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));
    return review;
  }

  private com.linkedin.anomaly.AnomalySource createSource() {
    com.linkedin.anomaly.AnomalySource source = new com.linkedin.anomaly.AnomalySource();
    source.setType(com.linkedin.anomaly.AnomalySourceType.INFERRED_ASSERTION_FAILURE);
    return source;
  }

  private void verifyRequiredFields(Anomaly output, AnomalyInfo input) {
    assertEquals(output.getType(), EntityType.ANOMALY);
    assertEquals(output.getAnomalyType().toString(), input.getType().toString());
    assertEquals(output.getCreated().getTime(), input.getCreated().getTime());
    assertEquals(output.getCreated().getActor(), input.getCreated().getActor().toString());
    assertEquals(output.getEntity().getUrn(), input.getEntity().toString());
    assertEquals(output.getStatus().getState().toString(), input.getStatus().getState().toString());
    assertEquals(
        output.getStatus().getLastUpdated().getTime(),
        input.getStatus().getLastUpdated().getTime());
    assertEquals(
        output.getStatus().getLastUpdated().getActor(),
        input.getStatus().getLastUpdated().getActor().toString());
    assertEquals(output.getSource().getType().toString(), input.getSource().getType().toString());
    assertEquals(output.getReview().getState().toString(), input.getReview().getState().toString());
    assertEquals(
        output.getReview().getLastUpdated().getTime(),
        input.getReview().getLastUpdated().getTime());
    assertEquals(
        output.getReview().getLastUpdated().getActor(),
        input.getReview().getLastUpdated().getActor().toString());
  }
}
