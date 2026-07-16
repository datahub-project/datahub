package com.linkedin.datahub.graphql.util;

import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.ResolvedAuditStamp;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import java.net.URISyntaxException;
import org.testng.annotations.Test;

public class EntityResponseUtilsTest {
  @Test
  public void testExtractAspectCreatedAuditStamp() throws URISyntaxException {
    // Create a mock EntityResponse
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(Urn.createFromString("urn:li:corpuser:test"));
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    AuditStamp created = new AuditStamp();
    created.setActor(Urn.createFromString("urn:li:corpuser:test"));
    created.setTime(1234567890L);
    aspect.setCreated(created);
    aspectMap.put("testAspect", aspect);
    entityResponse.setAspects(aspectMap);

    // Call the method to be tested
    ResolvedAuditStamp auditStamp =
        EntityResponseUtils.extractAspectCreatedAuditStamp(entityResponse, "testAspect");

    // Assert the results
    assertEquals(auditStamp.getActor().getUrn(), "urn:li:corpuser:test");
    assertEquals(auditStamp.getTime(), (Long) 1234567890L);
  }

  @Test
  public void testExtractAspectCreatedAuditStampDefault() throws URISyntaxException {
    // Test with a null EntityResponse
    ResolvedAuditStamp auditStamp1 =
        EntityResponseUtils.extractAspectCreatedAuditStamp(null, "testAspect");
    assertNull(auditStamp1);

    // Test with an EntityResponse with no aspects
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(Urn.createFromString("urn:li:corpuser:test"));
    ResolvedAuditStamp auditStamp2 =
        EntityResponseUtils.extractAspectCreatedAuditStamp(entityResponse, "testAspect");
    assertNull(auditStamp2);

    // Test with an EntityResponse with an empty aspect map
    entityResponse.setAspects(new EnvelopedAspectMap());
    ResolvedAuditStamp auditStamp3 =
        EntityResponseUtils.extractAspectCreatedAuditStamp(entityResponse, "testAspect");
    assertNull(auditStamp3);

    // Test with an EntityResponse with a missing aspect
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put("otherAspect", new EnvelopedAspect());
    entityResponse.setAspects(aspectMap);
    ResolvedAuditStamp auditStamp4 =
        EntityResponseUtils.extractAspectCreatedAuditStamp(entityResponse, "testAspect");
    assertNull(auditStamp4);

    // Test with an EntityResponse with an aspect with no created field
    aspectMap.put("testAspect", new EnvelopedAspect());
    entityResponse.setAspects(aspectMap);
    ResolvedAuditStamp auditStamp5 =
        EntityResponseUtils.extractAspectCreatedAuditStamp(entityResponse, "testAspect");
    assertNull(auditStamp5);
  }
}
