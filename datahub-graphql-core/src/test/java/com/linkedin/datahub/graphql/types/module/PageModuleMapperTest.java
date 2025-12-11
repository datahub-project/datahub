/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.module;

import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.DataHubPageModule;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.module.DataHubPageModuleParams;
import com.linkedin.module.DataHubPageModuleProperties;
import com.linkedin.module.DataHubPageModuleType;
import com.linkedin.module.DataHubPageModuleVisibility;
import com.linkedin.module.PageModuleScope;
import org.testng.annotations.Test;

public class PageModuleMapperTest {

  @Test
  public void testPageModuleMapper() throws Exception {
    // Create test data
    Urn moduleUrn = UrnUtils.getUrn("urn:li:dataHubPageModule:test-module");

    // Create GMS properties
    DataHubPageModuleProperties gmsProperties = new DataHubPageModuleProperties();
    gmsProperties.setName("Test Module");
    gmsProperties.setType(DataHubPageModuleType.LINK);

    // Create visibility
    DataHubPageModuleVisibility visibility = new DataHubPageModuleVisibility();
    visibility.setScope(PageModuleScope.GLOBAL);
    gmsProperties.setVisibility(visibility);

    // Create params
    DataHubPageModuleParams params = new DataHubPageModuleParams();
    // Note: We're not setting linkParams due to the schema inconsistency
    gmsProperties.setParams(params);

    // Create audit stamps
    AuditStamp created = new AuditStamp();
    created.setTime(System.currentTimeMillis());
    created.setActor(UrnUtils.getUrn("urn:li:corpuser:test-user"));
    gmsProperties.setCreated(created);

    AuditStamp lastModified = new AuditStamp();
    lastModified.setTime(System.currentTimeMillis());
    lastModified.setActor(UrnUtils.getUrn("urn:li:corpuser:test-user"));
    gmsProperties.setLastModified(lastModified);

    // Create entity response
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(moduleUrn);

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new com.linkedin.entity.Aspect(gmsProperties.data()));
    aspectMap.put("dataHubPageModuleProperties", aspect);
    entityResponse.setAspects(aspectMap);

    // Test mapping
    DataHubPageModule result = PageModuleMapper.map(null, entityResponse);

    // Verify basic fields
    assertEquals(result.getUrn(), moduleUrn.toString());
    assertEquals(result.getType(), EntityType.DATAHUB_PAGE_MODULE);

    // Verify properties
    assertNotNull(result.getProperties());
    assertEquals(result.getProperties().getName(), "Test Module");
    assertEquals(
        result.getProperties().getType(),
        com.linkedin.datahub.graphql.generated.DataHubPageModuleType.LINK);

    // Verify visibility
    assertNotNull(result.getProperties().getVisibility());
    assertEquals(
        result.getProperties().getVisibility().getScope(),
        com.linkedin.datahub.graphql.generated.PageModuleScope.GLOBAL);

    // Verify params (should be null due to schema inconsistency)
    assertNotNull(result.getProperties().getParams());
    assertNull(result.getProperties().getParams().getLinkParams());
    assertNull(result.getProperties().getParams().getRichTextParams());

    // Verify audit stamps
    assertNotNull(result.getProperties().getCreated());
    assertEquals(result.getProperties().getCreated().getTime(), created.getTime());
    assertNotNull(result.getProperties().getCreated().getActor());
    assertEquals(
        result.getProperties().getCreated().getActor().getUrn(), created.getActor().toString());

    assertNotNull(result.getProperties().getLastModified());
    assertEquals(result.getProperties().getLastModified().getTime(), lastModified.getTime());
    assertNotNull(result.getProperties().getLastModified().getActor());
    assertEquals(
        result.getProperties().getLastModified().getActor().getUrn(),
        lastModified.getActor().toString());
  }

  @Test
  public void testDefaultModule() {
    // Create test data with no aspects
    Urn moduleUrn = UrnUtils.getUrn("urn:li:dataHubPageModule:test-module");
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(moduleUrn);
    entityResponse.setAspects(new EnvelopedAspectMap());

    // Map the form
    DataHubPageModule result = PageModuleMapper.map(null, entityResponse);

    // Verify the default form
    assertNotNull(result);
    assertEquals(result.getUrn(), moduleUrn.toString());
    assertEquals(result.getType(), EntityType.DATAHUB_PAGE_MODULE);
    assertEquals(result.getProperties().getName(), "");
    assertEquals(
        result.getProperties().getType(),
        com.linkedin.datahub.graphql.generated.DataHubPageModuleType.OWNED_ASSETS);
    assertEquals(
        result.getProperties().getVisibility().getScope(),
        com.linkedin.datahub.graphql.generated.PageModuleScope.GLOBAL);
    assertNotNull(result.getProperties().getCreated());
    assertNotNull(result.getProperties().getLastModified());
    assertNotNull(result.getProperties().getParams());
  }
}
