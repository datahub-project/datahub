package com.linkedin.datahub.graphql.types.template;

import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.DataHubPageModule;
import com.linkedin.datahub.graphql.generated.DataHubPageTemplate;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.template.DataHubPageTemplateProperties;
import com.linkedin.template.DataHubPageTemplateRow;
import com.linkedin.template.DataHubPageTemplateRowArray;
import com.linkedin.template.DataHubPageTemplateSurface;
import com.linkedin.template.DataHubPageTemplateVisibility;
import org.testng.annotations.Test;

public class PageTemplateMapperTest {

  @Test
  public void testPageTemplateMapper() {
    // Create test data
    Urn templateUrn = UrnUtils.getUrn("urn:li:dataHubPageTemplate:test-template");
    Urn moduleUrn = UrnUtils.getUrn("urn:li:dataHubPageModule:test-module");

    // Create GMS template properties
    DataHubPageTemplateProperties gmsProperties = new DataHubPageTemplateProperties();

    // Create rows with modules
    DataHubPageTemplateRow row = new DataHubPageTemplateRow();
    row.setModules(new UrnArray(moduleUrn));
    gmsProperties.setRows(new DataHubPageTemplateRowArray(row));

    // Create surface
    DataHubPageTemplateSurface surface = new DataHubPageTemplateSurface();
    surface.setSurfaceType(com.linkedin.template.PageTemplateSurfaceType.HOME_PAGE);
    gmsProperties.setSurface(surface);

    // Create visibility
    DataHubPageTemplateVisibility visibility = new DataHubPageTemplateVisibility();
    visibility.setScope(com.linkedin.template.PageTemplateScope.GLOBAL);
    gmsProperties.setVisibility(visibility);

    // Create audit stamps
    AuditStamp created = new AuditStamp();
    created.setTime(System.currentTimeMillis());
    created.setActor(UrnUtils.getUrn("urn:li:corpuser:testuser"));
    gmsProperties.setCreated(created);

    AuditStamp lastModified = new AuditStamp();
    lastModified.setTime(System.currentTimeMillis());
    lastModified.setActor(UrnUtils.getUrn("urn:li:corpuser:testuser"));
    gmsProperties.setLastModified(lastModified);

    // Create EntityResponse
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(templateUrn);

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new com.linkedin.entity.Aspect(gmsProperties.data()));
    aspectMap.put("dataHubPageTemplateProperties", aspect);
    entityResponse.setAspects(aspectMap);

    // Test mapping
    DataHubPageTemplate result = PageTemplateMapper.map(null, entityResponse);

    // Verify results
    assertNotNull(result);
    assertEquals(result.getUrn(), templateUrn.toString());
    assertEquals(result.getType(), EntityType.DATAHUB_PAGE_TEMPLATE);

    assertNotNull(result.getProperties());
    assertNotNull(result.getProperties().getRows());
    assertEquals(result.getProperties().getRows().size(), 1);

    com.linkedin.datahub.graphql.generated.DataHubPageTemplateRow resultRow =
        result.getProperties().getRows().get(0);
    assertNotNull(resultRow.getModules());
    assertEquals(resultRow.getModules().size(), 1);

    DataHubPageModule resultModule = resultRow.getModules().get(0);
    assertEquals(resultModule.getUrn(), moduleUrn.toString());
    assertEquals(resultModule.getType(), EntityType.DATAHUB_PAGE_MODULE);

    // Verify surface
    assertNotNull(result.getProperties().getSurface());
    assertEquals(
        result.getProperties().getSurface().getSurfaceType(),
        com.linkedin.datahub.graphql.generated.PageTemplateSurfaceType.HOME_PAGE);

    // Verify visibility
    assertNotNull(result.getProperties().getVisibility());
    assertEquals(
        result.getProperties().getVisibility().getScope(),
        com.linkedin.datahub.graphql.generated.PageTemplateScope.GLOBAL);

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
}
