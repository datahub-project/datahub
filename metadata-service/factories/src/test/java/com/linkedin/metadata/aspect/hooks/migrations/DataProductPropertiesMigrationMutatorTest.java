package com.linkedin.metadata.aspect.hooks.migrations;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.RetrieverContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DataProductPropertiesMigrationMutatorTest {

  @Test
  public void testVersionsAndAspectName() {
    DataProductPropertiesMigrationMutator mutator = new DataProductPropertiesMigrationMutator();
    assertEquals(mutator.getAspectName(), Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    assertEquals(mutator.getSourceVersion(), 1L);
    assertEquals(mutator.getTargetVersion(), 2L);
  }

  @Test
  public void testTransformReturnsCopy() {
    DataProductProperties properties = new DataProductProperties();
    properties.setName("Ads");
    DataProductAssociationArray assets = new DataProductAssociationArray();
    DataProductAssociation association = new DataProductAssociation();
    association.setDestinationUrn(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users,PROD)"));
    assets.add(association);
    properties.setAssets(assets);

    DataProductPropertiesMigrationMutator mutator = new DataProductPropertiesMigrationMutator();
    RecordTemplate migrated = mutator.transform(properties, Mockito.mock(RetrieverContext.class));

    assertNotNull(migrated);
    assertNotSame(migrated, properties);
    assertEquals(new DataProductProperties(migrated.data()).getName(), "Ads");
  }
}
