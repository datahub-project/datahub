package com.linkedin.metadata.dao.utils;

import com.linkedin.common.Ownership;
import com.linkedin.common.urn.DatasetGroupUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.delta.DatasetGroupDelta;
import com.linkedin.metadata.delta.Delta;
import com.linkedin.metadata.search.Document;
import com.linkedin.metadata.validator.InvalidSchemaException;
import org.testng.annotations.Test;

import static com.linkedin.metadata.utils.TestUtils.*;
import static org.testng.Assert.*;


public class ModelUtilsTest {

  @Test
  public void testGetAspectName() {
    String aspectName = ModelUtils.getAspectName(Ownership.class);
    assertEquals(aspectName, "com.linkedin.common.Ownership");
  }

  @Test
  public void testGetAspectClass() {
    Class ownershipClass = ModelUtils.getAspectClass("com.linkedin.common.Ownership");
    assertEquals(ownershipClass, Ownership.class);
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void testGetInvalidAspectClass() {
    ModelUtils.getAspectClass(Document.class.getCanonicalName());
  }

  @Test(expectedExceptions = InvalidSchemaException.class)
  public void testGetInvalidSnapshotClassFromName() {
    ModelUtils.getMetadataSnapshotClassFromName(Ownership.class.getCanonicalName());
  }

  @Test
  public void testGetUrnFromDelta() {
    DatasetGroupUrn datasetGroupUrn = makeDatasetGroupUrn("foo");
    DatasetGroupDelta delta = new DatasetGroupDelta().setUrn(datasetGroupUrn);

    Urn urn = ModelUtils.getUrnFromDelta(delta);
    assertEquals(urn.getClass(), DatasetGroupUrn.class);
    assertEquals(urn, datasetGroupUrn);
  }

  @Test
  public void testGetUrnFromDeltaUnion() {
    DatasetGroupUrn datasetGroupUrn = makeDatasetGroupUrn("foo");
    DatasetGroupDelta delta = new DatasetGroupDelta().setUrn(datasetGroupUrn);
    Delta deltaUnion = new Delta();
    deltaUnion.setDatasetGroupDelta(delta);

    Urn urn = ModelUtils.getUrnFromDeltaUnion(deltaUnion);
    assertEquals(urn.getClass(), DatasetGroupUrn.class);
    assertEquals(urn, datasetGroupUrn);
  }

  @Test
  public void testUrnClassForDelta() {
    assertEquals(ModelUtils.urnClassForDelta(DatasetGroupDelta.class), DatasetGroupUrn.class);
  }
}
