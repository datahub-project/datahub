package com.linkedin.metadata.dataproducts;

import static com.linkedin.metadata.Constants.DATA_PRODUCT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.utils.AuditStampUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Optional;
import org.testng.annotations.Test;

/**
 * Verifies that a 3-level Data Product hierarchy indexes {@code parentDataProduct} / {@code
 * hasParentDataProduct} correctly, including re-parent and clear-parent cases.
 */
public class DataProductParentHierarchySearchTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final EntitySpec ENTITY_SPEC =
      OP_CONTEXT.getEntityRegistry().getEntitySpec(DATA_PRODUCT_ENTITY_NAME);
  private static final AspectSpec ASPECT_SPEC =
      ENTITY_SPEC.getAspectSpec(DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
  private static final SearchDocumentTransformer TRANSFORMER =
      new SearchDocumentTransformer(1000, 1000, 1000);

  private static final Urn ROOT = UrnUtils.getUrn("urn:li:dataProduct:hierarchy-root");
  private static final Urn MID = UrnUtils.getUrn("urn:li:dataProduct:hierarchy-mid");
  private static final Urn LEAF = UrnUtils.getUrn("urn:li:dataProduct:hierarchy-leaf");

  private Optional<ObjectNode> transform(Urn urn, DataProductProperties props) throws Exception {
    return TRANSFORMER.transformAspect(
        OP_CONTEXT, urn, props, ASPECT_SPEC, false, AuditStampUtils.createDefaultAuditStamp());
  }

  @Test
  public void testThreeLevelCreateReparentAndClear() throws Exception {
    // Create: root (no parent)
    DataProductProperties rootProps = new DataProductProperties().setName("Root");
    Optional<ObjectNode> rootDoc = transform(ROOT, rootProps);
    assertTrue(rootDoc.isPresent());
    assertFalse(rootDoc.get().path("hasParentDataProduct").asBoolean(false));
    assertTrue(
        !rootDoc.get().has("parentDataProduct") || rootDoc.get().get("parentDataProduct").isNull());

    // Create: mid under root
    DataProductProperties midProps =
        new DataProductProperties().setName("Mid").setParentDataProduct(ROOT);
    Optional<ObjectNode> midDoc = transform(MID, midProps);
    assertTrue(midDoc.isPresent());
    assertTrue(midDoc.get().get("hasParentDataProduct").asBoolean());
    assertEquals(midDoc.get().get("parentDataProduct").asText(), ROOT.toString());

    // Create: leaf under mid
    DataProductProperties leafProps =
        new DataProductProperties().setName("Leaf").setParentDataProduct(MID);
    Optional<ObjectNode> leafDoc = transform(LEAF, leafProps);
    assertTrue(leafDoc.isPresent());
    assertTrue(leafDoc.get().get("hasParentDataProduct").asBoolean());
    assertEquals(leafDoc.get().get("parentDataProduct").asText(), MID.toString());

    // Re-parent: leaf under root
    DataProductProperties reparented =
        new DataProductProperties().setName("Leaf").setParentDataProduct(ROOT);
    Optional<ObjectNode> reparentedDoc = transform(LEAF, reparented);
    assertTrue(reparentedDoc.isPresent());
    assertTrue(reparentedDoc.get().get("hasParentDataProduct").asBoolean());
    assertEquals(reparentedDoc.get().get("parentDataProduct").asText(), ROOT.toString());

    // Clear parent (delete parent pointer / move to root of taxonomy)
    DataProductProperties cleared = new DataProductProperties().setName("Leaf");
    Optional<ObjectNode> clearedDoc = transform(LEAF, cleared);
    assertTrue(clearedDoc.isPresent());
    assertFalse(clearedDoc.get().path("hasParentDataProduct").asBoolean(false));
  }
}
