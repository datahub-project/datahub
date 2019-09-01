package com.linkedin.datahub.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.metadata.query.BrowseResultEntity;
import com.linkedin.metadata.query.BrowseResultGroup;
import com.linkedin.metadata.query.BrowseResultMetadata;
import com.linkedin.restli.common.CollectionMetadata;
import com.linkedin.restli.common.CollectionResponse;
import java.util.Collections;
import org.testng.annotations.Test;

import static com.linkedin.datahub.util.RestliUtil.*;
import static org.testng.Assert.*;


public class RestliUtilTest {

  @Test
  public void testCollectionResponseToJsonNodeForBrowse() throws Exception {
    DataMap documentsDataMap = new DataMap();
    documentsDataMap.put("elements", new DataList(Collections.singletonList(new BrowseResultEntity().data())));
    documentsDataMap.put("paging", new CollectionMetadata().setStart(0).setCount(1).setTotal(10).data());
    CollectionResponse<BrowseResultEntity> resp = new CollectionResponse<>(documentsDataMap, BrowseResultEntity.class);

    DataMap dataMap = new DataMap();
    dataMap.put("path", "/foo");
    dataMap.put("groups",
        new DataList(Collections.singletonList(new BrowseResultGroup().setName("/foo/bar").setCount(31L).data())));
    dataMap.put("totalNumEntities", 100L);

    DataMap metadataMap = new DataMap();
    metadataMap.put("metadata", dataMap);
    resp.setMetadataRaw(metadataMap);

    JsonNode node = collectionResponseToJsonNode(resp, new BrowseResultMetadata(dataMap));

    assertNotNull(node);
    assertEquals(node.get("start").asInt(), 0);
    assertEquals(node.get("count").asInt(), 1);
    assertEquals(node.get("total").asInt(), 10);
    assertTrue(node.get("elements").isArray());
    assertEquals(node.get("elements").size(), 1);
    assertEquals(node.get("elements").get(0), toJsonNode(new BrowseResultEntity()));

    assertEquals(node.get("metadata").get("path").asText(), "/foo");
    assertTrue(node.get("metadata").get("groups").isArray());
    assertEquals(node.get("metadata").get("groups").size(), 1);
    assertEquals(node.get("metadata").get("groups").get(0).get("name").asText(), "/foo/bar");
    assertEquals(node.get("metadata").get("groups").get(0).get("count").asLong(), 31L);
    assertEquals(node.get("metadata").get("totalNumEntities").asLong(), 100);
  }
}
