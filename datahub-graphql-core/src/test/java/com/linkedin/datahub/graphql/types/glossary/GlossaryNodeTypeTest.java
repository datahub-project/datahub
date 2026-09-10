package com.linkedin.datahub.graphql.types.glossary;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.metadata.key.GlossaryNodeKey;
import graphql.execution.DataFetcherResult;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class GlossaryNodeTypeTest {

  private static final String TEST_NODE_URN = "urn:li:glossaryNode:Classification";

  @Test
  public void testBatchLoadRequestsGlobalTagsAspect() throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    Urn nodeUrn = Urn.createFromString(TEST_NODE_URN);

    GlossaryNodeKey key = new GlossaryNodeKey().setName("Classification");
    GlossaryNodeInfo info =
        new GlossaryNodeInfo().setName("Classification").setDefinition("Classification terms");
    GlobalTags tags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(new TagAssociation().setTag(new TagUrn("sensitive")))));

    Mockito.when(client.batchGetV2(any(), eq(GLOSSARY_NODE_ENTITY_NAME), any(), any()))
        .thenReturn(
            ImmutableMap.of(
                nodeUrn,
                new EntityResponse()
                    .setEntityName(GLOSSARY_NODE_ENTITY_NAME)
                    .setUrn(nodeUrn)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                GLOSSARY_NODE_KEY_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(key.data())),
                                GLOSSARY_NODE_INFO_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(info.data())),
                                GLOBAL_TAGS_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(tags.data())))))));

    GlossaryNodeType type = new GlossaryNodeType(client);
    QueryContext mockContext = getMockAllowContext();

    List<DataFetcherResult<GlossaryNode>> result =
        type.batchLoad(ImmutableList.of(TEST_NODE_URN), mockContext);

    ArgumentCaptor<Set<String>> aspectsCaptor = ArgumentCaptor.forClass(Set.class);
    Mockito.verify(client)
        .batchGetV2(
            any(),
            eq(GLOSSARY_NODE_ENTITY_NAME),
            eq(new HashSet<>(ImmutableSet.of(nodeUrn))),
            aspectsCaptor.capture());
    assertTrue(
        aspectsCaptor.getValue().contains(GLOBAL_TAGS_ASPECT_NAME),
        "GlossaryNodeType must request the globalTags aspect");

    assertEquals(result.size(), 1);
    GlossaryNode node = result.get(0).getData();
    assertEquals(node.getUrn(), TEST_NODE_URN);
    assertEquals(node.getType(), EntityType.GLOSSARY_NODE);
    assertNotNull(node.getTags());
    assertEquals(node.getTags().getTags().size(), 1);
    assertEquals(node.getTags().getTags().get(0).getTag().getUrn(), "urn:li:tag:sensitive");
  }
}
