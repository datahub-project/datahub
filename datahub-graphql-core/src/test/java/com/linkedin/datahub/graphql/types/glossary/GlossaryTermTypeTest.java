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
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.key.GlossaryTermKey;
import graphql.execution.DataFetcherResult;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class GlossaryTermTypeTest {

  private static final String TEST_TERM_URN = "urn:li:glossaryTerm:Classification.PII";

  @Test
  public void testBatchLoadRequestsGlobalTagsAspect() throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    Urn termUrn = Urn.createFromString(TEST_TERM_URN);

    GlossaryTermKey key = new GlossaryTermKey().setName("Classification.PII");
    GlossaryTermInfo info =
        new GlossaryTermInfo()
            .setName("PII")
            .setDefinition("Personally Identifiable Information")
            .setTermSource("INTERNAL");
    GlobalTags tags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(new TagAssociation().setTag(new TagUrn("sensitive")))));

    Mockito.when(client.batchGetV2(any(), eq(GLOSSARY_TERM_ENTITY_NAME), any(), any()))
        .thenReturn(
            ImmutableMap.of(
                termUrn,
                new EntityResponse()
                    .setEntityName(GLOSSARY_TERM_ENTITY_NAME)
                    .setUrn(termUrn)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                GLOSSARY_TERM_KEY_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(key.data())),
                                GLOSSARY_TERM_INFO_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(info.data())),
                                GLOBAL_TAGS_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(tags.data())))))));

    GlossaryTermType type = new GlossaryTermType(client);
    QueryContext mockContext = getMockAllowContext();

    List<DataFetcherResult<GlossaryTerm>> result =
        type.batchLoad(ImmutableList.of(TEST_TERM_URN), mockContext);

    ArgumentCaptor<Set<String>> aspectsCaptor = ArgumentCaptor.forClass(Set.class);
    Mockito.verify(client)
        .batchGetV2(
            any(),
            eq(GLOSSARY_TERM_ENTITY_NAME),
            eq(new HashSet<>(ImmutableSet.of(termUrn))),
            aspectsCaptor.capture());
    assertTrue(
        aspectsCaptor.getValue().contains(GLOBAL_TAGS_ASPECT_NAME),
        "GlossaryTermType must request the globalTags aspect");

    assertEquals(result.size(), 1);
    GlossaryTerm term = result.get(0).getData();
    assertEquals(term.getUrn(), TEST_TERM_URN);
    assertEquals(term.getType(), EntityType.GLOSSARY_TERM);
    assertNotNull(term.getTags());
    assertEquals(term.getTags().getTags().size(), 1);
    assertEquals(term.getTags().getTags().get(0).getTag().getUrn(), "urn:li:tag:sensitive");
  }
}
