package com.linkedin.datahub.graphql.types.tag;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Deprecation;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.AspectLoadContext;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Tag;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.tag.TagProperties;
import graphql.execution.DataFetcherResult;
import java.util.List;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

/**
 * Guards the TagType fallback aspect allowlist: when optimization falls back to ASPECTS_TO_FETCH,
 * deprecation (read by TagMapper) must be included alongside tagProperties.
 */
public class TagTypeAspectAllowlistTest {

  private static final String TAG_URN = "urn:li:tag:allowlist-tag";

  @Test
  public void testFallbackAllowlistIncludesDeprecationAndTagProperties() throws Exception {
    EntityClient client = mock(EntityClient.class);
    QueryContext context = mock(QueryContext.class);
    when(context.getAspectLoadContext("Tag")).thenReturn(AspectLoadContext.fetchAll());
    when(context.getOperationContext())
        .thenReturn(mock(io.datahubproject.metadata.context.OperationContext.class));

    Urn urn = UrnUtils.getUrn(TAG_URN);
    Deprecation deprecation =
        new Deprecation()
            .setDeprecated(true)
            .setNote("deprecated")
            .setActor(UrnUtils.getUrn("urn:li:corpuser:datahub"));
    TagProperties props = new TagProperties().setName("allowlist-tag").setDescription("desc");

    when(client.batchGetV2(any(), eq(TAG_ENTITY_NAME), any(), any()))
        .thenReturn(
            ImmutableMap.of(
                urn,
                new EntityResponse()
                    .setEntityName(TAG_ENTITY_NAME)
                    .setUrn(urn)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                TAG_PROPERTIES_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(props.data())),
                                DEPRECATION_ASPECT_NAME,
                                new EnvelopedAspect().setValue(new Aspect(deprecation.data())))))));

    TagType type = new TagType(client);
    List<DataFetcherResult<Tag>> results = type.batchLoad(List.of(TAG_URN), context);

    ArgumentCaptor<Set<String>> aspectsCaptor = ArgumentCaptor.forClass(Set.class);
    verify(client)
        .batchGetV2(any(), eq(TAG_ENTITY_NAME), eq(ImmutableSet.of(urn)), aspectsCaptor.capture());
    Set<String> fetched = aspectsCaptor.getValue();
    assertTrue(fetched.contains(TAG_PROPERTIES_ASPECT_NAME));
    assertTrue(fetched.contains(DEPRECATION_ASPECT_NAME));
    assertTrue(fetched.contains(OWNERSHIP_ASPECT_NAME));

    Tag tag = results.get(0).getData();
    assertEquals(tag.getDescription(), "desc");
    assertNotNull(tag.getDeprecation());
    assertTrue(tag.getDeprecation().getDeprecated());
  }
}
