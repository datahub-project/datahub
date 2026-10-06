package com.linkedin.datahub.graphql.types.glossary.mappers;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.key.GlossaryTermKey;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GlossaryTermMapperTest {

  private static final String TEST_TERM_URN = "urn:li:glossaryTerm:Classification.PII";
  private static final String TEST_TERM_ID = "Classification.PII";
  private static final String TEST_TAG_NAME = "sensitive";

  private Urn glossaryTermUrn;
  private QueryContext mockQueryContext;

  @BeforeMethod
  public void setup() throws URISyntaxException {
    glossaryTermUrn = Urn.createFromString(TEST_TERM_URN);
    mockQueryContext = mock(QueryContext.class);
  }

  @Test
  public void testMapGlossaryTermWithTags() throws Exception {
    EntityResponse entityResponse = createBasicEntityResponse();

    GlobalTags globalTags = new GlobalTags();
    globalTags.setTags(
        new TagAssociationArray(
            ImmutableList.of(new TagAssociation().setTag(new TagUrn(TEST_TAG_NAME)))));
    addAspectToResponse(entityResponse, GLOBAL_TAGS_ASPECT_NAME, globalTags);

    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      // GlobalTagsMapper also checks canView on each tag URN.
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), any())).thenReturn(true);

      GlossaryTerm result = GlossaryTermMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result);
      assertEquals(result.getUrn(), TEST_TERM_URN);
      assertEquals(result.getType(), EntityType.GLOSSARY_TERM);
      assertNotNull(result.getTags());
      assertNotNull(result.getTags().getTags());
      assertEquals(result.getTags().getTags().size(), 1);
      assertEquals(
          result.getTags().getTags().get(0).getTag().getUrn(), "urn:li:tag:" + TEST_TAG_NAME);
    }
  }

  @Test
  public void testMapGlossaryTermWithoutTags() {
    EntityResponse entityResponse = createBasicEntityResponse();

    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock
          .when(() -> AuthorizationUtils.canView(any(), eq(glossaryTermUrn)))
          .thenReturn(true);

      GlossaryTerm result = GlossaryTermMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result);
      assertEquals(result.getUrn(), TEST_TERM_URN);
      assertNull(result.getTags());
    }
  }

  private EntityResponse createBasicEntityResponse() {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(glossaryTermUrn);

    GlossaryTermKey key = new GlossaryTermKey();
    key.setName(TEST_TERM_ID);

    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(key.data()));

    GlossaryTermInfo info = new GlossaryTermInfo();
    info.setName("PII");
    info.setDefinition("Personally Identifiable Information");
    info.setTermSource("INTERNAL");

    EnvelopedAspect infoAspect = new EnvelopedAspect();
    infoAspect.setValue(new Aspect(info.data()));

    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(GLOSSARY_TERM_KEY_ASPECT_NAME, keyAspect);
    aspects.put(GLOSSARY_TERM_INFO_ASPECT_NAME, infoAspect);

    entityResponse.setAspects(new EnvelopedAspectMap(aspects));
    return entityResponse;
  }

  private void addAspectToResponse(
      EntityResponse entityResponse, String aspectName, RecordTemplate aspectData) {
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new Aspect(aspectData.data()));
    entityResponse.getAspects().put(aspectName, aspect);
  }
}
