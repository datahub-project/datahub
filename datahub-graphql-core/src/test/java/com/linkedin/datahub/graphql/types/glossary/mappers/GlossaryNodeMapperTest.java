package com.linkedin.datahub.graphql.types.glossary.mappers;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.domain.DomainAssociationArray;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.GlossaryNodeKey;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GlossaryNodeMapperTest {

  private static final String TEST_GLOSSARY_NODE_URN = "urn:li:glossaryNode:engineering";
  private static final String TEST_DOMAIN_URN = "urn:li:domain:engineering";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:testuser";
  private static final String TEST_TAG_NAME = "sensitive";
  private static final Long TEST_TIMESTAMP = 1640995200000L;

  private Urn glossaryNodeUrn;
  private Urn domainUrn;
  private Urn actorUrn;
  private QueryContext mockQueryContext;

  @BeforeMethod
  public void setup() throws URISyntaxException {
    glossaryNodeUrn = Urn.createFromString(TEST_GLOSSARY_NODE_URN);
    domainUrn = Urn.createFromString(TEST_DOMAIN_URN);
    actorUrn = Urn.createFromString(TEST_ACTOR_URN);
    mockQueryContext = mock(QueryContext.class);
  }

  @Test
  public void testMapWithDomainAspect() {
    EntityResponse entityResponse = createEntityResponse();

    Domains domains = new Domains();
    domains.setDomains(new UrnArray(domainUrn));
    com.linkedin.domain.DomainAssociation assoc = new com.linkedin.domain.DomainAssociation();
    assoc.setDomain(domainUrn);
    domains.setDomainAssociations(new DomainAssociationArray(assoc));
    addAspect(entityResponse, DOMAINS_ASPECT_NAME, domains);

    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock
          .when(() -> AuthorizationUtils.canView(any(), eq(glossaryNodeUrn)))
          .thenReturn(true);
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(domainUrn))).thenReturn(true);

      GlossaryNode result = GlossaryNodeMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result);
      assertEquals(result.getUrn(), TEST_GLOSSARY_NODE_URN);
      assertEquals(result.getType(), EntityType.GLOSSARY_NODE);
      assertNotNull(result.getDomain());
      assertEquals(result.getDomain().getDomain().getUrn(), TEST_DOMAIN_URN);
      assertEquals(result.getDomain().getAssociatedUrn(), TEST_GLOSSARY_NODE_URN);
    }
  }

  @Test
  public void testMapWithDomainAttribution() {
    EntityResponse entityResponse = createEntityResponse();

    Domains domains = new Domains();
    domains.setDomains(new UrnArray(domainUrn));
    com.linkedin.domain.DomainAssociation assoc = new com.linkedin.domain.DomainAssociation();
    assoc.setDomain(domainUrn);
    MetadataAttribution attribution = new MetadataAttribution();
    attribution.setTime(TEST_TIMESTAMP);
    attribution.setActor(actorUrn);
    assoc.setAttribution(attribution);
    domains.setDomainAssociations(new DomainAssociationArray(assoc));
    addAspect(entityResponse, DOMAINS_ASPECT_NAME, domains);

    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock
          .when(() -> AuthorizationUtils.canView(any(), eq(glossaryNodeUrn)))
          .thenReturn(true);
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), eq(domainUrn))).thenReturn(true);

      GlossaryNode result = GlossaryNodeMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result.getDomain());
      assertNotNull(result.getDomain().getAttribution());
      assertEquals(result.getDomain().getAttribution().getTime(), TEST_TIMESTAMP);
      assertEquals(result.getDomain().getAttribution().getActor().getUrn(), TEST_ACTOR_URN);
    }
  }

  @Test
  public void testMapWithoutDomainAspect() {
    EntityResponse entityResponse = createEntityResponse();

    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock
          .when(() -> AuthorizationUtils.canView(any(), eq(glossaryNodeUrn)))
          .thenReturn(true);

      GlossaryNode result = GlossaryNodeMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result);
      assertNull(result.getDomain());
    }
  }

  @Test
  public void testMapWithNullQueryContext() {
    EntityResponse entityResponse = createEntityResponse();

    Domains domains = new Domains();
    domains.setDomains(new UrnArray(domainUrn));
    com.linkedin.domain.DomainAssociation assoc = new com.linkedin.domain.DomainAssociation();
    assoc.setDomain(domainUrn);
    domains.setDomainAssociations(new DomainAssociationArray(assoc));
    addAspect(entityResponse, DOMAINS_ASPECT_NAME, domains);

    GlossaryNode result = GlossaryNodeMapper.map(null, entityResponse);

    assertNotNull(result);
    assertNotNull(result.getDomain());
    assertEquals(result.getDomain().getDomain().getUrn(), TEST_DOMAIN_URN);
  }

  @Test
  public void testMapGlossaryNodeWithTags() throws Exception {
    EntityResponse entityResponse = createEntityResponse();

    GlobalTags globalTags = new GlobalTags();
    globalTags.setTags(
        new TagAssociationArray(
            ImmutableList.of(new TagAssociation().setTag(new TagUrn(TEST_TAG_NAME)))));
    addAspect(entityResponse, GLOBAL_TAGS_ASPECT_NAME, globalTags);

    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      // GlobalTagsMapper also checks canView on each tag URN.
      authUtilsMock.when(() -> AuthorizationUtils.canView(any(), any())).thenReturn(true);

      GlossaryNode result = GlossaryNodeMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result);
      assertEquals(result.getUrn(), TEST_GLOSSARY_NODE_URN);
      assertEquals(result.getType(), EntityType.GLOSSARY_NODE);
      assertNotNull(result.getTags());
      assertNotNull(result.getTags().getTags());
      assertEquals(result.getTags().getTags().size(), 1);
      assertEquals(
          result.getTags().getTags().get(0).getTag().getUrn(), "urn:li:tag:" + TEST_TAG_NAME);
    }
  }

  @Test
  public void testMapGlossaryNodeWithoutTags() {
    EntityResponse entityResponse = createEntityResponse();

    try (MockedStatic<AuthorizationUtils> authUtilsMock = mockStatic(AuthorizationUtils.class)) {
      authUtilsMock
          .when(() -> AuthorizationUtils.canView(any(), eq(glossaryNodeUrn)))
          .thenReturn(true);

      GlossaryNode result = GlossaryNodeMapper.map(mockQueryContext, entityResponse);

      assertNotNull(result);
      assertEquals(result.getUrn(), TEST_GLOSSARY_NODE_URN);
      assertNull(result.getTags());
    }
  }

  private EntityResponse createEntityResponse() {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(glossaryNodeUrn);

    GlossaryNodeKey key = new GlossaryNodeKey();
    key.setName("engineering");

    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(key.data()));

    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(GLOSSARY_NODE_KEY_ASPECT_NAME, keyAspect);
    entityResponse.setAspects(new EnvelopedAspectMap(aspects));
    return entityResponse;
  }

  private void addAspect(
      EntityResponse entityResponse, String aspectName, RecordTemplate aspectData) {
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new Aspect(aspectData.data()));
    entityResponse.getAspects().put(aspectName, aspect);
  }
}
