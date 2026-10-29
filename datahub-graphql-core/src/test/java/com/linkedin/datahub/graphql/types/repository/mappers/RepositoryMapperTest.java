package com.linkedin.datahub.graphql.types.repository.mappers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.*;
import static org.testng.Assert.assertSame;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePathEntry;
import com.linkedin.common.BrowsePathEntryArray;
import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Repository;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.repository.RepositoryLineage;
import com.linkedin.repository.RepositoryProperties;
import com.linkedin.repository.RepositorySource;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RepositoryMapperTest {

  private static final Urn REPO_URN = UrnUtils.getUrn("urn:li:repository:(github,acme/tools)");
  private static final Urn PLATFORM_URN = UrnUtils.getUrn("urn:li:dataPlatform:github");

  private QueryContext queryContext;

  @BeforeMethod
  public void setup() {
    queryContext = TestUtils.getMockAllowContext();
  }

  private static EnvelopedAspect envelop(final Object pdl) {
    return new EnvelopedAspect()
        .setValue(new Aspect(((com.linkedin.data.template.RecordTemplate) pdl).data()));
  }

  @Test
  public void testMapFullRepository() {
    final RepositoryProperties properties = new RepositoryProperties();
    properties.setName("acme/tools");
    properties.setDescription("The tools monorepo");
    properties.setDefaultBranch("main");
    properties.setLanguages(new StringArray("Java", "Kotlin"));
    properties.setLicense("Apache-2.0");
    properties.setHomepageUrl("https://github.com/acme/tools");
    properties.setArchived(false);

    final RepositorySource source = new RepositorySource();
    source.setExternalUrl("https://github.com/acme/tools");
    source.setExternalId("acme/tools");

    final Status status = new Status();
    status.setRemoved(false);

    final DataPlatformInstance platformInstance = new DataPlatformInstance();
    platformInstance.setPlatform(PLATFORM_URN);

    final EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(Constants.REPOSITORY_PROPERTIES_ASPECT_NAME, envelop(properties));
    aspects.put(Constants.REPOSITORY_SOURCE_ASPECT_NAME, envelop(source));
    aspects.put(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME, envelop(platformInstance));
    aspects.put(Constants.STATUS_ASPECT_NAME, envelop(status));

    final EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(REPO_URN);
    entityResponse.setEntityName(Constants.REPOSITORY_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    final Repository result = RepositoryMapper.map(queryContext, entityResponse);

    assertNotNull(result);
    assertEquals(result.getUrn(), REPO_URN.toString());
    assertEquals(result.getType(), EntityType.REPOSITORY);

    assertNotNull(result.getProperties());
    assertEquals(result.getProperties().getName(), "acme/tools");
    assertEquals(result.getProperties().getDescription(), "The tools monorepo");
    assertEquals(result.getProperties().getDefaultBranch(), "main");
    // languages is modeled as an ordered list, most-prevalent first.
    assertEquals(result.getProperties().getLanguages(), java.util.List.of("Java", "Kotlin"));
    assertEquals(result.getProperties().getLicense(), "Apache-2.0");
    assertEquals(result.getProperties().getHomepageUrl(), "https://github.com/acme/tools");
    assertFalse(result.getProperties().getArchived());

    assertNotNull(result.getSource());
    assertEquals(result.getSource().getExternalUrl(), "https://github.com/acme/tools");
    assertEquals(result.getSource().getExternalId(), "acme/tools");

    assertNotNull(result.getPlatform());
    assertEquals(result.getPlatform().getUrn(), PLATFORM_URN.toString());
    assertNotNull(result.getDataPlatformInstance());
    assertEquals(result.getDataPlatformInstance().getPlatform().getUrn(), PLATFORM_URN.toString());

    assertNotNull(result.getStatus());
    assertFalse(result.getStatus().getRemoved());
  }

  @Test
  public void testMapForkExposesForkOfStub() {
    final Urn parentUrn = UrnUtils.getUrn("urn:li:repository:(github,acme/tools-upstream)");

    final RepositoryProperties properties = new RepositoryProperties();
    properties.setName("acme/tools-fork");

    final RepositoryLineage lineage = new RepositoryLineage();
    lineage.setForkOf(parentUrn);

    final EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(Constants.REPOSITORY_PROPERTIES_ASPECT_NAME, envelop(properties));
    aspects.put(Constants.REPOSITORY_LINEAGE_ASPECT_NAME, envelop(lineage));

    final EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(REPO_URN);
    entityResponse.setEntityName(Constants.REPOSITORY_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    final Repository result = RepositoryMapper.map(queryContext, entityResponse);

    // forkOf is a bare stub (urn + type) hydrated later by the DataLoader.
    assertNotNull(result.getForkOf());
    assertEquals(result.getForkOf().getUrn(), parentUrn.toString());
    assertEquals(result.getForkOf().getType(), EntityType.REPOSITORY);
  }

  @Test
  public void testMapMinimalRepositoryOmitsOptionalSections() {
    // A repo with only properties: everything optional stays absent, no NPE.
    final RepositoryProperties properties = new RepositoryProperties();
    properties.setName("acme/bare");

    final EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(Constants.REPOSITORY_PROPERTIES_ASPECT_NAME, envelop(properties));

    final EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(REPO_URN);
    entityResponse.setEntityName(Constants.REPOSITORY_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    final Repository result = RepositoryMapper.map(queryContext, entityResponse);

    assertNotNull(result.getProperties());
    assertEquals(result.getProperties().getName(), "acme/bare");
    assertNull(result.getProperties().getDescription());
    assertNull(result.getProperties().getLanguages());
    assertNull(result.getSource());
    assertNull(result.getForkOf());
    assertNull(result.getPlatform());
  }

  @Test
  public void testMapBrowsePathV2() {
    final RepositoryProperties properties = new RepositoryProperties();
    properties.setName("acme/tools");

    final BrowsePathEntry entry = new BrowsePathEntry();
    entry.setId("Repositories");
    final BrowsePathsV2 browsePathsV2 = new BrowsePathsV2();
    browsePathsV2.setPath(new BrowsePathEntryArray(entry));

    final EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(Constants.REPOSITORY_PROPERTIES_ASPECT_NAME, envelop(properties));
    aspects.put(Constants.BROWSE_PATHS_V2_ASPECT_NAME, envelop(browsePathsV2));

    final EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(REPO_URN);
    entityResponse.setEntityName(Constants.REPOSITORY_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    final Repository result = RepositoryMapper.map(queryContext, entityResponse);

    assertNotNull(result.getBrowsePathV2());
    assertEquals(result.getBrowsePathV2().getPath().size(), 1);
    assertEquals(result.getBrowsePathV2().getPath().get(0).getName(), "Repositories");
  }

  @Test
  public void testMapPropertiesWithCreatedAndLastModified() {
    final RepositoryProperties properties = new RepositoryProperties();
    properties.setName("acme/tools");

    final Urn actorUrn = UrnUtils.getUrn("urn:li:corpuser:github-connector");
    final AuditStamp created = new AuditStamp();
    created.setActor(actorUrn);
    created.setTime(1000L);
    properties.setCreated(created);

    final AuditStamp lastModified = new AuditStamp();
    lastModified.setActor(actorUrn);
    lastModified.setTime(2000L);
    properties.setLastModified(lastModified);

    final EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(Constants.REPOSITORY_PROPERTIES_ASPECT_NAME, envelop(properties));

    final EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(REPO_URN);
    entityResponse.setEntityName(Constants.REPOSITORY_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    final Repository result = RepositoryMapper.map(queryContext, entityResponse);

    assertNotNull(result.getProperties().getCreated());
    assertEquals(result.getProperties().getCreated().getActor(), actorUrn.toString());
    assertEquals(result.getProperties().getCreated().getTime(), 1000L);

    assertNotNull(result.getProperties().getLastModified());
    assertEquals(result.getProperties().getLastModified().getActor(), actorUrn.toString());
    assertEquals(result.getProperties().getLastModified().getTime(), 2000L);
  }

  @Test
  public void testMapPropertiesOmitsAuditStampsWhenAbsent() {
    // Regression guard for the `hasCreated()`/`hasLastModified()` guards
    // added alongside these fields: a repo with none of them set should not NPE and should leave
    // the GraphQL fields null rather than defaulting to empty/zero values.
    final RepositoryProperties properties = new RepositoryProperties();
    properties.setName("acme/bare");

    final EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(Constants.REPOSITORY_PROPERTIES_ASPECT_NAME, envelop(properties));

    final EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(REPO_URN);
    entityResponse.setEntityName(Constants.REPOSITORY_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    final Repository result = RepositoryMapper.map(queryContext, entityResponse);

    assertNull(result.getProperties().getCreated());
    assertNull(result.getProperties().getLastModified());
  }

  @Test
  public void testMapRestrictsRepositoryTheViewerCannotSee() {
    final RepositoryProperties properties = new RepositoryProperties();
    properties.setName("acme/private");

    final EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(Constants.REPOSITORY_PROPERTIES_ASPECT_NAME, envelop(properties));

    final EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(REPO_URN);
    entityResponse.setEntityName(Constants.REPOSITORY_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    try (MockedStatic<AuthorizationUtils> authUtils = mockStatic(AuthorizationUtils.class)) {
      authUtils.when(() -> AuthorizationUtils.canView(any(), eq(REPO_URN))).thenReturn(false);
      final Repository restricted = new Repository();
      authUtils
          .when(
              () -> AuthorizationUtils.restrictEntity(any(Repository.class), eq(Repository.class)))
          .thenReturn(restricted);

      assertSame(RepositoryMapper.map(queryContext, entityResponse), restricted);
    }
  }
}
