package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.AspectIngestionUtils;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.service.UpdateIndicesService;
import io.ebean.Database;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EbeanAspectMigrationsDaoTest extends AspectMigrationsDaoTest<EbeanAspectDao> {

  public EbeanAspectMigrationsDaoTest() throws EntityRegistryException {}

  @BeforeMethod
  public void setupTest() {
    Database server =
        EbeanTestUtils.createTestServer(EbeanAspectMigrationsDaoTest.class.getSimpleName());
    _mockProducer = mock(EventProducer.class);
    EbeanAspectDao dao = new EbeanAspectDao(server);
    dao.setConnectionValidated(true);
    _mockUpdateIndicesService = mock(UpdateIndicesService.class);
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    _entityServiceImpl =
        new EntityServiceImpl(
            dao,
            _mockProducer,
            _testEntityRegistry,
            true,
            _mockUpdateIndicesService,
            preProcessHooks);
    _retentionService = new EbeanRetentionService(_entityServiceImpl, server, 1000);
    _entityServiceImpl.setRetentionService(_retentionService);

    _migrationsDao = dao;
  }

  @Test
  public void testStreamAspects() throws AssertionError {
    final int totalAspects = 30;
    Map<Urn, CorpUserKey> ingestedAspects =
        AspectIngestionUtils.ingestCorpUserKeyAspects(_entityServiceImpl, totalAspects);
    List<String> ingestedUrns =
        ingestedAspects.keySet().stream().map(Urn::toString).collect(Collectors.toList());

    Stream<EntityAspect> aspectStream =
        _migrationsDao.streamAspects(CORP_USER_ENTITY_NAME, CORP_USER_KEY_ASPECT_NAME);
    List<EntityAspect> aspectList = aspectStream.collect(Collectors.toList());
    assertEquals(ingestedUrns.size(), aspectList.size());
    Set<String> urnsFetched =
        aspectList.stream().map(EntityAspect::getUrn).collect(Collectors.toSet());
    for (String urn : ingestedUrns) {
      assertTrue(urnsFetched.contains(urn));
    }
  }
}
