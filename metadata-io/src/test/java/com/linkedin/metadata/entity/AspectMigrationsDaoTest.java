package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.AspectIngestionUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.service.UpdateIndicesService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

public abstract class AspectMigrationsDaoTest<T extends AspectMigrationsDao> {

  protected T _migrationsDao;
  protected EventProducer _mockProducer;
  protected EntityServiceImpl _entityServiceImpl;
  protected RetentionService _retentionService;
  protected UpdateIndicesService _mockUpdateIndicesService;

  @Test
  public void testListAllUrns() throws AssertionError {
    final int totalAspects = 30;
    final int pageSize = 25;
    final int lastPageSize = 5;
    Map<Urn, CorpUserKey> ingestedAspects =
        AspectIngestionUtils.ingestCorpUserKeyAspects(_entityServiceImpl, totalAspects);
    List<String> ingestedUrns =
        ingestedAspects.keySet().stream().map(Urn::toString).collect(Collectors.toList());
    List<String> seenUrns = new ArrayList<>();

    Iterable<String> page1 = _migrationsDao.listAllUrns(0, pageSize);
    List<String> page1Urns = ImmutableList.copyOf(page1);

    // validate first page
    assertEquals(page1Urns.size(), pageSize);
    for (String urn : page1Urns) {
      assertNotNull(UrnUtils.getUrn(urn));
      seenUrns.add(urn);
    }

    Iterable<String> page2 = _migrationsDao.listAllUrns(pageSize, pageSize);
    List<String> page2Urns = ImmutableList.copyOf(page2);

    // validate last page
    assertEquals(page2Urns.size(), lastPageSize);
    for (String urn : page2Urns) {
      assertNotNull(UrnUtils.getUrn(urn));
      seenUrns.add(urn);
    }

    // validate all ingested URNs were returned exactly once
    for (String urn : ingestedUrns) {
      assertEquals(seenUrns.stream().filter(u -> u.equals(urn)).count(), 1);
    }
  }

  @Test
  public void testCountEntities() throws AssertionError {
    AspectIngestionUtils.ingestCorpUserInfoAspects(_entityServiceImpl, 11);
    AspectIngestionUtils.ingestChartInfoAspects(_entityServiceImpl, 22);
    final int expected = 33;

    long actual = _migrationsDao.countEntities();

    assertEquals(actual, expected);
  }

  @Test
  public void testCheckIfAspectExists() throws AssertionError {
    boolean actual = _migrationsDao.checkIfAspectExists(CORP_USER_INFO_ASPECT_NAME);
    assertFalse(actual);

    AspectIngestionUtils.ingestCorpUserInfoAspects(_entityServiceImpl, 1);

    actual = _migrationsDao.checkIfAspectExists(CORP_USER_INFO_ASPECT_NAME);
    assertTrue(actual);
  }
}
