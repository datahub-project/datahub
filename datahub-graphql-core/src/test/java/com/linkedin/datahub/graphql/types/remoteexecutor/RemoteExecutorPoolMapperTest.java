package com.linkedin.datahub.graphql.types.remoteexecutor;

import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.RemoteExecutorPool;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.executorpool.RemoteExecutorPoolInfo;
import com.linkedin.metadata.AcrylConstants;
import org.testng.annotations.Test;

public class RemoteExecutorPoolMapperTest {

  @Test
  public void testMap() throws Exception {
    EntityResponse entityResponse = new EntityResponse();
    Urn urn = Urn.createFromString("urn:li:remoteExecutorPool:test-pool-1");
    entityResponse.setUrn(urn);

    EnvelopedAspect envelopedPoolInfo = new EnvelopedAspect();
    long createdAtTimestamp = System.currentTimeMillis();
    RemoteExecutorPoolInfo poolInfo = new RemoteExecutorPoolInfo().setCreatedAt(createdAtTimestamp);

    envelopedPoolInfo.setValue(new Aspect(poolInfo.data()));

    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME, envelopedPoolInfo)));

    // Test non-default pool
    RemoteExecutorPool pool = RemoteExecutorPoolMapper.map(null, entityResponse, "other-pool");

    assertNotNull(pool);
    assertEquals(pool.getUrn(), "urn:li:remoteExecutorPool:test-pool-1");
    assertEquals(pool.getType(), EntityType.REMOTE_EXECUTOR_POOL);
    assertEquals(pool.getExecutorPoolId(), "test-pool-1"); // pool id should be set from URN ID
    assertEquals(pool.getCreatedAt(), createdAtTimestamp);
    assertFalse(pool.getIsDefault());

    // Test default pool
    pool = RemoteExecutorPoolMapper.map(null, entityResponse, "test-pool-1");
    assertTrue(pool.getIsDefault());
  }

  @Test
  public void testMapNoAspect() throws Exception {
    EntityResponse entityResponse = new EntityResponse();
    Urn urn = Urn.createFromString("urn:li:remoteExecutorPool:test-pool-1");
    entityResponse.setUrn(urn);
    entityResponse.setAspects(new EnvelopedAspectMap(ImmutableMap.of()));

    RemoteExecutorPool pool = RemoteExecutorPoolMapper.map(null, entityResponse, null);

    assertNotNull(pool);
    assertEquals(pool.getUrn(), "urn:li:remoteExecutorPool:test-pool-1");
    assertEquals(pool.getType(), EntityType.REMOTE_EXECUTOR_POOL);
    assertEquals(
        pool.getExecutorPoolId(),
        "test-pool-1"); // pool id should still be there when no aspect is present
    assertFalse(pool.getIsDefault());
  }

  @Test
  public void testMapNullDefaultPool() throws Exception {
    EntityResponse entityResponse = new EntityResponse();
    Urn urn = Urn.createFromString("urn:li:remoteExecutorPool:test-pool-1");
    entityResponse.setUrn(urn);

    EnvelopedAspect envelopedPoolInfo = new EnvelopedAspect();
    RemoteExecutorPoolInfo poolInfo =
        new RemoteExecutorPoolInfo().setCreatedAt(System.currentTimeMillis());

    envelopedPoolInfo.setValue(new Aspect(poolInfo.data()));

    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME, envelopedPoolInfo)));

    RemoteExecutorPool pool = RemoteExecutorPoolMapper.map(null, entityResponse, null);

    assertNotNull(pool);
    assertEquals(pool.getExecutorPoolId(), "test-pool-1"); // pool id should be set from URN ID
    assertFalse(pool.getIsDefault());
  }
}
