/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.common.mappers;

import static org.testng.Assert.*;

import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.AuditStamp;
import org.testng.annotations.Test;

public class TimeStampToAuditStampMapperTest {

  private static final String TEST_ACTOR_URN = "urn:li:corpuser:testUser";
  private static final long TEST_TIME = 1234567890L;

  @Test
  public void testMapWithActor() throws Exception {
    TimeStamp input = new TimeStamp();
    input.setTime(TEST_TIME);
    input.setActor(Urn.createFromString(TEST_ACTOR_URN));

    AuditStamp result = TimeStampToAuditStampMapper.map(null, input);

    assertNotNull(result);
    assertEquals(result.getTime().longValue(), TEST_TIME);
    assertEquals(result.getActor(), TEST_ACTOR_URN);
  }

  @Test
  public void testMapWithoutActor() {
    TimeStamp input = new TimeStamp();
    input.setTime(TEST_TIME);

    AuditStamp result = TimeStampToAuditStampMapper.map(null, input);

    assertNotNull(result);
    assertEquals(result.getTime().longValue(), TEST_TIME);
    assertNull(result.getActor());
  }

  @Test
  public void testMapNull() {
    AuditStamp result = TimeStampToAuditStampMapper.map(null, null);

    assertNull(result);
  }
}
