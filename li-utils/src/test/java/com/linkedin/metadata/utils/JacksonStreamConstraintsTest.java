package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

public class JacksonStreamConstraintsTest {

  @Test
  public void testDefaultLimitsRaisedAboveJacksonDefaults() {
    StreamReadConstraints constraints = JacksonStreamConstraints.streamReadConstraints();
    assertEquals(constraints.getMaxStringLength(), 16000000);
    assertEquals(constraints.getMaxNameLength(), 16000000);
  }

  @Test
  public void testCreateObjectMapperAppliesConstraints() throws Exception {
    ObjectMapper mapper = JacksonStreamConstraints.createObjectMapper();
    assertEquals(mapper.getFactory().streamReadConstraints().getMaxStringLength(), 16000000);
    assertEquals(mapper.getFactory().streamReadConstraints().getMaxNameLength(), 16000000);

    // A property name over Jackson's default 50k limit must deserialize without a
    // StreamConstraintsException.
    String longName = "f".repeat(60_000);
    assertNotNull(mapper.readTree("{\"" + longName + "\": 1}").get(longName));
  }

  @Test
  public void testApplyToExistingMapper() {
    ObjectMapper mapper = new ObjectMapper();
    JacksonStreamConstraints.applyTo(mapper);
    assertEquals(mapper.getFactory().streamReadConstraints().getMaxStringLength(), 16000000);
  }
}
