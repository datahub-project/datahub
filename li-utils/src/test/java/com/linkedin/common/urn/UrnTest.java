package com.linkedin.common.urn;

import java.net.URISyntaxException;
import org.testng.annotations.Test;

import static com.linkedin.common.urn.Urn.*;
import static org.junit.Assert.*;


public class UrnTest {

  private static final String LEGACY_URN_START = "urn:" + LINKEDIN_NAMESPACE;
  private static final String DEFAULT_URN_START = "urn:" + DEFAULT_NAMESPACE;

  private static final String LEGACY_URN = LEGACY_URN_START + ":corpuser:user";
  private static final String VERSIONED_URN = DEFAULT_URN_START + ":" + DEFAULT_VERSION + ":corpuser:user";
  private static final String DIFFERENT_NAMESPACE = DEFAULT_URN_START + ":corpuser:user";
  private static final String BAD_VERSION = DEFAULT_URN_START
      + ":notarealversion:dataset:(urn:li:dataPlatform:platform,name,TEST)";
  private static final String CONVERTED_URN_BAD_VERSION = DEFAULT_URN_START + ":" + DEFAULT_VERSION
      + ":notarealversion:dataset:(urn:li:dataPlatform:platform,name,TEST)";
  private static final String BAD_NAMESPACE = "urn:namespacefailuremustbelessthan32characters1234567890:corpuser:user";

  @Test
  public void testSerializeByString() {
    Urn urn1 = null;
    Urn urn2 = null;
    Urn urn3 = null;
    Urn urn4 = null;
    Urn urn5 = null;
    try {
      urn1 = Urn.createFromString(LEGACY_URN);
      urn2 = Urn.createFromString(VERSIONED_URN);
      urn3 = Urn.createFromString(DIFFERENT_NAMESPACE);
      urn4 = Urn.createFromString(BAD_VERSION);
      assertEquals(urn1.toString(), LEGACY_URN);
      assertEquals(urn2.toString(), VERSIONED_URN);
      // Not a typo
      assertEquals(urn3.toString(), VERSIONED_URN);
      assertEquals(urn4.toString(), CONVERTED_URN_BAD_VERSION);
    } catch (URISyntaxException e) {

    }
    assertNotNull(urn1);
    assertNotNull(urn2);
    assertNotNull(urn3);
    assertNotNull(urn4);

    URISyntaxException exception = null;
    urn4 = null;
    try {
      // Has to validate the Entity type for this to fail, otherwise it is a valid Urn with a defaulted version
      urn4 = DatasetUrn.createFromString(BAD_VERSION);
    } catch (URISyntaxException e) {
      exception = e;
    }
    assertNull(urn4);
    assertNotNull(exception);

    exception = null;
    try {
      urn5 = Urn.createFromString(BAD_NAMESPACE);
    } catch (URISyntaxException e) {
      exception = e;
    }
    assertNull(urn5);
    assertNotNull(exception);
  }

  @Test
  public static void validateConstructors() throws URISyntaxException {
    TupleKey tupleKey = new TupleKey();
    Urn urn1 = new Urn(DEFAULT_NAMESPACE, DatasetUrn.ENTITY_TYPE, tupleKey, DEFAULT_VERSION);
    Urn urn2 = new Urn(LINKEDIN_NAMESPACE, DatasetUrn.ENTITY_TYPE, tupleKey, DEFAULT_VERSION);
    Urn urn3 = new Urn(DatasetUrn.ENTITY_TYPE, tupleKey);
    Urn urn4 = new Urn(DatasetUrn.ENTITY_TYPE, tupleKey, DEFAULT_VERSION);
    Urn urn5 = new Urn(LINKEDIN_NAMESPACE, DatasetUrn.ENTITY_TYPE, tupleKey);
    Urn urn6 = new Urn(LEGACY_URN_START + ":dataset");

    assertNotEquals(urn1, urn2);
    assertEquals(urn2, urn3);
    assertEquals(urn3, urn4);
    assertEquals(urn4, urn5);
    assertEquals(urn5, urn6);
  }
}
