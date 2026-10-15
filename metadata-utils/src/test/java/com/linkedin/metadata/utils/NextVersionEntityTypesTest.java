package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

public class NextVersionEntityTypesTest {

  @Test
  public void testLooksUpTheEntityTypeRowOncePerName() {
    List<Urn> lookups = new ArrayList<>();
    NextVersionEntityTypes types =
        new NextVersionEntityTypes(
            urn -> {
              lookups.add(urn);
              return urn.toString().equals("urn:li:entityType:datahub.entityFromNewerBuild");
            });

    assertTrue(types.test("entityFromNewerBuild"));
    assertTrue(types.test("entityFromNewerBuild"));
    assertFalse(types.test("corpusr"));
    assertFalse(types.test("corpusr"));

    assertEquals(lookups.size(), 2);
  }

  @Test
  public void testFailedLookupToleratesAndRetries() {
    List<Urn> lookups = new ArrayList<>();
    NextVersionEntityTypes types =
        new NextVersionEntityTypes(
            urn -> {
              lookups.add(urn);
              throw new IllegalStateException("store unavailable");
            });

    assertTrue(types.test("entityFromNewerBuild"));
    assertTrue(types.test("entityFromNewerBuild"));

    assertEquals(lookups.size(), 2);
  }

  @Test
  public void testNamesThatCannotFormAnUrnAreNotRegistered() {
    NextVersionEntityTypes types = new NextVersionEntityTypes(urn -> true);

    assertFalse(types.test(null));
    assertFalse(types.test(""));
    assertFalse(types.test("bad(name"));
  }
}
