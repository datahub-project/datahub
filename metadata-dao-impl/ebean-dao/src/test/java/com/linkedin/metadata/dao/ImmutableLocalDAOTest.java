package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.EntityAspectUnion;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.json.simple.parser.ParseException;
import org.testng.annotations.Test;

import static com.linkedin.metadata.utils.TestUtils.*;
import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;


public class ImmutableLocalDAOTest {

  @Test
  public void testLoadAspects() {
    Map<Urn, AspectFoo> aspects = loadAspectsFromResource("immutable.json");

    assertEquals(aspects.size(), 3);
    assertTrue(aspects.containsKey(makeUrn(1)));
    assertTrue(aspects.containsKey(makeUrn(2)));
    assertTrue(aspects.containsKey(makeUrn(3)));
  }

  @Test
  public void testGet() {
    ImmutableLocalDAO<EntityAspectUnion, Urn> dao =
        new ImmutableLocalDAO<>(EntityAspectUnion.class, loadAspectsFromResource("immutable.json"), true);

    Optional<AspectFoo> foo1 = dao.get(AspectFoo.class, makeUrn(1));

    assertTrue(foo1.isPresent());
    assertEquals(foo1.get(), new AspectFoo().setValue("1"));
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testAdd() {
    ImmutableLocalDAO<EntityAspectUnion, Urn> dao =
        new ImmutableLocalDAO<>(EntityAspectUnion.class, new HashMap<>(), true);

    dao.add(makeUrn(1), new AspectFoo().setValue("1"), makeAuditStamp("foo"));
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testNewNumericId() {
    ImmutableLocalDAO<EntityAspectUnion, Urn> dao =
        new ImmutableLocalDAO<>(EntityAspectUnion.class, new HashMap<>(), true);

    dao.newNumericId();
  }

  private Map<Urn, AspectFoo> loadAspectsFromResource(String name) {
    try {
      return ImmutableLocalDAO.loadAspects(AspectFoo.class, getClass().getClassLoader().getResourceAsStream(name));
    } catch (ParseException | IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
