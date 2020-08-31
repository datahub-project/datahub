package com.linkedin.metadata.dao;

import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.urn.FooUrn;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.json.simple.parser.ParseException;
import org.testng.annotations.Test;

import static com.linkedin.metadata.utils.AuditStamps.*;
import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;


public class ImmutableLocalDAOTest {

  @Test
  public void testLoadAspects() {
    Map<FooUrn, AspectFoo> aspects = loadAspectsFromResource("immutable.json");

    assertEquals(aspects.size(), 3);
    assertTrue(aspects.containsKey(makeFooUrn(1)));
    assertTrue(aspects.containsKey(makeFooUrn(2)));
    assertTrue(aspects.containsKey(makeFooUrn(3)));
  }

  @Test
  public void testGet() {
    ImmutableLocalDAO<EntityAspectUnion, FooUrn> dao =
        new ImmutableLocalDAO<>(EntityAspectUnion.class, loadAspectsFromResource("immutable.json"), true, FooUrn.class);

    Optional<AspectFoo> foo1 = dao.get(AspectFoo.class, makeFooUrn(1));

    assertTrue(foo1.isPresent());
    assertEquals(foo1.get(), new AspectFoo().setValue("1"));
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testAdd() {
    ImmutableLocalDAO<EntityAspectUnion, FooUrn> dao =
        new ImmutableLocalDAO<>(EntityAspectUnion.class, new HashMap<>(), true, FooUrn.class);

    dao.add(makeFooUrn(1), new AspectFoo().setValue("1"), makeAuditStamp("foo"));
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testNewNumericId() {
    ImmutableLocalDAO<EntityAspectUnion, FooUrn> dao =
        new ImmutableLocalDAO<>(EntityAspectUnion.class, new HashMap<>(), true, FooUrn.class);

    dao.newNumericId();
  }

  private Map<FooUrn, AspectFoo> loadAspectsFromResource(String name) {
    try {
      return ImmutableLocalDAO.loadAspects(AspectFoo.class, getClass().getClassLoader().getResourceAsStream(name));
    } catch (ParseException | IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
