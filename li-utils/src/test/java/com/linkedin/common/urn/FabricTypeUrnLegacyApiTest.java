package com.linkedin.common.urn;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.FabricType;
import com.linkedin.data.template.DataTemplateUtil;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.function.Function;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Clients that never use custom environments keep the URN API exactly as before: the FabricType
 * constructors, {@code getOriginEntity()}, the string form, parsing, equality and Rest.li coercion
 * behave the same for every built-in value, and malformed envs are still rejected.
 */
public class FabricTypeUrnLegacyApiTest {

  private static final DataPlatformUrn HIVE = new DataPlatformUrn("hive");

  /** One URN class: how to build it the old way, its expected string, and its old accessors. */
  private static final class Case {
    final String name;
    final Function<FabricType, Urn> build;
    final Function<FabricType, String> expectedString;
    final ThrowingParser parse;
    final Function<Urn, FabricType> origin;
    final Class<? extends Urn> urnClass;

    Case(
        String name,
        Function<FabricType, Urn> build,
        Function<FabricType, String> expectedString,
        ThrowingParser parse,
        Function<Urn, FabricType> origin,
        Class<? extends Urn> urnClass) {
      this.name = name;
      this.build = build;
      this.expectedString = expectedString;
      this.parse = parse;
      this.origin = origin;
      this.urnClass = urnClass;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  private interface ThrowingParser {
    Urn parse(String raw) throws URISyntaxException;
  }

  @DataProvider(name = "cases")
  public Object[][] cases() {
    Case[] cases = {
      new Case(
          "dataset",
          env -> new DatasetUrn(HIVE, "db.t", env),
          env -> "urn:li:dataset:(urn:li:dataPlatform:hive,db.t," + env.name() + ")",
          DatasetUrn::createFromString,
          urn -> ((DatasetUrn) urn).getOriginEntity(),
          DatasetUrn.class),
      new Case(
          "mlModel",
          env -> new MLModelUrn(HIVE, "model", env),
          env -> "urn:li:mlModel:(urn:li:dataPlatform:hive,model," + env.name() + ")",
          MLModelUrn::createFromString,
          urn -> ((MLModelUrn) urn).getOriginEntity(),
          MLModelUrn.class),
      new Case(
          "mlModelGroup",
          env -> new MlModelGroupUrn(HIVE, "group", env),
          env -> "urn:li:mlModelGroup:(urn:li:dataPlatform:hive,group," + env.name() + ")",
          MlModelGroupUrn::createFromString,
          urn -> ((MlModelGroupUrn) urn).getOriginEntity(),
          MlModelGroupUrn.class),
      new Case(
          "dataProcess",
          env -> new DataProcessUrn("airflow", "proc", env),
          env -> "urn:li:dataProcess:(airflow,proc," + env.name() + ")",
          DataProcessUrn::createFromString,
          urn -> ((DataProcessUrn) urn).getOriginEntity(),
          DataProcessUrn.class),
    };
    return Arrays.stream(cases).map(c -> new Object[] {c}).toArray(Object[][]::new);
  }

  @Test(dataProvider = "cases")
  public void testEveryBuiltInBehavesAsBefore(Case c) throws Exception {
    for (FabricType env : builtIns()) {
      Urn built = c.build.apply(env);
      String expected = c.expectedString.apply(env);

      assertEquals(built.toString(), expected);
      assertSame(c.origin.apply(built), env);

      Urn parsed = c.parse.parse(expected);
      assertEquals(parsed, built);
      assertEquals(parsed.hashCode(), built.hashCode());
      assertSame(c.origin.apply(parsed), env);

      // Rest.li reads and writes typed URN fields through the registered coercer.
      assertEquals(DataTemplateUtil.coerceOutput(expected, c.urnClass), built);
      assertEquals(DataTemplateUtil.coerceInput(built, (Class) c.urnClass, String.class), expected);
    }
  }

  @Test(dataProvider = "cases")
  public void testMalformedEnvStillRejected(Case c) {
    for (String bad : new String[] {"prod", "MY-ENV", "1ENV"}) {
      String raw = c.expectedString.apply(FabricType.PROD).replace(",PROD)", "," + bad + ")");
      assertThrows(URISyntaxException.class, () -> c.parse.parse(raw));
    }
  }

  @Test
  public void testDatasetFieldUrnBehavesAsBefore() throws Exception {
    DatasetFieldUrn field = new DatasetFieldUrn("hive", "db.t", FabricType.PROD, "col");

    assertEquals(
        field.toString(),
        "urn:li:datasetField:(urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD),col)");
    assertSame(field.getDatasetEntity().getOriginEntity(), FabricType.PROD);
    assertEquals(DatasetFieldUrn.createFromString(field.toString()), field);
  }

  @Test
  public void testToDatasetUrnBehavesAsBefore() {
    for (FabricType env : builtIns()) {
      DatasetUrn expected = new DatasetUrn(HIVE, "db.t", env);
      assertEquals(UrnUtils.toDatasetUrn("hive", "db.t", env.name()), expected);
      assertEquals(UrnUtils.toDatasetUrn("hive", "db.t", env.name().toLowerCase()), expected);
    }
    assertThrows(
        IllegalArgumentException.class, () -> UrnUtils.toDatasetUrn("hive", "db.t", "my-env"));
  }

  private static FabricType[] builtIns() {
    return Arrays.stream(FabricType.values())
        .filter(env -> env != FabricType.$UNKNOWN)
        .toArray(FabricType[]::new);
  }
}
