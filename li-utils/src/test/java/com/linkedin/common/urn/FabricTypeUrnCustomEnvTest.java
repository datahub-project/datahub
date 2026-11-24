package com.linkedin.common.urn;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.FabricType;
import com.linkedin.data.template.DataTemplateUtil;
import java.net.URISyntaxException;
import java.util.List;
import org.testng.annotations.Test;

/** C2: URN classes keep a custom env exactly; the FabricType view of it is $UNKNOWN. */
public class FabricTypeUrnCustomEnvTest {

  private static final DataPlatformUrn HIVE = new DataPlatformUrn("hive");

  @Test
  public void testDatasetUrnKeepsCustomEnv() throws Exception {
    DatasetUrn urn = new DatasetUrn(HIVE, "db.t", "MY_ENV");

    assertEquals(urn.toString(), "urn:li:dataset:(urn:li:dataPlatform:hive,db.t,MY_ENV)");
    assertEquals(urn.getEnv(), "MY_ENV");
    assertSame(urn.getOriginEntity(), FabricType.$UNKNOWN);

    DatasetUrn parsed = DatasetUrn.createFromString(urn.toString());
    assertEquals(parsed, urn);
    assertEquals(parsed.getEnv(), "MY_ENV");
    assertEquals(DataTemplateUtil.coerceOutput(urn.toString(), DatasetUrn.class), urn);
  }

  @Test
  public void testEveryUrnClassParsesCustomEnv() throws Exception {
    List<String> raws =
        List.of(
            "urn:li:mlModel:(urn:li:dataPlatform:hive,m,MY_ENV)",
            "urn:li:mlModelGroup:(urn:li:dataPlatform:hive,g,MY_ENV)",
            "urn:li:dataProcess:(airflow,p,MY_ENV)");

    MLModelUrn model = MLModelUrn.createFromString(raws.get(0));
    MlModelGroupUrn group = MlModelGroupUrn.createFromString(raws.get(1));
    DataProcessUrn process = DataProcessUrn.createFromString(raws.get(2));

    assertEquals(model.getEnv(), "MY_ENV");
    assertEquals(group.getEnv(), "MY_ENV");
    assertEquals(process.getEnv(), "MY_ENV");
    assertSame(model.getOriginEntity(), FabricType.$UNKNOWN);
    assertEquals(model.toString(), raws.get(0));
    assertEquals(group.toString(), raws.get(1));
    assertEquals(process.toString(), raws.get(2));
  }

  @Test
  public void testStringConstructorMatchesEnumConstructorForBuiltIns() {
    assertEquals(
        new DatasetUrn(HIVE, "db.t", "PROD"), new DatasetUrn(HIVE, "db.t", FabricType.PROD));
    assertSame(new DatasetUrn(HIVE, "db.t", "PROD").getOriginEntity(), FabricType.PROD);
    assertEquals(new DatasetUrn(HIVE, "db.t", FabricType.DEV).getEnv(), "DEV");
  }

  @Test
  public void testStringConstructorRejectsMalformedEnv() {
    assertThrows(IllegalArgumentException.class, () -> new DatasetUrn(HIVE, "db.t", "my_env"));
    assertThrows(IllegalArgumentException.class, () -> new MLModelUrn(HIVE, "m", "MY-ENV"));
    assertThrows(
        URISyntaxException.class,
        () -> DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,t,my_env)"));
  }

  @Test
  public void testDatasetFieldUrnAndUrnUtils() throws Exception {
    DatasetFieldUrn field = new DatasetFieldUrn("hive", "db.t", "MY_ENV", "col");
    assertEquals(field.getDatasetEntity().getEnv(), "MY_ENV");
    assertEquals(DatasetFieldUrn.createFromString(field.toString()), field);

    assertEquals(UrnUtils.toDatasetUrn("hive", "db.t", "my_env").getEnv(), "MY_ENV");
  }
}
