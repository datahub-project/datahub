package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.linkedin.common.FabricType;
import com.linkedin.container.ContainerProperties;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.dataplatforminstance.IcebergWarehouseInfo;
import com.linkedin.metadata.key.DataProcessKey;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.key.MLModelDeploymentKey;
import com.linkedin.metadata.key.MLModelGroupKey;
import com.linkedin.metadata.key.MLModelKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Clients that never use custom environments keep the generated model API exactly as before: for
 * all 9 env / origin fields the accessors take and return {@link FabricType}, values are stored as
 * the symbol name, and JSON written before the change reads back to the same enum.
 */
public class FabricTypeLegacyModelApiTest {

  private static final class Field<T extends RecordTemplate> {
    final String name;
    final Supplier<T> create;
    final String fieldName;
    final BiConsumer<T, FabricType> set;
    final Function<T, FabricType> get;
    final Class<T> recordClass;

    Field(
        Class<T> recordClass,
        Supplier<T> create,
        String fieldName,
        BiConsumer<T, FabricType> set,
        Function<T, FabricType> get) {
      this.name = recordClass.getSimpleName() + "." + fieldName;
      this.recordClass = recordClass;
      this.create = create;
      this.fieldName = fieldName;
      this.set = set;
      this.get = get;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  @DataProvider(name = "fields")
  public Object[][] fields() {
    List<Field<?>> fields =
        List.of(
            new Field<>(
                DatasetKey.class,
                DatasetKey::new,
                "origin",
                DatasetKey::setOrigin,
                DatasetKey::getOrigin),
            new Field<>(
                MLModelKey.class,
                MLModelKey::new,
                "origin",
                MLModelKey::setOrigin,
                MLModelKey::getOrigin),
            new Field<>(
                MLModelGroupKey.class,
                MLModelGroupKey::new,
                "origin",
                MLModelGroupKey::setOrigin,
                MLModelGroupKey::getOrigin),
            new Field<>(
                MLModelDeploymentKey.class,
                MLModelDeploymentKey::new,
                "origin",
                MLModelDeploymentKey::setOrigin,
                MLModelDeploymentKey::getOrigin),
            new Field<>(
                DataProcessKey.class,
                DataProcessKey::new,
                "origin",
                DataProcessKey::setOrigin,
                DataProcessKey::getOrigin),
            new Field<>(
                ContainerProperties.class,
                ContainerProperties::new,
                "env",
                ContainerProperties::setEnv,
                ContainerProperties::getEnv),
            new Field<>(
                DataFlowInfo.class,
                DataFlowInfo::new,
                "env",
                DataFlowInfo::setEnv,
                DataFlowInfo::getEnv),
            new Field<>(
                DataJobInfo.class,
                DataJobInfo::new,
                "env",
                DataJobInfo::setEnv,
                DataJobInfo::getEnv),
            new Field<>(
                IcebergWarehouseInfo.class,
                IcebergWarehouseInfo::new,
                "env",
                IcebergWarehouseInfo::setEnv,
                IcebergWarehouseInfo::getEnv));
    return fields.stream().map(f -> new Object[] {f}).toArray(Object[][]::new);
  }

  @Test(dataProvider = "fields")
  public <T extends RecordTemplate> void testTypedAccessorsRoundTripEveryBuiltIn(Field<T> field) {
    for (FabricType env : builtIns()) {
      T record = field.create.get();
      field.set.accept(record, env);

      assertEquals(record.data().get(field.fieldName), env.name());
      assertSame(field.get.apply(record), env);

      T copy = RecordUtils.toRecordTemplate(field.recordClass, RecordUtils.toJsonString(record));
      assertEquals(copy, record);
      assertSame(field.get.apply(copy), env);
    }
  }

  @Test(dataProvider = "fields")
  public <T extends RecordTemplate> void testJsonWrittenBeforeTheChangeReadsBack(Field<T> field) {
    for (FabricType env : builtIns()) {
      T record =
          RecordUtils.toRecordTemplate(
              field.recordClass, "{\"" + field.fieldName + "\": \"" + env.name() + "\"}");
      assertSame(field.get.apply(record), env);
    }
  }

  @Test(dataProvider = "fields")
  public <T extends RecordTemplate> void testBuiltInValuesPassSchemaValidation(Field<T> field) {
    T record = field.create.get();
    field.set.accept(record, FabricType.PROD);
    List<Object> failures = new ArrayList<>();
    // Other required fields are absent; only failures on the env field matter here.
    RecordTemplateValidator.validate(
        record,
        result -> {
          if (String.valueOf(result).contains("/" + field.fieldName + " ")) {
            failures.add(result);
          }
        });
    assertTrue(failures.isEmpty(), failures.toString());
  }

  private static FabricType[] builtIns() {
    return Arrays.stream(FabricType.values())
        .filter(env -> env != FabricType.$UNKNOWN)
        .toArray(FabricType[]::new);
  }
}
