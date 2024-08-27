package com.linkedin.metadata.entity;

import com.datahub.common.DummySnapshot;
import com.datahub.util.RecordUtils;
import com.datahub.util.exception.InvalidSchemaException;
import com.datahub.util.validator.EntityValidator;
import com.datahub.util.validator.SnapshotValidator;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.TemplateOutputCastException;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.data.template.WrappingArrayTemplate;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.reflections.Reflections;
import org.reflections.scanners.Scanner;

public class NewModelUtils {
  private static final ClassLoader CLASS_LOADER = DummySnapshot.class.getClassLoader();

  private NewModelUtils() {}

  public static <T extends DataTemplate> String getAspectName(@Nonnull Class<T> aspectClass) {
    return aspectClass.getCanonicalName();
  }

  @Nonnull
  public static Class<? extends RecordTemplate> getAspectClass(@Nonnull String aspectName) {
    return getClassFromName(aspectName, RecordTemplate.class);
  }

  @Nonnull
  public static <T> Class<? extends T> getClassFromName(
      @Nonnull String className, @Nonnull Class<T> parentClass) {
    try {
      return CLASS_LOADER.loadClass(className).asSubclass(parentClass);
    } catch (ClassNotFoundException var3) {
      throw new RuntimeException(var3);
    }
  }

  @Nonnull
  public static <SNAPSHOT extends RecordTemplate>
      List<Pair<String, RecordTemplate>> getAspectsFromSnapshot(@Nonnull SNAPSHOT snapshot) {
    SnapshotValidator.validateSnapshotSchema(snapshot.getClass());
    return getAspects(snapshot);
  }

  @Nonnull
  private static List<Pair<String, RecordTemplate>> getAspects(@Nonnull RecordTemplate snapshot) {
    Class<? extends WrappingArrayTemplate> clazz = getAspectsArrayClass(snapshot.getClass());
    WrappingArrayTemplate aspectArray =
        (WrappingArrayTemplate)
            RecordUtils.getRecordTemplateWrappedField(snapshot, "aspects", clazz);
    List<Pair<String, RecordTemplate>> aspects = new ArrayList();
    aspectArray.forEach(
        (item) -> {
          try {
            RecordTemplate aspect =
                RecordUtils.getSelectedRecordTemplateFromUnion((UnionTemplate) item);
            String name = PegasusUtils.getAspectNameFromSchema(aspect.schema());
            aspects.add(Pair.of(name, aspect));
          } catch (InvalidSchemaException e) {
            // ignore fields that are not part of the union
          } catch (TemplateOutputCastException e) {
            // ignore fields that are not part of the union
          }
        });
    return aspects;
  }

  @Nonnull
  private static <SNAPSHOT extends RecordTemplate>
      Class<? extends WrappingArrayTemplate> getAspectsArrayClass(
          @Nonnull Class<SNAPSHOT> snapshotClass) {
    try {
      return snapshotClass
          .getMethod("getAspects")
          .getReturnType()
          .asSubclass(WrappingArrayTemplate.class);
    } catch (ClassCastException | NoSuchMethodException var2) {
      throw new RuntimeException(var2);
    }
  }

  @Nonnull
  public static Set<Class<? extends RecordTemplate>> getAllEntities() {
    return (Set)
        (new Reflections("com.linkedin.metadata.entity", new Scanner[0]))
            .getSubTypesOf(RecordTemplate.class).stream()
                .filter(EntityValidator::isValidEntitySchema)
                .collect(Collectors.toSet());
  }
}
