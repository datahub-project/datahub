package io.datahubproject.test;

import static org.mockito.Mockito.mock;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import net.datafaker.Faker;
import net.datafaker.providers.base.Animal;
import net.datafaker.providers.base.Cat;
import org.apache.commons.lang3.NotImplementedException;

public class DataGenerator {
  private static final Faker FAKER = new Faker();
  private final EntityRegistry entityRegistry;
  private final EntityService entityService;

  public DataGenerator(EntityService entityService) {
    this.entityService = entityService;
    this.entityRegistry = entityService.getEntityRegistry();
  }

  public static DataGenerator build(EntityRegistry entityRegistry) {
    EntityServiceImpl mockEntityServiceImpl =
        new EntityServiceImpl(
            mock(AspectDao.class),
            mock(EventProducer.class),
            entityRegistry,
            false,
            mock(UpdateIndicesService.class),
            mock(PreProcessHooks.class));
    return new DataGenerator(mockEntityServiceImpl);
  }

  public Stream<List<MetadataChangeProposal>> generateDatasets() {
    return generateMCPs("dataset", 10, List.of());
  }

  public List<MetadataChangeProposal> generateTags(long count) {
    return generateMCPs("tag", count, List.of()).findFirst().get();
  }

  public Stream<List<MetadataChangeProposal>> generateMCPs(
      String entityName, long count, List<String> aspects) {
    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);

    // Prevent duplicate tags and terms generated as secondary entities
    Set<Urn> secondaryUrns = new HashSet<>();

    return LongStream.range(0, count)
        .mapToObj(
            idx -> {
              RecordTemplate key = randomKeyAspect(entitySpec);
              MetadataChangeProposal mcp = new MetadataChangeProposal();
              mcp.setEntityType(entitySpec.getName());
              mcp.setAspectName(entitySpec.getKeyAspectName());
              mcp.setAspect(GenericRecordUtils.serializeAspect(key));
              mcp.setEntityUrn(EntityKeyUtils.convertEntityKeyToUrn(key, entityName));
              mcp.setChangeType(ChangeType.UPSERT);
              return mcp;
            })
        .flatMap(
            mcp -> {
              // Expand with additional random aspects
              List<MetadataChangeProposal> additionalMCPs = new LinkedList<>();

              for (String aspectName : aspects) {
                AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
                if (aspectSpec == null) {
                  throw new IllegalStateException(
                      "Aspect " + aspectName + " not found for entity " + entityName);
                }

                RecordTemplate aspect =
                    randomAspectGenerators
                        .getOrDefault(aspectName, DataGenerator::defaultRandomAspect)
                        .apply(entitySpec, aspectSpec);

                // Maybe generate nested entities at the same time, like globalTags/glossaryTerms
                List<MetadataChangeProposal> secondaryEntities =
                    nestedRandomAspectGenerators
                        .getOrDefault(aspectSpec.getName(), (a, c) -> List.of())
                        .apply(aspect, 5)
                        .stream()
                        .filter(
                            secondaryMCP -> {
                              if (!secondaryUrns.contains(secondaryMCP.getEntityUrn())) {
                                secondaryUrns.add(secondaryMCP.getEntityUrn());
                                return true;
                              }
                              return false;
                            })
                        .collect(Collectors.toList());
                additionalMCPs.addAll(secondaryEntities);

                MetadataChangeProposal additionalMCP = new MetadataChangeProposal();
                additionalMCP.setEntityType(entitySpec.getName());
                additionalMCP.setAspectName(aspectName);
                additionalMCP.setAspect(GenericRecordUtils.serializeAspect(aspect));
                additionalMCP.setEntityUrn(mcp.getEntityUrn());
                additionalMCP.setChangeType(ChangeType.UPSERT);

                additionalMCPs.add(additionalMCP);
              }

              return Stream.concat(Stream.of(mcp), additionalMCPs.stream());
            })
        .map(
            mcp -> {
              // Expand with default aspects per normal
              return Stream.concat(
                      Stream.of(mcp),
                      AspectUtils.getAdditionalChanges(mcp, entityService, true).stream())
                  .collect(Collectors.toList());
            });
  }

  public static Map<String, BiFunction<EntitySpec, AspectSpec, ? extends RecordTemplate>>
      randomAspectGenerators =
          Map.of(
              "glossaryTermInfo",
              (e, a) -> {
                GlossaryTermInfo glossaryTermInfo = (GlossaryTermInfo) defaultRandomAspect(e, a);
                glossaryTermInfo.setName(normalize(FAKER.company().buzzword()));
                return glossaryTermInfo;
              });

  public Map<String, BiFunction<RecordTemplate, Integer, List<MetadataChangeProposal>>>
      nestedRandomAspectGenerators =
          Map.of(
              "globalTags",
                  (aspect, count) -> {
                    try {
                      List<MetadataChangeProposal> tags = generateTags(count);
                      Method setTagsMethod =
                          aspect.getClass().getMethod("setTags", TagAssociationArray.class);
                      TagAssociationArray tagAssociations = new TagAssociationArray();
                      tagAssociations.addAll(
                          tags.stream()
                              .map(
                                  tagMCP -> {
                                    try {
                                      return new TagAssociation()
                                          .setTag(TagUrn.createFromUrn(tagMCP.getEntityUrn()));
                                    } catch (URISyntaxException e) {
                                      throw new RuntimeException(e);
                                    }
                                  })
                              .collect(Collectors.toList()));
                      setTagsMethod.invoke(aspect, tagAssociations);
                      return tags;
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  },
              "glossaryTerms",
                  (aspect, count) -> {
                    try {
                      List<MetadataChangeProposal> terms =
                          generateMCPs("glossaryTerm", count, List.of("glossaryTermInfo"))
                              .map(mcps -> mcps.get(0))
                              .collect(Collectors.toList());
                      Method setTermsMethod =
                          aspect
                              .getClass()
                              .getMethod("setTerms", GlossaryTermAssociationArray.class);
                      GlossaryTermAssociationArray termAssociations =
                          new GlossaryTermAssociationArray();
                      termAssociations.addAll(
                          terms.stream()
                              .map(
                                  termMCP -> {
                                    try {
                                      return new GlossaryTermAssociation()
                                          .setUrn(
                                              GlossaryTermUrn.createFromUrn(
                                                  termMCP.getEntityUrn()));
                                    } catch (URISyntaxException e) {
                                      throw new RuntimeException(e);
                                    }
                                  })
                              .collect(Collectors.toList()));
                      setTermsMethod.invoke(aspect, termAssociations);
                      return terms;
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  });

  private static RecordTemplate defaultRandomAspect(
      @Nonnull EntitySpec entitySpec, @Nonnull AspectSpec aspectSpec) {
    Class<RecordTemplate> aspectClass = aspectSpec.getDataTemplateClass();
    try {
      Object aspect = aspectClass.getDeclaredConstructor().newInstance();

      List<Method> booleanMethods =
          Arrays.stream(aspectClass.getMethods())
              .filter(
                  m ->
                      m.getName().startsWith("set")
                          && m.getParameterCount() == 1
                          && m.getParameterTypes()[0] == Boolean.class)
              .collect(Collectors.toList());

      for (Method boolMethod : booleanMethods) {
        boolMethod.invoke(aspect, FAKER.random().nextBoolean());
      }

      List<Method> stringMethods =
          Arrays.stream(aspectClass.getMethods())
              .filter(
                  m ->
                      m.getName().startsWith("set")
                          && m.getParameterCount() == 1
                          && m.getParameterTypes()[0] == String.class)
              .collect(Collectors.toList());

      for (Method stringMethod : stringMethods) {
        String value;
        switch (aspectSpec.getName() + "_" + stringMethod.getName()) {
          default:
            value = FAKER.lorem().characters(8, 16, false);
            break;
        }

        // global
        if (stringMethod.getName().toLowerCase().contains("description")
            || stringMethod.getName().toLowerCase().contains("definition")) {
          value = FAKER.lorem().paragraph();
        }

        stringMethod.invoke(aspect, value);
      }

      List<Method> enumMethods =
          Arrays.stream(aspectClass.getMethods())
              .filter(
                  m ->
                      m.getName().startsWith("set")
                          && m.getParameterCount() == 1
                          && m.getParameterTypes()[0].isEnum())
              .collect(Collectors.toList());

      for (Method enumMethod : enumMethods) {
        Object[] enumClass = enumMethod.getParameterTypes()[0].getEnumConstants();
        // Excluding $UNKNOWNs
        enumMethod.invoke(aspect, enumClass[FAKER.random().nextInt(0, enumClass.length - 2)]);
      }

      // auditStamp
      Arrays.stream(aspectClass.getMethods())
          .filter(
              m ->
                  m.getName().startsWith("set")
                      && m.getParameterCount() == 1
                      && m.getParameterTypes()[0] == AuditStamp.class)
          .findFirst()
          .ifPresent(
              auditStampMethod -> {
                try {
                  AuditStamp auditStamp =
                      new AuditStamp()
                          .setActor(Urn.createFromString(Constants.DATAHUB_ACTOR))
                          .setTime(System.currentTimeMillis());
                  auditStampMethod.invoke(aspect, auditStamp);
                } catch (URISyntaxException
                    | IllegalAccessException
                    | InvocationTargetException e) {
                  throw new RuntimeException(e);
                }
              });

      return aspectClass.cast(aspect);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static RecordTemplate randomKeyAspect(EntitySpec entitySpec) {
    Class<RecordTemplate> keyClass = entitySpec.getKeyAspectSpec().getDataTemplateClass();
    try {
      Object key = keyClass.getDeclaredConstructor().newInstance();

      List<Method> stringMethods =
          Arrays.stream(keyClass.getMethods())
              .filter(
                  m ->
                      m.getName().startsWith("set")
                          && m.getParameterCount() == 1
                          && m.getParameterTypes()[0] == String.class)
              .collect(Collectors.toList());

      switch (entitySpec.getName()) {
        case "tag":
          stringMethods.get(0).invoke(key, normalize(FAKER.marketing().buzzwords()));
          break;
        case "glossaryTerm":
          stringMethods.get(0).invoke(key, normalize(UUID.randomUUID().toString()));
          break;
        case "container":
          stringMethods.get(0).invoke(key, FAKER.examplify("b5e95fce839e7d78151ed7e0a7420d84"));
          break;
        default:
          switch (stringMethods.size()) {
            case 1:
              stringMethods.get(0).invoke(key, String.join(".", multiName(3)));
              break;
            case 2:
              Cat cat = FAKER.cat();
              stringMethods.get(0).invoke(key, cat.breed().toLowerCase());
              stringMethods.get(1).invoke(key, cat.name().toLowerCase());
              break;
            default:
              Animal animal = FAKER.animal();
              stringMethods.get(0).invoke(key, animal.genus().toLowerCase());
              stringMethods.get(1).invoke(key, animal.species().toLowerCase());
              stringMethods.get(2).invoke(key, animal.name().toLowerCase());
              break;
          }
          break;
      }

      List<Method> urnMethods =
          Arrays.stream(keyClass.getMethods())
              .filter(
                  m ->
                      m.getName().startsWith("set")
                          && m.getParameterCount() == 1
                          && m.getParameterTypes()[0] == Urn.class)
              .collect(Collectors.toList());

      for (Method urnMethod : urnMethods) {
        switch (entitySpec.getName()) {
          case "dataset":
            urnMethod.invoke(
                key, randomUrnLowerCase("dataPlatform", List.of(randomDataPlatform())));
            break;
          default:
            throw new NotImplementedException(entitySpec.getName());
        }
      }

      List<Method> enumMethods =
          Arrays.stream(keyClass.getMethods())
              .filter(
                  m ->
                      m.getName().startsWith("set")
                          && m.getParameterCount() == 1
                          && m.getParameterTypes()[0].isEnum())
              .collect(Collectors.toList());

      for (Method enumMethod : enumMethods) {
        Object[] enumClass = enumMethod.getParameterTypes()[0].getEnumConstants();
        // Excluding $UNKNOWNs
        enumMethod.invoke(key, enumClass[FAKER.random().nextInt(0, enumClass.length - 2)]);
      }

      return keyClass.cast(key);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static List<String> multiName(int size) {
    switch (size) {
      case 1:
        return Stream.of(FAKER.marketing().buzzwords())
            .map(String::toLowerCase)
            .collect(Collectors.toList());
      case 2:
        Cat cat = FAKER.cat();
        return Stream.of(cat.breed(), cat.name())
            .map(String::toLowerCase)
            .collect(Collectors.toList());
      case 3:
        Animal animal = FAKER.animal();
        return Stream.of(animal.genus(), animal.species(), animal.name())
            .map(String::toLowerCase)
            .collect(Collectors.toList());
      default:
        return IntStream.range(0, size)
            .mapToObj(i -> FAKER.expression("#{numerify 'test####'}"))
            .collect(Collectors.toList());
    }
  }

  private static Urn randomUrnLowerCase(String entityType, List<String> tuple) {
    return Urn.createFromTuple(
        entityType, tuple.stream().map(DataGenerator::normalize).collect(Collectors.toList()));
  }

  private static String normalize(String input) {
    return input.toLowerCase().replaceAll("\\W+", "_");
  }

  private static String randomDataPlatform() {
    String[] platforms = {
      "ambry",
      "bigquery",
      "couchbase",
      "druid",
      "external",
      "feast",
      "glue",
      "hdfs",
      "hive",
      "kafka",
      "kusto",
      "looker",
      "mongodb",
      "mssql",
      "mysql",
      "oracle",
      "pinot",
      "postgres",
      "presto",
      "redshift",
      "s3",
      "sagemaker",
      "snowflake",
      "teradata",
      "voldemort"
    };

    return platforms[FAKER.random().nextInt(0, platforms.length - 1)];
  }
}
