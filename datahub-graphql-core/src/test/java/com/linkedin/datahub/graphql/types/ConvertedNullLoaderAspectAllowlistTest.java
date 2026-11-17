package com.linkedin.datahub.graphql.types;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.types.corpgroup.CorpGroupType;
import com.linkedin.datahub.graphql.types.corpuser.CorpUserType;
import com.linkedin.datahub.graphql.types.dataplatform.DataPlatformType;
import com.linkedin.datahub.graphql.types.mlmodel.MLFeatureTableType;
import com.linkedin.datahub.graphql.types.mlmodel.MLFeatureType;
import com.linkedin.datahub.graphql.types.mlmodel.MLModelGroupType;
import com.linkedin.datahub.graphql.types.mlmodel.MLModelType;
import com.linkedin.datahub.graphql.types.mlmodel.MLPrimaryKeyType;
import com.linkedin.datahub.graphql.types.tag.TagType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Guards loaders that previously passed {@code null} (fetch every aspect) to {@code batchGetV2}.
 * Their explicit fallback allowlists must cover every aspect their mapper attempts to read.
 */
public class ConvertedNullLoaderAspectAllowlistTest {

  @DataProvider
  public Object[][] convertedTypes() {
    return new Object[][] {
      {
        "CorpUser",
        "corpuser",
        "urn:li:corpuser:allowlist-test",
        (Function<EntityClient, LoadableType<?, String>>)
            client -> new CorpUserType(client, mock(FeatureFlags.class))
      },
      {
        "CorpGroup",
        "corpGroup",
        "urn:li:corpGroup:allowlist-test",
        (Function<EntityClient, LoadableType<?, String>>) CorpGroupType::new
      },
      {
        "Tag",
        "tag",
        "urn:li:tag:allowlist-test",
        (Function<EntityClient, LoadableType<?, String>>) TagType::new
      },
      {
        "DataPlatform",
        "dataPlatform",
        "urn:li:dataPlatform:mysql",
        (Function<EntityClient, LoadableType<?, String>>) DataPlatformType::new
      },
      {
        "MLModel",
        "mlModel",
        "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,model,PROD)",
        (Function<EntityClient, LoadableType<?, String>>) MLModelType::new
      },
      {
        "MLModelGroup",
        "mlModelGroup",
        "urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,group,PROD)",
        (Function<EntityClient, LoadableType<?, String>>) MLModelGroupType::new
      },
      {
        "MLFeature",
        "mlFeature",
        "urn:li:mlFeature:(namespace,feature)",
        (Function<EntityClient, LoadableType<?, String>>) MLFeatureType::new
      },
      {
        "MLPrimaryKey",
        "mlPrimaryKey",
        "urn:li:mlPrimaryKey:(namespace,primary-key)",
        (Function<EntityClient, LoadableType<?, String>>) MLPrimaryKeyType::new
      },
      {
        "MLFeatureTable",
        "mlFeatureTable",
        "urn:li:mlFeatureTable:(urn:li:dataPlatform:feast,feature-table)",
        (Function<EntityClient, LoadableType<?, String>>) MLFeatureTableType::new
      }
    };
  }

  @Test(dataProvider = "convertedTypes")
  @SuppressWarnings("unchecked")
  public void testFallbackAllowlistCoversMapperAspectReads(
      String graphTypeName,
      String entityName,
      String urnString,
      Function<EntityClient, LoadableType<?, String>> typeFactory)
      throws Exception {
    EntityClient client = mock(EntityClient.class);
    EnvelopedAspectMap recordingAspectMap = mock(EnvelopedAspectMap.class);
    Urn urn = Urn.createFromString(urnString);
    EntityResponse entityResponse = mock(EntityResponse.class);
    when(entityResponse.getEntityName()).thenReturn(entityName);
    when(entityResponse.getUrn()).thenReturn(urn);
    when(entityResponse.getAspects()).thenReturn(recordingAspectMap);

    when(client.batchGetV2(any(), eq(entityName), any(), any()))
        .thenReturn(Map.of(urn, entityResponse));

    QueryContext context = getMockAllowContext();

    LoadableType<?, String> type = typeFactory.apply(client);
    type.batchLoad(List.of(urnString), context);

    ArgumentCaptor<Set<String>> allowlistCaptor = ArgumentCaptor.forClass(Set.class);
    Mockito.verify(client).batchGetV2(any(), eq(entityName), any(), allowlistCaptor.capture());
    Set<String> allowlist = allowlistCaptor.getValue();

    Set<String> mapperReads =
        Mockito.mockingDetails(recordingAspectMap).getInvocations().stream()
            .filter(
                invocation ->
                    invocation.getMethod().getName().equals("containsKey")
                        || invocation.getMethod().getName().equals("get")
                        || invocation.getMethod().getName().equals("getOrDefault"))
            .filter(invocation -> invocation.getArguments().length > 0)
            .map(invocation -> invocation.getArgument(0))
            .filter(String.class::isInstance)
            .map(String.class::cast)
            .collect(Collectors.toSet());

    assertTrue(
        allowlist.containsAll(mapperReads),
        graphTypeName
            + " fallback allowlist is missing mapper-read aspects: "
            + difference(mapperReads, allowlist)
            + "; mapper reads="
            + mapperReads
            + ", allowlist="
            + allowlist);
  }

  private static Set<String> difference(Set<String> left, Set<String> right) {
    return left.stream().filter(value -> !right.contains(value)).collect(Collectors.toSet());
  }
}
