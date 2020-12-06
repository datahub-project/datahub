package com.linkedin.metadata.builders.search;

import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.FabricType;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.dataplatform.PlatformType;
import com.linkedin.dataset.DatasetDeprecation;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.aspect.DataPlatformAspect;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.dao.RestliRemoteDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.search.DatasetDocument;
import com.linkedin.metadata.snapshot.DataPlatformSnapshot;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import com.linkedin.restli.client.RestClient;
import com.linkedin.schema.BooleanType;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.FieldSetter;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.*;


public class DatasetIndexBuilderTest {

  public static final String DATASET_BAR = "/bar";
  public static final String PLATFORM_FOO = "foo";
  @Mock
  private RestClient mockRestClient;

  @Mock
  private RestliRemoteDAO<DataPlatformSnapshot, DataPlatformAspect, DataPlatformUrn> dataPlatformDAO;

  private DatasetIndexBuilder indexBuilder;

  @BeforeMethod
  public void setup() throws NoSuchFieldException {
    MockitoAnnotations.initMocks(this);
    Mockito.when(dataPlatformDAO.get(Mockito.eq(DataPlatformInfo.class), Mockito.eq(new DataPlatformUrn("hdfs"))))
        .thenReturn(Optional.of(
            new DataPlatformInfo().setName("hdfs").setType(PlatformType.FILE_SYSTEM).setDatasetNameDelimiter("/")));

    Mockito.when(dataPlatformDAO.get(Mockito.eq(DataPlatformInfo.class), Mockito.eq(new DataPlatformUrn("foo"))))
        .thenReturn(Optional.of(
            new DataPlatformInfo().setName("foo").setType(PlatformType.FILE_SYSTEM).setDatasetNameDelimiter("/")));

    Mockito.when(dataPlatformDAO.get(Mockito.eq(DataPlatformInfo.class), Mockito.eq(new DataPlatformUrn("hive"))))
        .thenReturn(Optional.of(
            new DataPlatformInfo().setName("hive").setType(PlatformType.FILE_SYSTEM).setDatasetNameDelimiter(".")));

    indexBuilder = new DatasetIndexBuilder(mockRestClient);

    FieldSetter.setField(indexBuilder, indexBuilder.getClass().getDeclaredField("dataPlatformDAO"), dataPlatformDAO);
  }

  @Test
  public void datasetPropertiesSetsDescription() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn(PLATFORM_FOO), DATASET_BAR, FabricType.PROD);
    final DatasetProperties datasetProperties = new DatasetProperties().setDescription("baz");
    final DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, datasetProperties)));
    final DatasetDocument expectedDocument1 = new DatasetDocument().setUrn(datasetUrn).setDescription("baz");
    final DatasetDocument expectedDocument2 = new DatasetDocument().setUrn(datasetUrn)
        .setBrowsePaths(new StringArray("/prod/foo/bar"))
        .setOrigin(FabricType.PROD)
        .setName(DATASET_BAR)
        .setPlatform(PLATFORM_FOO);

    // when
    final List<DatasetDocument> actualDocs = indexBuilder.getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void browsePaths() {
    assertThat(DatasetIndexBuilder.buildBrowsePath(
        new DatasetUrn(new DataPlatformUrn("hive"), "foo.bar", FabricType.PROD))).isEqualTo("/prod/hive/foo/bar");
    assertThat(DatasetIndexBuilder.buildBrowsePath(
        new DatasetUrn(new DataPlatformUrn("hdfs"), "/foo/bar", FabricType.PROD))).isEqualTo("/prod/hdfs/foo/bar");
    assertThat(DatasetIndexBuilder.buildBrowsePath(
        new DatasetUrn(new DataPlatformUrn("hdfs"), "/foo/bar.baz", FabricType.PROD))).isEqualTo(
        "/prod/hdfs/foo/bar.baz");
  }

  @Test
  public void datasetPropertiesNoDescription() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn(PLATFORM_FOO), DATASET_BAR, FabricType.PROD);
    final DatasetProperties datasetProperties = new DatasetProperties();
    final DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, datasetProperties)));
    final DatasetDocument expectedDocument1 = new DatasetDocument().setUrn(datasetUrn);
    final DatasetDocument expectedDocument2 = new DatasetDocument().setUrn(datasetUrn)
        .setBrowsePaths(new StringArray("/prod/foo/bar"))
        .setOrigin(FabricType.PROD)
        .setName(DATASET_BAR)
        .setPlatform(PLATFORM_FOO);

    // when
    final List<DatasetDocument> actualDocs = indexBuilder.getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void datasetDeprecation() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn(PLATFORM_FOO), DATASET_BAR, FabricType.PROD);
    final DatasetDeprecation datasetDeprecation = new DatasetDeprecation().setDeprecated(true);
    final DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, datasetDeprecation)));
    final DatasetDocument expectedDocument1 = new DatasetDocument().setUrn(datasetUrn).setDeprecated(true);
    final DatasetDocument expectedDocument2 = new DatasetDocument().setUrn(datasetUrn)
        .setBrowsePaths(new StringArray("/prod/foo/bar"))
        .setOrigin(FabricType.PROD)
        .setName(DATASET_BAR)
        .setPlatform(PLATFORM_FOO);

    // when
    final List<DatasetDocument> actualDocs = indexBuilder.getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void datasetDeprecationClearDeprecation() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn(PLATFORM_FOO), DATASET_BAR, FabricType.PROD);
    final DatasetDeprecation datasetDeprecation = new DatasetDeprecation().setDeprecated(false);
    final DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, datasetDeprecation)));
    final DatasetDocument expectedDocument1 = new DatasetDocument().setUrn(datasetUrn).setDeprecated(false);
    final DatasetDocument expectedDocument2 = new DatasetDocument().setUrn(datasetUrn)
        .setBrowsePaths(new StringArray("/prod/foo/bar"))
        .setOrigin(FabricType.PROD)
        .setName(DATASET_BAR)
        .setPlatform(PLATFORM_FOO);

    // when
    final List<DatasetDocument> actualDocs = indexBuilder.getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void ownership() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn(PLATFORM_FOO), DATASET_BAR, FabricType.PROD);
    final Owner owner = new Owner().setOwner(new CorpuserUrn("testUser"))
        .setSource(new OwnershipSource().setType(OwnershipSourceType.FILE_SYSTEM))
        .setType(OwnershipType.DATAOWNER);
    final Ownership ownership = new Ownership().setOwners(new OwnerArray(owner));
    final DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, ownership)));
    final DatasetDocument expectedDocument1 =
        new DatasetDocument().setUrn(datasetUrn).setHasOwners(true).setOwners(new StringArray("testUser"));
    final DatasetDocument expectedDocument2 = new DatasetDocument().setUrn(datasetUrn)
        .setBrowsePaths(new StringArray("/prod/foo/bar"))
        .setOrigin(FabricType.PROD)
        .setName(DATASET_BAR)
        .setPlatform(PLATFORM_FOO);

    // when
    final List<DatasetDocument> actualDocs = indexBuilder.getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void ownershipIgnoresNonCorpUsers() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn(PLATFORM_FOO), DATASET_BAR, FabricType.PROD);
    final Owner owner1 = new Owner().setOwner(new CorpuserUrn("testUser1"))
        .setSource(new OwnershipSource().setType(OwnershipSourceType.FILE_SYSTEM))
        .setType(OwnershipType.DATAOWNER);
    final Owner owner2 = new Owner().setOwner(new CorpuserUrn("testUser2"))
        .setSource(new OwnershipSource().setType(OwnershipSourceType.FILE_SYSTEM))
        .setType(OwnershipType.DATAOWNER);
    final Owner owner3 = new Owner().setOwner(new CorpGroupUrn("testGroup"))
        .setSource(new OwnershipSource().setType(OwnershipSourceType.FILE_SYSTEM))
        .setType(OwnershipType.DATAOWNER);
    final Ownership ownership = new Ownership().setOwners(new OwnerArray(owner1, owner2, owner3));
    final DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, ownership)));
    final DatasetDocument expectedDocument1 = new DatasetDocument().setUrn(datasetUrn)
        .setHasOwners(true)
        .setOwners(new StringArray("testUser1", "testUser2"));
    final DatasetDocument expectedDocument2 = new DatasetDocument().setUrn(datasetUrn)
        .setBrowsePaths(new StringArray("/prod/foo/bar"))
        .setOrigin(FabricType.PROD)
        .setName(DATASET_BAR)
        .setPlatform(PLATFORM_FOO);

    // when
    final List<DatasetDocument> actualDocs = indexBuilder.getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void schemaMetadata() {
    // given
    final SchemaFieldArray schemaFieldArray = new SchemaFieldArray(new SchemaField().setFieldPath("foo.bar.baz")
        .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BooleanType())))
        .setNullable(false)
        .setNativeDataType("boolean")
        .setRecursive(false));
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn(PLATFORM_FOO), DATASET_BAR, FabricType.PROD);
    final SchemaMetadata schemaMetadata = new SchemaMetadata().setFields(schemaFieldArray);
    final DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, schemaMetadata)));
    final DatasetDocument expectedDocument1 =
        new DatasetDocument().setUrn(datasetUrn).setHasSchema(true).setFieldPaths(new StringArray("foo.bar.baz"));
    final DatasetDocument expectedDocument2 = new DatasetDocument().setUrn(datasetUrn)
        .setBrowsePaths(new StringArray("/prod/foo/bar"))
        .setOrigin(FabricType.PROD)
        .setName(DATASET_BAR)
        .setPlatform(PLATFORM_FOO);

    // when
    final List<DatasetDocument> actualDocs = indexBuilder.getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void removedStatus() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn(PLATFORM_FOO), DATASET_BAR, FabricType.PROD);
    final Status status = new Status().setRemoved(true);
    final DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, status)));
    final DatasetDocument expectedDocument1 = new DatasetDocument().setUrn(datasetUrn).setRemoved(true);
    final DatasetDocument expectedDocument2 = new DatasetDocument().setUrn(datasetUrn)
        .setBrowsePaths(new StringArray("/prod/foo/bar"))
        .setOrigin(FabricType.PROD)
        .setName(DATASET_BAR)
        .setPlatform(PLATFORM_FOO);

    // when
    final List<DatasetDocument> actualDocs = indexBuilder.getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void nonRemovedStatus() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn(PLATFORM_FOO), DATASET_BAR, FabricType.PROD);
    final Status status = new Status().setRemoved(false);
    final DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, status)));
    final DatasetDocument expectedDocument1 = new DatasetDocument().setUrn(datasetUrn).setRemoved(false);
    final DatasetDocument expectedDocument2 = new DatasetDocument().setUrn(datasetUrn)
        .setBrowsePaths(new StringArray("/prod/foo/bar"))
        .setOrigin(FabricType.PROD)
        .setName(DATASET_BAR)
        .setPlatform(PLATFORM_FOO);

    // when
    final List<DatasetDocument> actualDocs = indexBuilder.getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void upstreamLineage() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn(PLATFORM_FOO), DATASET_BAR, FabricType.PROD);
    final DatasetUrn upstreamUrn1 = new DatasetUrn(new DataPlatformUrn("testPlatform"), "dataset1", FabricType.DEV);
    final DatasetUrn upstreamUrn2 = new DatasetUrn(new DataPlatformUrn("testPlatform"), "dataset2", FabricType.DEV);
    final Upstream upstream1 = new Upstream().setDataset(upstreamUrn1);
    final Upstream upstream2 = new Upstream().setDataset(upstreamUrn2);
    final UpstreamLineage upstreamLineage = new UpstreamLineage().setUpstreams(new UpstreamArray(upstream1, upstream2));
    final DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, upstreamLineage)));
    final DatasetDocument expectedDocument1 =
        new DatasetDocument().setUrn(datasetUrn).setUpstreams(new DatasetUrnArray(upstreamUrn1, upstreamUrn2));
    final DatasetDocument expectedDocument2 = new DatasetDocument().setUrn(datasetUrn)
        .setBrowsePaths(new StringArray("/prod/foo/bar"))
        .setOrigin(FabricType.PROD)
        .setName(DATASET_BAR)
        .setPlatform(PLATFORM_FOO);

    // when
    final List<DatasetDocument> actualDocs = indexBuilder.getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }
}