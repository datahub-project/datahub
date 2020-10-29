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
import com.linkedin.dataset.DatasetDeprecation;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.search.DatasetDocument;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import com.linkedin.schema.SchemaMetadata;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.*;


public class DatasetIndexBuilderTest {
  private final DatasetIndexBuilder _indexBuilder = new DatasetIndexBuilder();

  @Test
  public void datasetPropertiesSetsDescription() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn("foo"), "bar", FabricType.PROD);
    final DatasetProperties datasetProperties = new DatasetProperties().setDescription("baz");
    final DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, datasetProperties)));
    final DatasetDocument expectedDocument1 = new DatasetDocument().setUrn(datasetUrn).setDescription("baz");
    final DatasetDocument expectedDocument2 = new DatasetDocument().setUrn(datasetUrn)
        .setBrowsePaths(new StringArray("/prod/foo/bar"))
        .setOrigin(FabricType.PROD)
        .setName("bar")
        .setPlatform("foo");

    // when
    final List<DatasetDocument> actualDocs = _indexBuilder.getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void datasetPropertiesNoDescription() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn("foo"), "bar", FabricType.PROD);
    final DatasetProperties datasetProperties = new DatasetProperties();
    final DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, datasetProperties)));
    final DatasetDocument expectedDocument1 = new DatasetDocument().setUrn(datasetUrn);
    final DatasetDocument expectedDocument2 = new DatasetDocument().setUrn(datasetUrn)
        .setBrowsePaths(new StringArray("/prod/foo/bar"))
        .setOrigin(FabricType.PROD)
        .setName("bar")
        .setPlatform("foo");

    // when
    final List<DatasetDocument> actualDocs = new DatasetIndexBuilder().getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void datasetDeprecation() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn("foo"), "bar", FabricType.PROD);
    final DatasetDeprecation datasetDeprecation = new DatasetDeprecation().setDeprecated(true);
    final DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, datasetDeprecation)));
    final DatasetDocument expectedDocument1 = new DatasetDocument().setUrn(datasetUrn).setDeprecated(true);
    final DatasetDocument expectedDocument2 = new DatasetDocument().setUrn(datasetUrn)
        .setBrowsePaths(new StringArray("/prod/foo/bar"))
        .setOrigin(FabricType.PROD)
        .setName("bar")
        .setPlatform("foo");

    // when
    final List<DatasetDocument> actualDocs = new DatasetIndexBuilder().getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void datasetDeprecationClearDeprecation() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn("foo"), "bar", FabricType.PROD);
    final DatasetDeprecation datasetDeprecation = new DatasetDeprecation().setDeprecated(false);
    final DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, datasetDeprecation)));
    final DatasetDocument expectedDocument1 = new DatasetDocument().setUrn(datasetUrn).setDeprecated(false);
    final DatasetDocument expectedDocument2 = new DatasetDocument().setUrn(datasetUrn)
        .setBrowsePaths(new StringArray("/prod/foo/bar"))
        .setOrigin(FabricType.PROD)
        .setName("bar")
        .setPlatform("foo");

    // when
    final List<DatasetDocument> actualDocs = new DatasetIndexBuilder().getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void ownership() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn("foo"), "bar", FabricType.PROD);
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
        .setName("bar")
        .setPlatform("foo");

    // when
    final List<DatasetDocument> actualDocs = new DatasetIndexBuilder().getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void ownershipIgnoresNonCorpUsers() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn("foo"), "bar", FabricType.PROD);
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
        .setName("bar")
        .setPlatform("foo");

    // when
    final List<DatasetDocument> actualDocs = new DatasetIndexBuilder().getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void schemaMetadata() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn("foo"), "bar", FabricType.PROD);
    final SchemaMetadata schemaMetadata = new SchemaMetadata();
    final DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, schemaMetadata)));
    final DatasetDocument expectedDocument1 = new DatasetDocument().setUrn(datasetUrn).setHasSchema(true);
    final DatasetDocument expectedDocument2 = new DatasetDocument().setUrn(datasetUrn)
        .setBrowsePaths(new StringArray("/prod/foo/bar"))
        .setOrigin(FabricType.PROD)
        .setName("bar")
        .setPlatform("foo");

    // when
    final List<DatasetDocument> actualDocs = new DatasetIndexBuilder().getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void removedStatus() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn("foo"), "bar", FabricType.PROD);
    final Status status = new Status().setRemoved(true);
    final DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, status)));
    final DatasetDocument expectedDocument1 = new DatasetDocument().setUrn(datasetUrn)
        .setRemoved(true);
    final DatasetDocument expectedDocument2 = new DatasetDocument().setUrn(datasetUrn)
        .setBrowsePaths(new StringArray("/prod/foo/bar"))
        .setOrigin(FabricType.PROD)
        .setName("bar")
        .setPlatform("foo");

    // when
    final List<DatasetDocument> actualDocs = new DatasetIndexBuilder().getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void nonRemovedStatus() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn("foo"), "bar", FabricType.PROD);
    final Status status = new Status().setRemoved(false);
    final DatasetSnapshot datasetSnapshot = ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn,
        Collections.singletonList(ModelUtils.newAspectUnion(DatasetAspect.class, status)));
    final DatasetDocument expectedDocument1 = new DatasetDocument().setUrn(datasetUrn).setRemoved(false);
    final DatasetDocument expectedDocument2 = new DatasetDocument().setUrn(datasetUrn)
        .setBrowsePaths(new StringArray("/prod/foo/bar"))
        .setOrigin(FabricType.PROD)
        .setName("bar")
        .setPlatform("foo");

    // when
    final List<DatasetDocument> actualDocs = new DatasetIndexBuilder().getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }

  @Test
  public void upstreamLineage() {
    // given
    final DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn("foo"), "bar", FabricType.PROD);
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
        .setName("bar")
        .setPlatform("foo");

    // when
    final List<DatasetDocument> actualDocs = new DatasetIndexBuilder().getDocumentsToUpdate(datasetSnapshot);

    // then
    assertThat(actualDocs).containsExactly(expectedDocument1, expectedDocument2);
  }
}