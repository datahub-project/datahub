package com.linkedin.metadata.kafka.hook;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.aspect.DatasetAspectArray;
import com.linkedin.metadata.graph.Edge;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import com.linkedin.util.Pair;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class UpdateIndicesHookTest {

  @BeforeMethod
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class.getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testEdgeConstruction() throws IOException, URISyntaxException {
    // Get entity registry
    final EntityRegistry entityRegistry = buildRegistry();
    final EntitySpec entitySpec = entityRegistry.getEntitySpec("dataset");

    final DatasetSnapshot datasetSnapshot = new DatasetSnapshot();
    final DatasetUrn urn =
        DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,test-rollback,PROD)");
    datasetSnapshot.setUrn(urn);

    final GlobalTags tags = new GlobalTags()
        .setTags(new TagAssociationArray(ImmutableList.of(
            new TagAssociation().setTag(new TagUrn("test"))))
        );

    final DatasetAspect datasetAspect = new DatasetAspect();
    datasetAspect.setGlobalTags(tags);

    final Ownership owner = new Ownership();
    final Owner userOwner = new Owner().setOwner(Urn.createFromString("urn:li:corpuser:user1")).setType(OwnershipType.DATAOWNER);
    owner.setOwners(new OwnerArray(ImmutableList.of(userOwner)));
    datasetAspect.setOwnership(owner);

    final DatasetAspectArray datasetAspects = new DatasetAspectArray();
    datasetAspects.add(datasetAspect);
    datasetSnapshot.setAspects(datasetAspects);

    Pair<List<Edge>, Set<String>> output =
        getEdgesAndRelationshipTypesFromAspect(urn, entitySpec.getAspectSpec("ownership"), owner);
    System.out.println(entityRegistry);
  }

  private Pair<List<Edge>, Set<String>> getEdgesAndRelationshipTypesFromAspect(Urn urn, AspectSpec aspectSpec, RecordTemplate aspect) {
    final Set<String> relationshipTypesBeingAdded = new HashSet<>();
    final List<Edge> edgesToAdd = new ArrayList<>();

    Map<RelationshipFieldSpec, List<Object>> extractedFields =
        FieldExtractor.extractFields(aspect, aspectSpec.getRelationshipFieldSpecs());

    for (Map.Entry<RelationshipFieldSpec, List<Object>> entry : extractedFields.entrySet()) {
      relationshipTypesBeingAdded.add(entry.getKey().getRelationshipName());
      for (Object fieldValue : entry.getValue()) {
        try {
          edgesToAdd.add(
              new Edge(urn, Urn.createFromString(fieldValue.toString()), entry.getKey().getRelationshipName(),
                  aspectSpec.getName(), entry.getKey().getPath()));
        } catch (URISyntaxException e) {
          System.out.println("Invalid destination urn: "+ e.getLocalizedMessage());
        }
      }
    }
    return Pair.of(edgesToAdd, relationshipTypesBeingAdded);
  }

  private EntityRegistry buildRegistry() throws FileNotFoundException {
    ConfigEntityRegistry configEntityRegistry = new ConfigEntityRegistry(new FileInputStream("/Users/pedro/dev/oss/personal/datahub/metadata-models/src/main/resources/entity-registry.yml")
        );
    try {
      return new MergedEntityRegistry(SnapshotEntityRegistry.getInstance())
          .apply(configEntityRegistry);
    } catch (EntityRegistryException e) {
      throw new RuntimeException(e);
    }
  }

}