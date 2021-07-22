package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.Tag;
import com.linkedin.datahub.graphql.generated.MLFeature;
import com.linkedin.datahub.graphql.generated.MLFeatureTable;
import com.linkedin.datahub.graphql.generated.MLPrimaryKey;
import com.linkedin.datahub.graphql.generated.MLModel;
import com.linkedin.datahub.graphql.generated.MLModelGroup;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;


public class UrnToEntityMapper implements ModelMapper<com.linkedin.common.urn.Urn, Entity>  {
  public static final UrnToEntityMapper INSTANCE = new UrnToEntityMapper();

  public static Entity map(@Nonnull final com.linkedin.common.urn.Urn urn) {
    return INSTANCE.apply(urn);
  }

  @Override
  public Entity apply(Urn input) {
    Entity partialEntity = null;
    if (input.getEntityType().equals("dataset")) {
      partialEntity = new Dataset();
      ((Dataset) partialEntity).setUrn(input.toString());
    }
    if (input.getEntityType().equals("glossaryTerm")) {
      partialEntity = new GlossaryTerm();
      ((GlossaryTerm) partialEntity).setUrn(input.toString());
    }
    if (input.getEntityType().equals("chart")) {
      partialEntity = new Chart();
      ((Chart) partialEntity).setUrn(input.toString());
    }
    if (input.getEntityType().equals("dashboard")) {
      partialEntity = new Dashboard();
      ((Dashboard) partialEntity).setUrn(input.toString());
    }
    if (input.getEntityType().equals("dataJob")) {
      partialEntity = new DataJob();
      ((DataJob) partialEntity).setUrn(input.toString());
    }
    if (input.getEntityType().equals("dataFlow")) {
      partialEntity = new DataFlow();
      ((DataFlow) partialEntity).setUrn(input.toString());
    }
    if (input.getEntityType().equals("tag")) {
      partialEntity = new Tag();
      ((Tag) partialEntity).setUrn(input.toString());
    }
    if (input.getEntityType().equals("corpuser")) {
      partialEntity = new CorpUser();
      ((CorpUser) partialEntity).setUrn(input.toString());
    }
    if (input.getEntityType().equals("corpGroup")) {
      partialEntity = new CorpUser();
      ((CorpGroup) partialEntity).setUrn(input.toString());
    }
    if (input.getEntityType().equals("mlFeature")) {
      partialEntity = new MLFeature();
      ((MLFeature) partialEntity).setUrn(input.toString());
    }
    if (input.getEntityType().equals("mlFeatureTable")) {
      partialEntity = new MLFeatureTable();
      ((MLFeatureTable) partialEntity).setUrn(input.toString());
    }
    if (input.getEntityType().equals("mlPrimaryKey")) {
      partialEntity = new MLPrimaryKey();
      ((MLPrimaryKey) partialEntity).setUrn(input.toString());
    }
    if (input.getEntityType().equals("mlModel")) {
      partialEntity = new MLModel();
      ((MLModel) partialEntity).setUrn(input.toString());
    }
    if (input.getEntityType().equals("mlModelGroup")) {
      partialEntity = new MLModelGroup();
      ((MLModelGroup) partialEntity).setUrn(input.toString());
    }
    return partialEntity;
  }
}
