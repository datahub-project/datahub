package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.MLFeature;
import com.linkedin.datahub.graphql.generated.MLFeatureTable;
import com.linkedin.datahub.graphql.generated.MLModel;
import com.linkedin.datahub.graphql.generated.MLModelGroup;
import com.linkedin.datahub.graphql.generated.MLPrimaryKey;
import com.linkedin.datahub.graphql.generated.Notebook;
import com.linkedin.datahub.graphql.generated.Tag;
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
      ((Dataset) partialEntity).setType(EntityType.DATASET);
    }
    if (input.getEntityType().equals("glossaryTerm")) {
      partialEntity = new GlossaryTerm();
      ((GlossaryTerm) partialEntity).setUrn(input.toString());
      ((GlossaryTerm) partialEntity).setType(EntityType.GLOSSARY_TERM);
    }
    if (input.getEntityType().equals("chart")) {
      partialEntity = new Chart();
      ((Chart) partialEntity).setUrn(input.toString());
      ((Chart) partialEntity).setType(EntityType.CHART);
    }
    if (input.getEntityType().equals("dashboard")) {
      partialEntity = new Dashboard();
      ((Dashboard) partialEntity).setUrn(input.toString());
      ((Dashboard) partialEntity).setType(EntityType.DASHBOARD);
    }
    if (input.getEntityType().equals("notebook")) {
      partialEntity = new Notebook();
      ((Notebook) partialEntity).setUrn(input.toString());
      ((Notebook) partialEntity).setType(EntityType.NOTEBOOK);
    }
    if (input.getEntityType().equals("dataJob")) {
      partialEntity = new DataJob();
      ((DataJob) partialEntity).setUrn(input.toString());
      ((DataJob) partialEntity).setType(EntityType.DATA_JOB);
    }
    if (input.getEntityType().equals("dataFlow")) {
      partialEntity = new DataFlow();
      ((DataFlow) partialEntity).setUrn(input.toString());
      ((DataFlow) partialEntity).setType(EntityType.DATA_FLOW);
    }
    if (input.getEntityType().equals("tag")) {
      partialEntity = new Tag();
      ((Tag) partialEntity).setUrn(input.toString());
      ((Tag) partialEntity).setType(EntityType.TAG);
    }
    if (input.getEntityType().equals("corpuser")) {
      partialEntity = new CorpUser();
      ((CorpUser) partialEntity).setUrn(input.toString());
      ((CorpUser) partialEntity).setType(EntityType.CORP_USER);
    }
    if (input.getEntityType().equals("corpGroup")) {
      partialEntity = new CorpGroup();
      ((CorpGroup) partialEntity).setUrn(input.toString());
      ((CorpGroup) partialEntity).setType(EntityType.CORP_GROUP);
    }
    if (input.getEntityType().equals("mlFeature")) {
      partialEntity = new MLFeature();
      ((MLFeature) partialEntity).setUrn(input.toString());
      ((MLFeature) partialEntity).setType(EntityType.MLFEATURE);
    }
    if (input.getEntityType().equals("mlFeatureTable")) {
      partialEntity = new MLFeatureTable();
      ((MLFeatureTable) partialEntity).setUrn(input.toString());
      ((MLFeatureTable) partialEntity).setType(EntityType.MLFEATURE_TABLE);
    }
    if (input.getEntityType().equals("mlPrimaryKey")) {
      partialEntity = new MLPrimaryKey();
      ((MLPrimaryKey) partialEntity).setUrn(input.toString());
      ((MLPrimaryKey) partialEntity).setType(EntityType.MLPRIMARY_KEY);
    }
    if (input.getEntityType().equals("mlModel")) {
      partialEntity = new MLModel();
      ((MLModel) partialEntity).setUrn(input.toString());
      ((MLModel) partialEntity).setType(EntityType.MLMODEL);
    }
    if (input.getEntityType().equals("mlModelGroup")) {
      partialEntity = new MLModelGroup();
      ((MLModelGroup) partialEntity).setUrn(input.toString());
      ((MLModelGroup) partialEntity).setType(EntityType.MLMODEL_GROUP);
    }
    if (input.getEntityType().equals("dataPlatform")) {
      partialEntity = new DataPlatform();
      ((DataPlatform) partialEntity).setUrn(input.toString());
      ((DataPlatform) partialEntity).setType(EntityType.DATA_PLATFORM);
    }
    if (input.getEntityType().equals("container")) {
      partialEntity = new Container();
      ((Container) partialEntity).setUrn(input.toString());
      ((Container) partialEntity).setType(EntityType.CONTAINER);
    }
    if (input.getEntityType().equals("domain")) {
      partialEntity = new Domain();
      ((Domain) partialEntity).setUrn(input.toString());
      ((Domain) partialEntity).setType(EntityType.DOMAIN);
    }
    if (input.getEntityType().equals("assertion")) {
      partialEntity = new Assertion();
      ((Assertion) partialEntity).setUrn(input.toString());
      ((Assertion) partialEntity).setType(EntityType.ASSERTION);
    }
    return partialEntity;
  }
}
