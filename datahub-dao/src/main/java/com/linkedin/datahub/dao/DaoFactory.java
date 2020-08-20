package com.linkedin.datahub.dao;

import com.linkedin.datahub.dao.table.DataPlatformsDao;
import com.linkedin.datahub.dao.table.DatasetOwnerDao;
import com.linkedin.datahub.dao.table.DatasetsDao;
import com.linkedin.datahub.dao.table.GmsDao;
import com.linkedin.datahub.dao.table.LineageDao;
import com.linkedin.datahub.dao.view.BrowseDAO;
import com.linkedin.datahub.dao.view.CorpUserViewDao;
import com.linkedin.datahub.dao.view.DatasetViewDao;
import com.linkedin.datahub.dao.view.DocumentSearchDao;
import com.linkedin.datahub.dao.view.OwnerViewDao;
import com.linkedin.util.Configuration;

public class DaoFactory {

  private static final String GMS_HOST_ENV_VAR = "DATAHUB_GMS_HOST";
  private static final String GMS_PORT_ENV_VAR = "DATAHUB_GMS_PORT";

  private static GmsDao _gmsDao;
  private static DocumentSearchDao datasetDocumentSearchDao;
  private static DocumentSearchDao corpUserDocumentSearchDao;
  private static CorpUserViewDao corpUserViewDao;
  private static BrowseDAO datasetBrowseDao;
  private static OwnerViewDao ownerViewDao;
  private static DatasetViewDao datasetViewDao;
  private static DatasetOwnerDao datasetOwnerDao;
  private static DatasetsDao datasetsDao;
  private static LineageDao lineageDao;
  private static DataPlatformsDao dataPlatformsDao;

  private DaoFactory() {
  }

  private static GmsDao getGmsDao() {
    if (_gmsDao == null) {
      _gmsDao = new GmsDao(Configuration.getEnvironmentVariable(GMS_HOST_ENV_VAR),
              Integer.valueOf(Configuration.getEnvironmentVariable(GMS_PORT_ENV_VAR)));
    }
    return _gmsDao;
  }

  public static DocumentSearchDao getDatasetDocumentSearchDao() {
    if (datasetDocumentSearchDao == null) {
      datasetDocumentSearchDao = new DocumentSearchDao<>(getGmsDao().get_datasets());
    }
    return datasetDocumentSearchDao;
  }

  public static DocumentSearchDao getCorpUserDocumentSearchDao() {
    if (corpUserDocumentSearchDao == null) {
      corpUserDocumentSearchDao = new DocumentSearchDao<>(getGmsDao().get_corpUsers());
    }
    return corpUserDocumentSearchDao;
  }

  public static BrowseDAO getDatasetBrowseDAO() {
    if (datasetBrowseDao == null) {
      datasetBrowseDao = new BrowseDAO<>(getGmsDao().get_datasets());
    }
    return datasetBrowseDao;
  }

  public static CorpUserViewDao getCorpUserViewDao() {
    if (corpUserViewDao == null) {
      corpUserViewDao = new CorpUserViewDao(getGmsDao().get_corpUsers());
    }
    return corpUserViewDao;
  }

  public static OwnerViewDao getOwnerViewDao() {
    if (ownerViewDao == null) {
      ownerViewDao = new OwnerViewDao(getGmsDao().get_ownerships(), getGmsDao().get_corpUsers());
    }
    return ownerViewDao;
  }

  public static DatasetViewDao getDatasetViewDao() {
    if (datasetViewDao == null) {
      datasetViewDao = new DatasetViewDao(getGmsDao().get_datasets(), getGmsDao().get_deprecations(),
              getGmsDao().get_institutionalMemory(), getGmsDao().get_schemas());
    }
    return datasetViewDao;
  }

  public static DatasetOwnerDao getDatasetOwnerDao() {
    if (datasetOwnerDao == null) {
      datasetOwnerDao = new DatasetOwnerDao(getGmsDao().get_ownerships());
    }
    return datasetOwnerDao;
  }

  public static DatasetsDao getDatasetsDao() {
    if (datasetsDao == null) {
      datasetsDao = new DatasetsDao(getGmsDao().get_ownerships());
    }
    return datasetsDao;
  }

  public static LineageDao getLineageDao() {
    if (lineageDao == null) {
      lineageDao = new LineageDao(getGmsDao().get_lineages(), getGmsDao().get_datasets());
    }
    return lineageDao;
  }

  public static DataPlatformsDao getDataPlatformsDao() {
    if (dataPlatformsDao == null) {
      dataPlatformsDao = new DataPlatformsDao(getGmsDao().get_dataPlatforms());
    }
    return dataPlatformsDao;
  }
}
