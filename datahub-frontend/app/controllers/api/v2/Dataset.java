package controllers.api.v2;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.datahub.dao.DaoFactory;
import com.linkedin.datahub.dao.table.DataPlatformsDao;
import com.linkedin.datahub.dao.table.DatasetOwnerDao;
import com.linkedin.datahub.dao.table.LineageDao;
import com.linkedin.datahub.dao.view.DatasetViewDao;
import com.linkedin.datahub.dao.view.OwnerViewDao;
import com.linkedin.datahub.models.view.*;
import com.linkedin.metadata.dao.utils.RecordUtils;
import controllers.Secured;
import org.apache.commons.lang3.StringUtils;
import play.Logger;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Security;
import utils.ControllerUtil;

import javax.annotation.Nonnull;
import java.util.List;

public class Dataset extends Controller {

  private final DatasetViewDao _datasetViewDao;
  private final OwnerViewDao _ownerViewDao;
  private final DatasetOwnerDao _datasetOwnerDao;
  private final LineageDao _lineageDao;
  private final DataPlatformsDao _dataPlatformsDao;

  private static final JsonNode EMPTY_RESPONSE = Json.newObject();

  public Dataset() {
    _datasetViewDao = DaoFactory.getDatasetViewDao();
    _ownerViewDao = DaoFactory.getOwnerViewDao();
    _datasetOwnerDao = DaoFactory.getDatasetOwnerDao();
    _lineageDao = DaoFactory.getLineageDao();
    _dataPlatformsDao = DaoFactory.getDataPlatformsDao();
  }

  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result getDataset(@Nonnull String datasetUrn) {
    final DatasetView view;
    try {
      view = _datasetViewDao.getDatasetView(datasetUrn);
    } catch (Exception e) {
      if (ControllerUtil.checkErrorCode(e, 404)) {
        return notFound(EMPTY_RESPONSE);
      }

      Logger.error("Failed to get dataset view", e);
      return internalServerError(ControllerUtil.errorResponse(e));
    }

    return ok(Json.newObject().set("dataset", Json.toJson(view)));
  }

  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result updateDatasetDeprecation(@Nonnull String datasetUrn) {
    final String username = session("user");
    if (StringUtils.isBlank(username)) {
      return unauthorized(EMPTY_RESPONSE);
    }

    try {
      JsonNode record = request().body().asJson();

      boolean deprecated = record.get("deprecated").asBoolean();
      String deprecationNote = record.hasNonNull("deprecationNote") ? record.get("deprecationNote").asText() : "";
      long decommissionTime = record.hasNonNull("decommissionTime") ? record.get("decommissionTime").asLong() : 0;
      if (deprecated && decommissionTime <= 0) {
        throw new IllegalArgumentException("Invalid decommission time");
      }

      _datasetViewDao.setDatasetDeprecation(datasetUrn, deprecated, deprecationNote, decommissionTime, username);
    } catch (Exception e) {
      Logger.error("Update dataset deprecation fail", e);
      return internalServerError(ControllerUtil.errorResponse(e));
    }

    return ok(EMPTY_RESPONSE);
  }

  /**
   * Creates or Updates {@link InstitutionalMemory} aspect given dataset urn
   *
   * <p>{@link com.linkedin.common.InstitutionalMemoryMetadata} record that does not contain audit stamp is filled
   * here with session username as the actor and system time as the time</p>
   *
   * @param datasetUrn Dataset Urn
   */
  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result updateInstitutionalMemory(@Nonnull String datasetUrn) {
    final String username = session("user");
    if (StringUtils.isBlank(username)) {
      return unauthorized(EMPTY_RESPONSE);
    }
    final JsonNode requestBody = request().body().asJson();
    try {
      InstitutionalMemory institutionalMemory =
          RecordUtils.toRecordTemplate(InstitutionalMemory.class, requestBody.toString());
      institutionalMemory.getElements().forEach(element -> {
        if (!element.hasCreateStamp()) {
          element.setCreateStamp(
              new AuditStamp().setActor(new CorpuserUrn(username)).setTime(System.currentTimeMillis()));
        }
      });
      _datasetViewDao.updateInstitutionalMemory(datasetUrn, institutionalMemory);
    } catch (Exception e) {
      Logger.error("Failed to update Institutional Memory aspect", e);
      return internalServerError(ControllerUtil.errorResponse(e));
    }
    return ok(EMPTY_RESPONSE);
  }

  /**
   * Gets {@link InstitutionalMemory} aspect given dataset urn
   *
   * @param datasetUrn Dataset Urn
   */
  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result getInstitutionalMemory(String datasetUrn) {
    try {
      JsonNode responsenode = _datasetViewDao.getInstitutionalMemory(datasetUrn);
      return ok(responsenode);
    } catch (Exception e) {
      if (ControllerUtil.checkErrorCode(e, 404)) {
        return notFound(EMPTY_RESPONSE);
      }
      Logger.error("Failed to get Institutional Memory aspect", e);
      return internalServerError(ControllerUtil.errorResponse(e));
    }
  }

  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result getDatasetOwners(@Nonnull String datasetUrn) {
    final DatasetOwnership ownership;
    try {
      ownership = _ownerViewDao.getDatasetOwners(datasetUrn);
    } catch (Exception e) {
      if (ControllerUtil.checkErrorCode(e, 404)) {
        return notFound(EMPTY_RESPONSE);
      }

      Logger.error("Fetch owners fail", e);
      return internalServerError(ControllerUtil.errorResponse(e));
    }
    return ok(Json.toJson(ownership));
  }

  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result updateDatasetOwners(@Nonnull String datasetUrn) {
    final String username = session("user");
    if (StringUtils.isBlank(username)) {
      return unauthorized(EMPTY_RESPONSE);
    }

    final JsonNode content = request().body().asJson();
    // content should contain arraynode 'owners': []
    if (content == null || !content.has("owners") || !content.get("owners").isArray()) {
      return badRequest(ControllerUtil.errorResponse("Update dataset owners fail: missing owners field"));
    }

    try {
      final List<DatasetOwner> owners = Json.mapper().readerFor(new TypeReference<List<DatasetOwner>>() {
      }).readValue(content.get("owners"));

      long confirmedOwnerUserCount = owners.stream()
          .filter(s -> "DataOwner".equalsIgnoreCase(s.getType()) && "user".equalsIgnoreCase(s.getIdType())
              && "UI".equalsIgnoreCase(s.getSource()))
          .count();

      // enforce at least two UI (confirmed) USER DataOwner for a dataset before making any changes
      if (confirmedOwnerUserCount < 2) {
        return badRequest(ControllerUtil.errorResponse("Less than 2 UI USER owners"));
      }

      _datasetOwnerDao.updateDatasetOwners(datasetUrn, owners, username);
    } catch (Exception e) {
      Logger.error("Update Dataset owners fail", e);
      return internalServerError(ControllerUtil.errorResponse(e));
    }
    return ok(EMPTY_RESPONSE);
  }

  /**
   * Gets latest DatasetSnapshot given DatasetUrn
   *
   * @param datasetUrn String
   * @return DatasetSnapshot
   */
  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result getDatasetSnapshot(@Nonnull String datasetUrn) {
    try {
      return ok(_datasetViewDao.getSnapshot(datasetUrn));
    } catch (Exception e) {
      if (e.toString().contains("Response status 404")) {
        return notFound(EMPTY_RESPONSE);
      }

      Logger.error("Failed to get dataset snapshot", e);
      return internalServerError(ControllerUtil.errorResponse(e));
    }
  }

  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result getDatasetSchema(@Nonnull String datasetUrn) {
    final DatasetSchema schema;
    try {
      schema = _datasetViewDao.getDatasetSchema(datasetUrn);
    } catch (Exception e) {
      if (ControllerUtil.checkErrorCode(e, 404)) {
        return notFound(EMPTY_RESPONSE);
      }

      Logger.error("Fetch schema fail", e);
      return internalServerError(ControllerUtil.errorResponse(e));
    }

    if (schema == null) {
      return notFound(EMPTY_RESPONSE);
    }
    return ok(Json.newObject().set("schema", Json.toJson(schema)));
  }

  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result getDatasetSuggestedOwners(@Nonnull String datasetUrn) {
    return ok(EMPTY_RESPONSE);
  }

  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result getDatasetUpstreams(@Nonnull String datasetUrn) {
    final List<LineageView> upstreams;
    try {
      upstreams = _lineageDao.getUpstreamLineage(datasetUrn);
    } catch (Exception e) {
      Logger.error("Fetch Dataset upstreams error", e);
      return internalServerError(ControllerUtil.errorResponse(e));
    }
    return ok(Json.toJson(upstreams));
  }

  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result getDatasetDownstreams(@Nonnull String datasetUrn) {
    final List<LineageView> downstreams;
    try {
      downstreams = _lineageDao.getDownstreamLineage(datasetUrn);
    } catch (Exception e) {
      Logger.error("Fetch Dataset downstreams error", e);
      return internalServerError(ControllerUtil.errorResponse(e));
    }
    return ok(Json.toJson(downstreams));
  }

  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result getDataPlatforms() {
    try {
      return ok(ControllerUtil.jsonNode("platforms", _dataPlatformsDao.getAllPlatforms()));
    } catch (final Exception e) {
      Logger.error("Fail to get data platforms", e);
      return notFound(ControllerUtil.errorResponse(e));
    }
  }
}
