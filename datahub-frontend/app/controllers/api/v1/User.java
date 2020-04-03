package controllers.api.v1;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.datahub.dao.DaoFactory;
import com.linkedin.datahub.dao.view.CorpUserViewDao;
import com.linkedin.datahub.models.table.CompanyUser;
import com.linkedin.datahub.models.table.Group;
import com.linkedin.datahub.models.table.UserEntity;
import com.linkedin.datahub.util.CorpUserUtil;
import controllers.Secured;
import jersey.repackaged.com.google.common.cache.Cache;
import jersey.repackaged.com.google.common.cache.CacheBuilder;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Security;
import utils.ControllerUtil;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class User extends Controller {

  private static final String CACHE_INTERNAL_USERS = "internal.users.cache";
  private static final String CACHE_INTERNAL_GROUPS = "internal.groups.cache";
  private static final String CACHE_INTERNAL_ENTITIES = "internal.entities.cache";

  private final CorpUserViewDao _corpUserViewDao;

  private static final Cache<String, List> CACHE = CacheBuilder.newBuilder()
      .expireAfterWrite(24, TimeUnit.HOURS)
      .maximumSize(3)
      .build();

  public User() {
    _corpUserViewDao = DaoFactory.getCorpUserViewDao();
  }

  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result getLoggedInUser() {
    ObjectNode result = Json.newObject();
    com.linkedin.identity.CorpUser corpUser;
    String username = request().attrs().get(Security.USERNAME);
    try {
      corpUser = _corpUserViewDao.getByUserName(username);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (corpUser == null || !corpUser.getUsername().equals(username)
            || !corpUser.hasInfo()) {
      result.put("status", "failed");
      result.put("message", "can't find user info");
      return ok(result);
    }

    result.set("user", Json.toJson(CorpUserUtil.toCorpUserView(corpUser)));
    result.put("status", "ok");
    return ok(result);
  }

  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result getAllCompanyUsers() {
    List<CompanyUser> users = (List<CompanyUser>) CACHE.getIfPresent(CACHE_INTERNAL_USERS);
    if (users == null || users.size() == 0) {
      try {
        users = _corpUserViewDao.getAllCorpUsers().stream()
                .map(CorpUserUtil::toCompanyUserView)
                .collect(Collectors.toList());
      } catch (Exception e) {
        return internalServerError(ControllerUtil.errorResponse(e));
      }
      CACHE.put(CACHE_INTERNAL_USERS, users);
    }
    ObjectNode result = Json.newObject();
    result.put("status", "ok");
    result.set("employees", Json.toJson(users));
    return ok(result);
  }

  @Security.Authenticated(Secured.class)
  @Nonnull
  public Result getAllGroups() {
    List<Group> groups = (List<Group>) CACHE.getIfPresent(CACHE_INTERNAL_GROUPS);
    if (groups == null || groups.size() == 0) {
      groups = Collections.emptyList();
      CACHE.put(CACHE_INTERNAL_GROUPS, groups);
    }
    ObjectNode result = Json.newObject();
    result.put("status", "ok");
    result.set("groups", Json.toJson(groups));
    return ok(result);
  }

  @Nonnull
  public Result getAllUserEntities() {
    List<UserEntity> entities = (List<UserEntity>) CACHE.getIfPresent(CACHE_INTERNAL_ENTITIES);
    if (entities == null || entities.size() == 0) {
      try {
        entities = _corpUserViewDao.getAllCorpUsers().stream()
                .map(CorpUserUtil::toUserEntityView)
                .collect(Collectors.toList());
      } catch (Exception e) {
        return internalServerError(ControllerUtil.errorResponse(e));
      }
      CACHE.put(CACHE_INTERNAL_ENTITIES, entities);
    }
    ObjectNode result = Json.newObject();
    result.put("status", "ok");
    result.set("userEntities", Json.toJson(entities));
    return ok(result);
  }
}
