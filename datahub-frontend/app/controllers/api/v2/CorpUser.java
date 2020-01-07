package controllers.api.v2;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.datahub.dao.DaoFactory;
import com.linkedin.datahub.dao.view.CorpUserViewDao;
import com.linkedin.datahub.util.CorpUserUtil;
import com.linkedin.identity.CorpUserEditableInfo;
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
import java.net.URISyntaxException;

import static com.linkedin.datahub.util.RestliUtil.*;

public class CorpUser extends Controller {

    private static final JsonNode EMPTY_RESPONSE = Json.newObject();

    private final CorpUserViewDao _corpUserViewDao;

    public CorpUser() {
        _corpUserViewDao = DaoFactory.getCorpUserViewDao();
    }

    /**
     * Get CorpUser given corpUser urn
     * @param corpUserUrn String
     * @return CorpUser
     */
    @Security.Authenticated(Secured.class)
    @Nonnull
    public Result getCorpUser(@Nonnull String corpUserUrn) {
        try {
            return ok(toJsonNode(_corpUserViewDao.get(corpUserUrn)));
        } catch (Exception e) {
            if (e.toString().contains("Response status 404")) {
                return notFound(EMPTY_RESPONSE);
            }

            Logger.error("Failed to get corp user", e);
            return internalServerError(ControllerUtil.errorResponse(e));
        }
    }

    /**
     * Creates or Updates CorpUserEditableInfo aspect
     *
     * @param corpUserUrn CorpUser urn
     */
    @Security.Authenticated(Secured.class)
    @Nonnull
    public Result updateCorpUserEditableInfo(@Nonnull String corpUserUrn) {
        final String username = session("user");
        if (StringUtils.isBlank(username)) {
            return unauthorized(EMPTY_RESPONSE);
        }
        final CorpuserUrn corpUser;
        try {
            corpUser = CorpUserUtil.toCorpUserUrn(corpUserUrn);
        } catch (URISyntaxException e) {
            return unauthorized("Invalid urn");
        }
        if (!corpUser.getUsernameEntity().equals(username)) {
            return unauthorized(EMPTY_RESPONSE);
        }
        final JsonNode requestBody = request().body().asJson();
        try {
            CorpUserEditableInfo corpUserEditableInfo =
                    RecordUtils.toRecordTemplate(CorpUserEditableInfo.class, requestBody.toString());
            _corpUserViewDao.updateCorpUserEditableConfig(corpUserUrn, corpUserEditableInfo);
            return ok(Json.newObject().set("updated", Json.toJson(true)));
        } catch (Exception e) {
            Logger.error("Failed to upsert corp user editable info", e);
            return internalServerError(ControllerUtil.errorResponse(e));
        }
    }
}