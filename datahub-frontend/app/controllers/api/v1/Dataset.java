package controllers.api.v1;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.datahub.dao.DaoFactory;
import com.linkedin.datahub.dao.table.DatasetsDao;
import controllers.Secured;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Security;

import javax.annotation.Nonnull;


public class Dataset extends Controller {

    private final DatasetsDao _datasetsDao;

    public Dataset() {
        _datasetsDao = DaoFactory.getDatasetsDao();
    }

    @Security.Authenticated(Secured.class)
    @Nonnull
    public Result getDatasetOwnerTypes() {
        ObjectNode result = Json.newObject();

        result.put("status", "ok");
        result.set("ownerTypes", Json.toJson(_datasetsDao.getDatasetOwnerTypes()));
        return ok(result);
    }
}