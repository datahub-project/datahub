package react.controllers;

import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Security;
import react.auth.Authenticator;

import javax.annotation.Nonnull;
import javax.inject.Inject;

public class AdhocController extends Controller {

    private final Config _config;

    @Inject
    public AdhocController(@Nonnull Config config) {
        _config = config;
    }

    @Security.Authenticated(Authenticator.class)
    @Nonnull
    public Result create() throws Exception {
        JsonNode event;
        try {
            event = request().body().asJson();
        } catch (Exception e) {
            return badRequest();
        }
        try {
             return ok(event.toString());
        } catch(Exception e) {
            return internalServerError(e.getMessage());
        }
    }

}
