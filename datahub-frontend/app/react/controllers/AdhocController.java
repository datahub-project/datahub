package react.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.saasquatch.jsonschemainferrer.*;
import com.typesafe.config.Config;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Security;
import auth.Authenticator;

import javax.annotation.Nonnull;
import javax.inject.Inject;

public class AdhocController extends Controller {

    private final Config _config;
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final JsonSchemaInferrer inferrer = JsonSchemaInferrer.newBuilder()
            .setSpecVersion(SpecVersion.DRAFT_07)
            // Requires commons-validator
            .addFormatInferrers(FormatInferrers.email(), FormatInferrers.dateTime(), FormatInferrers.ip())
            .setAdditionalPropertiesPolicy(AdditionalPropertiesPolicies.allowed())
            .setRequiredPolicy(RequiredPolicies.noOp())
            .setTitleDescriptionGenerator(TitleDescriptionGenerators.useFieldNamesAsTitles())
            .addEnumExtractors(EnumExtractors.validEnum(java.time.Month.class),
                    EnumExtractors.validEnum(java.time.DayOfWeek.class))
            .build();
    @Inject
    public AdhocController(@Nonnull Config config) {
        _config = config;
    }

    @Security.Authenticated(Authenticator.class)
    @Nonnull
    public Result create() throws Exception {
        return ok();
    }

    @Security.Authenticated(Authenticator.class)
    @Nonnull
    public Result upload() {
        return badRequest();
    }
}
