package react.controllers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.saasquatch.jsonschemainferrer.*;
import com.typesafe.config.Config;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Security;
import react.auth.Authenticator;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
public class AdhocController extends Controller {

    private final Config _config;
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final JsonSchemaInferrer inferrer = JsonSchemaInferrer.newBuilder()
            .setSpecVersion(SpecVersion.DRAFT_07)
            // Requires commons-validator
            .addFormatInferrers(FormatInferrers.email(), FormatInferrers.dateTime(), FormatInferrers.ip())
            .setAdditionalPropertiesPolicy(AdditionalPropertiesPolicies.noOp())
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

    @Security.Authenticated(Authenticator.class)
    @Nonnull
    public Result upload() throws IOException, ParseException {
        play.mvc.Http.MultipartFormData<File> body = request().body().asMultipartFormData();
        play.mvc.Http.MultipartFormData.FilePart<File> requestFile = body.getFile("file");
        if (requestFile != null) {
            java.io.File file = requestFile.getFile();
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(new FileReader(file));
            JSONObject jsonObject =  (JSONObject) obj;

            final JsonNode jsonData = mapper.readTree(jsonObject.toString());
            final JsonNode jsonSchema = inferrer.inferForSample(jsonData);

            // return json schema
            return ok(jsonSchema);
        } else {
            flash("error", "Missing file");
            return badRequest();
        }
    }
}
