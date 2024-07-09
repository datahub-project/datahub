package io.datahubproject.openapi.tests;

import static com.linkedin.metadata.test.TestConstants.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.metadata.test.definition.TestDefinition;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.propertyeditors.StringArrayPropertyEditor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/operations/metadataTests")
@Slf4j
@Tag(name = "MetadataTestOperations", description = "An API for inspecting Metadata Tests")
public class MetadataTestsController {
  private final AuthorizerChain authorizerChain;
  private final OperationContext systemOperationContext;
  private final TestEngine testEngine;

  public MetadataTestsController(
      OperationContext systemOperationContext,
      TestEngine testEngine,
      AuthorizerChain authorizerChain) {
    this.systemOperationContext = systemOperationContext;
    this.testEngine = testEngine;
    this.authorizerChain = authorizerChain;
  }

  @InitBinder
  public void initBinder(WebDataBinder binder) {
    binder.registerCustomEditor(String[].class, new StringArrayPropertyEditor(null));
  }

  @Tag(name = "Tests")
  @PostMapping(path = "/explainTest", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Explain Test Execution")
  public ResponseEntity<String> explainTestExecution(
      @Parameter(
              name = "testJson",
              required = true,
              description = "JSON String for a Metadata test to evaluate.")
          @RequestBody
          @Nonnull
          String testJson) {

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    if (!AuthUtil.isAPIAuthorized(
        authentication, authorizerChain, PoliciesConfig.EXPLAIN_TEST_PRIVILEGE)) {
      log.error("{} is not authorized to get explain tests", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }
    OperationContext opContext =
        systemOperationContext.asSession(
            RequestContext.builder().buildOpenapi("explainTest", Collections.emptyList()),
            authorizerChain,
            authentication);
    TestDefinition testDefinition = testEngine.getParser().deserialize(DUMMY_TEST_URN, testJson);
    List<String> elasticSearchExplainSelect =
        testEngine.getElasticSearchTestExecutor().explainSelect(testDefinition);
    List<String> elasticSearchExplainEvaluate =
        testEngine.getElasticSearchTestExecutor().explainEvaluate(testDefinition);

    return ResponseEntity.ok(
        new JSONObject()
            .put("selectExplanation", elasticSearchExplainSelect)
            .put("evaluateExplanation", elasticSearchExplainEvaluate)
            .toString());
  }
}
