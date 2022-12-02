package app;

import controllers.routes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import play.Application;
import play.Environment;
import play.Mode;
import play.inject.guice.GuiceApplicationBuilder;
import play.mvc.Http;
import play.mvc.Result;
import play.test.Helpers;
import play.test.WithApplication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static play.test.Helpers.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SetEnvironmentVariable(key = "DATAHUB_SECRET", value = "test")
@SetEnvironmentVariable(key = "KAFKA_BOOTSTRAP_SERVER", value = "")
@SetEnvironmentVariable(key = "DATAHUB_ANALYTICS_ENABLED", value = "false")
public class ApplicationTest extends WithApplication {

  @Override
  protected Application provideApplication() {
    return new GuiceApplicationBuilder().in(new Environment(Mode.TEST)).build();
  }

  @BeforeAll
  public void startup() {
    app = provideApplication();
    Helpers.start(app);
  }

  @AfterAll
  public void shutdown() {
    Helpers.stop(app);
  }

  @Test
  public void testHealth() {
    Http.RequestBuilder request = fakeRequest(routes.Application.healthcheck());

    Result result = route(app, request);
    assertEquals(OK, result.status());
  }

  @Test
  public void testIndex() {
    Http.RequestBuilder request = fakeRequest(routes.Application.index(""));

    Result result = route(app, request);
    assertEquals(OK, result.status());
  }

  @Test
  public void testIndexNotFound() {
    Http.RequestBuilder request = fakeRequest(routes.Application.index("/other"));
    Result result = route(app, request);
    assertEquals(NOT_FOUND, result.status());
  }
}
