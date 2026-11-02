package controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import play.mvc.Http;
import play.mvc.Result;

public class SsoCallbackControllerTest {

  @Test
  public void requiredGroupsDenialPrefersRedirectUrlOverMessage() {
    final Result result =
        SsoCallbackController.requiredGroupsDenialResult(
            Optional.of("https://intranet.example.com/request-access"),
            "You need role X.",
            "/login");

    assertEquals(Http.Status.SEE_OTHER, result.status());
    assertEquals(
        Optional.of("https://intranet.example.com/request-access"), result.redirectLocation());
  }

  @Test
  public void requiredGroupsDenialFallsBackToLoginErrorMessage() {
    final Result result =
        SsoCallbackController.requiredGroupsDenialResult(
            Optional.empty(), "You need role X.", "/login");

    assertEquals(Http.Status.SEE_OTHER, result.status());
    assertTrue(result.redirectLocation().isPresent());
    assertTrue(result.redirectLocation().get().startsWith("/login?error_msg="));
    assertTrue(result.redirectLocation().get().contains("You"));
  }

  @Test
  public void requiredGroupsDenialIgnoresBlankRedirectUrl() {
    final Result result =
        SsoCallbackController.requiredGroupsDenialResult(
            Optional.of(""), "Custom denied.", "/login");

    assertEquals(Http.Status.SEE_OTHER, result.status());
    assertTrue(result.redirectLocation().orElse("").startsWith("/login?error_msg="));
  }
}
