package filters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import config.GracefulShutdownModule;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.stream.Materializer;
import org.junit.jupiter.api.Test;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Results;

class ShutdownConnectionCloseFilterTest {

  @Test
  void keepsConnectionOpenWhileServing() {
    GracefulShutdownModule shutdown = new GracefulShutdownModule();

    Result result = run(shutdown, CompletableFuture.completedFuture(Results.ok()));

    assertTrue(result.headers().get(Http.HeaderNames.CONNECTION) == null);
    assertEquals(200, result.status());
  }

  @Test
  void asksClientToCloseConnectionOnceShuttingDown() {
    GracefulShutdownModule shutdown = new GracefulShutdownModule();
    shutdown.markShuttingDown();

    Result result = run(shutdown, CompletableFuture.completedFuture(Results.ok()));

    assertEquals("close", result.headers().get(Http.HeaderNames.CONNECTION));
    assertEquals(200, result.status());
  }

  @Test
  void requestInFlightWhenShutdownStartsAlsoClosesConnection() {
    GracefulShutdownModule shutdown = new GracefulShutdownModule();
    CompletableFuture<Result> pending = new CompletableFuture<>();
    ShutdownConnectionCloseFilter filter =
        new ShutdownConnectionCloseFilter(mock(Materializer.class), shutdown);

    CompletionStage<Result> stage =
        filter.apply(request -> pending, mock(Http.RequestHeader.class));
    shutdown.markShuttingDown();
    pending.complete(Results.ok());

    assertEquals(
        "close", stage.toCompletableFuture().join().headers().get(Http.HeaderNames.CONNECTION));
  }

  private static Result run(GracefulShutdownModule shutdown, CompletionStage<Result> next) {
    ShutdownConnectionCloseFilter filter =
        new ShutdownConnectionCloseFilter(mock(Materializer.class), shutdown);
    return filter
        .apply(request -> next, mock(Http.RequestHeader.class))
        .toCompletableFuture()
        .join();
  }
}
