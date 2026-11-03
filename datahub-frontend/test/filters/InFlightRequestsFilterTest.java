package filters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.stream.Materializer;
import org.junit.jupiter.api.Test;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Results;

class InFlightRequestsFilterTest {

  @Test
  void inflight_incrementsUntilSuccess() {
    InFlightRequestsFilter filter = filter();
    Http.RequestHeader header = mock(Http.RequestHeader.class);
    CompletableFuture<Result> pending = new CompletableFuture<>();

    CompletionStage<Result> result = filter.apply(request -> pending, header);

    assertEquals(1, filter.inFlightCount().get());
    pending.complete(Results.ok());
    assertEquals(0, filter.inFlightCount().get());
    assertEquals(200, result.toCompletableFuture().join().status());
  }

  @Test
  void inflight_decrementsWhenTheInnerFutureFails() {
    InFlightRequestsFilter filter = filter();
    Http.RequestHeader header = mock(Http.RequestHeader.class);
    CompletableFuture<Result> pending = new CompletableFuture<>();

    filter.apply(request -> pending, header);

    assertEquals(1, filter.inFlightCount().get());
    pending.completeExceptionally(new IllegalStateException("downstream"));
    assertEquals(0, filter.inFlightCount().get());
  }

  @Test
  void inflight_decrementsWhenNextThrows() {
    InFlightRequestsFilter filter = filter();
    Http.RequestHeader header = mock(Http.RequestHeader.class);

    assertThrows(
        IllegalStateException.class,
        () ->
            filter.apply(
                request -> {
                  throw new IllegalStateException("sync");
                },
                header));

    assertEquals(0, filter.inFlightCount().get());
  }

  private static InFlightRequestsFilter filter() {
    return new InFlightRequestsFilter(mock(Materializer.class));
  }
}
