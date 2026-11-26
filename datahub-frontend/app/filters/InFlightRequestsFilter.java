package filters;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.pekko.stream.Materializer;
import play.mvc.Filter;
import play.mvc.Http;
import play.mvc.Result;

/**
 * Counts HTTP requests inside the Play filter chain, including {@code /admin} probes. The counter
 * is published as {@code play.http.requests.inflight} by {@link
 * auth.metrics.PlayHttpServerMetrics}.
 */
@Singleton
public class InFlightRequestsFilter extends Filter {

  private final AtomicInteger inFlight = new AtomicInteger();

  @Inject
  public InFlightRequestsFilter(Materializer mat) {
    super(mat);
  }

  public AtomicInteger inFlightCount() {
    return inFlight;
  }

  @Override
  public CompletionStage<Result> apply(
      Function<Http.RequestHeader, CompletionStage<Result>> next,
      Http.RequestHeader requestHeader) {
    inFlight.incrementAndGet();
    try {
      return next.apply(requestHeader).whenComplete((result, error) -> inFlight.decrementAndGet());
    } catch (RuntimeException | Error e) {
      inFlight.decrementAndGet();
      throw e;
    }
  }
}
