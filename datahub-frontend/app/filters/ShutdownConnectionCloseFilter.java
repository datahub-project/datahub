package filters;

import config.GracefulShutdownModule;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.pekko.stream.Materializer;
import play.mvc.Filter;
import play.mvc.Http;
import play.mvc.Result;

/**
 * Once graceful shutdown starts, adds {@code Connection: close} to every response so Play closes
 * the connection after it.
 *
 * <p>Removing a terminating pod from Service endpoints only stops new connections. An upstream
 * proxy that keeps a keep-alive pool to this pod keeps sending requests on the connections it
 * already has. Closing each connection after its next response moves that pool to other pods during
 * {@code play.server.waitBeforeTermination}, before Pekko terminates connections and answers any
 * new request on them with 503.
 *
 * <p>The flag is checked when the response is ready, so a request already in flight when shutdown
 * starts also closes its connection.
 */
@Singleton
public class ShutdownConnectionCloseFilter extends Filter {

  private final GracefulShutdownModule shutdownModule;

  @Inject
  public ShutdownConnectionCloseFilter(Materializer mat, GracefulShutdownModule shutdownModule) {
    super(mat);
    this.shutdownModule = shutdownModule;
  }

  @Override
  public CompletionStage<Result> apply(
      Function<Http.RequestHeader, CompletionStage<Result>> next,
      Http.RequestHeader requestHeader) {
    return next.apply(requestHeader)
        .thenApply(
            result ->
                shutdownModule.isShuttingDown()
                    ? result.withHeader(Http.HeaderNames.CONNECTION, "close")
                    : result);
  }
}
