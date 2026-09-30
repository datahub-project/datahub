import filters.BasePathRedirectFilter
import filters.InFlightRequestsFilter
import filters.StaticAssetPaths
import javax.inject.Inject
import org.apache.pekko.stream.Materializer
import play.api.http.DefaultHttpFilters
import play.api.http.EnabledFilters
import play.api.Logger
import play.api.mvc.EssentialFilter
import play.api.mvc.RequestHeader
import play.api.mvc.Result
import play.filters.csp.CSPFilter
import play.filters.gzip.GzipFilter
import play.filters.gzip.GzipFilterConfig
import play.filters.headers.SecurityHeadersFilter

/**
 * Custom filter chain: CSPFilter outermost, then SecurityHeadersFilter, then
 * InFlightRequestsFilter, then BasePathRedirectFilter, then Play's enabled filters (GzipFilter
 * from play.filters.enabled in application.conf, with static-asset paths excluded so Vite
 * `.br`/`.gz` sidecars are the only compression layer for `/assets` and `/node_modules`).
 *
 * CSP and security headers wrap the base-path filter so when BasePathRedirectFilter short-circuits
 * with a redirect (without calling inner filters), the response still gets CSP and optional
 * X-Frame-Options / X-Content-Type-Options / Referrer-Policy. InFlightRequestsFilter sits inside
 * those security filters and outside gzip so /admin probes are counted even when the base-path
 * filter returns without calling the rest of the chain.
 */
class Filters @Inject()(
    cspFilter: CSPFilter,
    securityHeadersFilter: SecurityHeadersFilter,
    inFlightRequestsFilter: InFlightRequestsFilter,
    basePathRedirectFilter: BasePathRedirectFilter,
    gzipFilterConfig: GzipFilterConfig,
    materializer: Materializer,
    enabledFilters: EnabledFilters
) extends DefaultHttpFilters(
      Filters.buildChain(
        cspFilter,
        securityHeadersFilter,
        inFlightRequestsFilter,
        basePathRedirectFilter,
        gzipFilterConfig,
        enabledFilters
      )(materializer): _*
    ) {

  private val logger = Logger(getClass)
  logger.info(
    "HTTP filters enabled: " + filters.map(_.getClass.getSimpleName).mkString(", ")
  )
}

object Filters {
  def buildGzipFilter(gzipFilterConfig: GzipFilterConfig)(implicit mat: Materializer): GzipFilter = {
    new GzipFilter(
      gzipFilterConfig.withShouldGzip { (req: RequestHeader, res: Result) =>
        !StaticAssetPaths.isPrecompressedStaticPath(req.path) && gzipFilterConfig.shouldGzip(req, res)
      }
    )
  }

  def buildChain(
      cspFilter: CSPFilter,
      securityHeadersFilter: SecurityHeadersFilter,
      inFlightRequestsFilter: InFlightRequestsFilter,
      basePathRedirectFilter: BasePathRedirectFilter,
      gzipFilterConfig: GzipFilterConfig,
      enabledFilters: EnabledFilters
  )(implicit mat: Materializer): Seq[EssentialFilter] = {
    cspFilter +: securityHeadersFilter +: inFlightRequestsFilter +: basePathRedirectFilter +:
      buildGzipFilter(gzipFilterConfig) +:
      enabledFilters.filters.filterNot(_.isInstanceOf[GzipFilter])
  }
}
