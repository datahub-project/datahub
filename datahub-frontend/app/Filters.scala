import filters.BasePathRedirectFilter
import filters.ContentHashedAssetCacheFilter
import filters.InFlightRequestsFilter
import filters.ShutdownConnectionCloseFilter
import javax.inject.Inject
import play.api.http.DefaultHttpFilters
import play.api.http.EnabledFilters
import play.api.Logger
import play.filters.csp.CSPFilter
import play.filters.headers.SecurityHeadersFilter

/**
 * Custom filter chain: ShutdownConnectionCloseFilter outermost, then CSPFilter, then
 * SecurityHeadersFilter, then InFlightRequestsFilter, then BasePathRedirectFilter, then Play's
 * enabled filters (GzipFilter from play.filters.enabled in application.conf), then
 * ContentHashedAssetCacheFilter closest to the action so it rewrites Cache-Control on the asset
 * result.
 *
 * ShutdownConnectionCloseFilter is outermost so every response, including redirects that
 * short-circuit inner filters, carries Connection: close once graceful shutdown starts.
 *
 * CSP and security headers wrap the base-path filter so when BasePathRedirectFilter short-circuits
 * with a redirect (without calling inner filters), the response still gets CSP and optional
 * X-Frame-Options / X-Content-Type-Options / Referrer-Policy. InFlightRequestsFilter sits inside
 * those security filters and outside gzip so /admin probes are counted even when the base-path
 * filter returns without calling the rest of the chain.
 */
class Filters @Inject()(
    shutdownConnectionCloseFilter: ShutdownConnectionCloseFilter,
    cspFilter: CSPFilter,
    securityHeadersFilter: SecurityHeadersFilter,
    inFlightRequestsFilter: InFlightRequestsFilter,
    basePathRedirectFilter: BasePathRedirectFilter,
    enabledFilters: EnabledFilters,
    contentHashedAssetCacheFilter: ContentHashedAssetCacheFilter
) extends DefaultHttpFilters(
      ((shutdownConnectionCloseFilter
        +: cspFilter
        +: securityHeadersFilter
        +: inFlightRequestsFilter
        +: basePathRedirectFilter
        +: enabledFilters.filters) :+ contentHashedAssetCacheFilter): _*
    ) {

  private val logger = Logger(getClass)
  private val chainForLog =
    (shutdownConnectionCloseFilter
      +: cspFilter
      +: securityHeadersFilter
      +: inFlightRequestsFilter
      +: basePathRedirectFilter
      +: enabledFilters.filters) :+ contentHashedAssetCacheFilter
  logger.info(
    "HTTP filters enabled: " + chainForLog.map(_.getClass.getSimpleName).mkString(", ")
  )
}
