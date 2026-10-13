import filters.BasePathRedirectFilter
import filters.InFlightRequestsFilter
import javax.inject.Inject
import play.api.http.DefaultHttpFilters
import play.api.http.EnabledFilters
import play.api.Logger
import play.filters.csp.CSPFilter
import play.filters.headers.SecurityHeadersFilter

/**
 * Custom filter chain: CSPFilter outermost, then SecurityHeadersFilter, then
 * InFlightRequestsFilter, then BasePathRedirectFilter, then Play's enabled filters (GzipFilter
 * from play.filters.enabled in application.conf).
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
    enabledFilters: EnabledFilters
) extends DefaultHttpFilters(
      (cspFilter
        +: securityHeadersFilter
        +: inFlightRequestsFilter
        +: basePathRedirectFilter
        +: enabledFilters.filters): _*
    ) {

  private val logger = Logger(getClass)
  private val chainForLog =
    cspFilter +: securityHeadersFilter +: inFlightRequestsFilter +: basePathRedirectFilter +: enabledFilters.filters
  logger.info(
    "HTTP filters enabled: " + chainForLog.map(_.getClass.getSimpleName).mkString(", ")
  )
}