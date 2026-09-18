import filters.BasePathRedirectFilter
import javax.inject.Inject
import play.api.http.DefaultHttpFilters
import play.api.http.EnabledFilters
import play.api.Logger
import play.filters.csp.CSPFilter
import play.filters.headers.SecurityHeadersFilter

/**
 * Custom filter chain: CSPFilter outermost, then SecurityHeadersFilter, then
 * BasePathRedirectFilter, then Play's enabled filters (GzipFilter from play.filters.enabled in
 * application.conf).
 *
 * CSP and security headers wrap the base-path filter so when BasePathRedirectFilter short-circuits
 * with a redirect (without calling inner filters), the response still gets CSP and optional
 * X-Frame-Options / X-Content-Type-Options / Referrer-Policy. Base-path logic is unchanged: it
 * still runs before gzip and the router on the request path.
 */
class Filters @Inject()(
    cspFilter: CSPFilter,
    securityHeadersFilter: SecurityHeadersFilter,
    basePathRedirectFilter: BasePathRedirectFilter,
    enabledFilters: EnabledFilters
) extends DefaultHttpFilters(
      (cspFilter +: securityHeadersFilter +: basePathRedirectFilter +: enabledFilters.filters): _*
    ) {

  private val logger = Logger(getClass)
  private val chainForLog =
    cspFilter +: securityHeadersFilter +: basePathRedirectFilter +: enabledFilters.filters
  logger.info(
    "HTTP filters enabled: " + chainForLog.map(_.getClass.getSimpleName).mkString(", ")
  )
}