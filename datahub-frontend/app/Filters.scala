import filters.BasePathRedirectFilter
import javax.inject.Inject
import play.api.http.DefaultHttpFilters
import play.api.http.EnabledFilters
import play.api.Logger
import play.filters.csp.CSPFilter

/**
 * Custom filter chain: CSPFilter outermost, then BasePathRedirectFilter, then Play's enabled filters
 * (GzipFilter from play.filters.enabled in application.conf).
 *
 * CSP must wrap the base-path filter so when BasePathRedirectFilter short-circuits with a redirect
 * (without calling inner filters), the response still passes through CSPFilter and gets CSP headers.
 * Base-path logic is unchanged: it still runs before gzip and the router on the request path.
 */
class Filters @Inject()(
    cspFilter: CSPFilter,
    basePathRedirectFilter: BasePathRedirectFilter,
    enabledFilters: EnabledFilters
) extends DefaultHttpFilters(
      (cspFilter +: basePathRedirectFilter +: enabledFilters.filters): _*
    ) {

  private val logger = Logger(getClass)
  private val chainForLog = cspFilter +: basePathRedirectFilter +: enabledFilters.filters
  logger.info(
    "HTTP filters enabled: " + chainForLog.map(_.getClass.getSimpleName).mkString(", ")
  )
}