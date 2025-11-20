import filters.BasePathRedirectFilter
import javax.inject.Inject
import play.api.http.HttpFilters

class Filters @Inject()(basePathRedirectFilter: BasePathRedirectFilter) extends HttpFilters {
  override val filters = Seq(basePathRedirectFilter)
}