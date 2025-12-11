/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

import filters.BasePathRedirectFilter
import javax.inject.Inject
import play.api.http.HttpFilters

class Filters @Inject()(basePathRedirectFilter: BasePathRedirectFilter) extends HttpFilters {
  override val filters = Seq(basePathRedirectFilter)
}