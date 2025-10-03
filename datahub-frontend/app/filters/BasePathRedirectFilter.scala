package filters

import akka.stream.Materializer
import javax.inject.{Inject, Singleton}
import play.api.mvc._
import play.api.Logger
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

/**
 * HTTP filter that redirects requests outside the configured base path to the base path.
 * This ensures that when play.http.context is configured (e.g., "/datahub"), requests to
 * paths like "/login" get redirected to "/datahub/login" instead of returning 404.
 */
@Singleton
class BasePathRedirectFilter @Inject()(config: Config)(implicit val mat: Materializer, ec: ExecutionContext) extends Filter {

  private val logger = Logger(this.getClass)

  private lazy val basePath: String = {
    val contextPath = if (config.hasPath("play.http.context")) {
      config.getString("play.http.context")
    } else {
      ""
    }
    val normalized = if (contextPath.trim.isEmpty) "" else contextPath.trim
    logger.info(s"BasePathRedirectFilter: Base path configured as: '$normalized'")
    normalized
  }

  def apply(nextFilter: RequestHeader => Future[Result])(requestHeader: RequestHeader): Future[Result] = {

    val path = requestHeader.path

    // First, handle trailing slash redirects (redirectToNoTrailingSlashIfPresent equivalent)
    if (path.endsWith("/") && path.length > 1) {
      val pathWithoutTrailingSlash = path.substring(0, path.length - 1)
      val redirectUrl = pathWithoutTrailingSlash +
        (if (requestHeader.rawQueryString.nonEmpty) "?" + requestHeader.rawQueryString else "")

      logger.debug(s"Redirecting trailing slash: $path to $redirectUrl")
      return Future.successful(Results.MovedPermanently(redirectUrl))
    }

    // Then handle base path redirects if base path is configured
    if (basePath.nonEmpty && !path.startsWith(basePath)) {
      val redirectUrl = basePath + path +
        (if (requestHeader.rawQueryString.nonEmpty) "?" + requestHeader.rawQueryString else "")

      logger.debug(s"Redirecting to base path: $path to $redirectUrl")
      return Future.successful(Results.MovedPermanently(redirectUrl))
    }

    // Continue with normal processing
    nextFilter(requestHeader)
  }
}