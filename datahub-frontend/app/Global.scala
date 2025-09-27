import play.api._
import play.api.mvc._
import play.api.mvc.Results._
import play.api.http.{DefaultHttpErrorHandler, Status}
import play.api.routing.Router
import javax.inject.{Inject, Provider, Singleton}
import scala.concurrent.Future

/**
 * Custom Global Settings for DataHub Frontend
 * Provides custom error pages and handles requests outside play.http.context
 */
@Singleton
class CustomHttpErrorHandler @Inject()(
  env: Environment,
  config: Configuration,
  sourceMapper: OptionalSourceMapper,
  router: Provider[Router]
) extends DefaultHttpErrorHandler(env, config, sourceMapper, router) {

  private lazy val logger = Logger(this.getClass)

  private lazy val basePath: String = {
    val contextPath = config.getOptional[String]("play.http.context").getOrElse("")
    val normalized = if (contextPath.trim.isEmpty) "" else contextPath.trim
    // Ensure base path ends with / for HTML base tag
    if (normalized.nonEmpty && !normalized.endsWith("/")) {
      normalized + "/"
    } else if (normalized.isEmpty) {
      "/"
    } else {
      normalized
    }
  }

  /**
   * Called when a client error occurs (4xx status codes)
   */
  override def onClientError(request: RequestHeader, statusCode: Int, message: String): Future[Result] = {
    logger.debug(s"Client error: ${statusCode} for ${request.uri} - ${message}")

    statusCode match {
      case Status.NOT_FOUND =>
        logger.debug(s"Serving custom 404 page for: ${request.uri}")
        Future.successful(
          NotFound(views.html.notFound(basePath, request.uri))
            .withHeaders("Cache-Control" -> "no-cache")
        )
      case Status.BAD_REQUEST =>
        Future.successful(
          BadRequest(views.html.serverError(basePath, s"Bad Request: $message"))
            .withHeaders("Cache-Control" -> "no-cache")
        )
      case Status.FORBIDDEN =>
        Future.successful(
          Forbidden(views.html.serverError(basePath, s"Forbidden: $message"))
            .withHeaders("Cache-Control" -> "no-cache")
        )
      case _ =>
        super.onClientError(request, statusCode, message)
    }
  }

  /**
   * Called when a server error occurs (5xx status codes)
   */
  override def onServerError(request: RequestHeader, exception: Throwable): Future[Result] = {
    logger.error(s"Server error for ${request.uri}", exception)

    val errorMessage = if (env.mode == Mode.Dev) {
      s"${exception.getClass.getSimpleName}: ${exception.getMessage}"
    } else {
      "An internal server error occurred"
    }

    Future.successful(
      InternalServerError(views.html.serverError(basePath, errorMessage))
        .withHeaders("Cache-Control" -> "no-cache")
    )
  }

  /**
   * Called when Play cannot find a handler for a request
   */
  override protected def onNotFound(request: RequestHeader, message: String): Future[Result] = {
    logger.debug(s"Handler not found for: ${request.uri}")

    Future.successful(
      NotFound(views.html.notFound(basePath, request.uri))
        .withHeaders("Cache-Control" -> "no-cache")
    )
  }

}