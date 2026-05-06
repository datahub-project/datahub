package server

import org.apache.pekko.http.scaladsl.settings.ParserSettings
import play.api.Logger
import play.core.server.PekkoHttpServer
import play.core.server.ServerProvider

/** Custom Pekko HTTP server to override parser settings not exposed via application.conf,
  * and base-path behavior when `play.http.context` is set.
  */
class CustomPekkoHttpServer(context: PekkoHttpServer.Context) extends PekkoHttpServer(context) {

  private lazy val logger = Logger(classOf[CustomPekkoHttpServer])

  private lazy val basePath: String = {
    val contextPath = context.config.configuration.getOptional[String]("play.http.context").getOrElse("")
    val normalized = if (contextPath.trim.isEmpty) "" else contextPath.trim
    logger.info(s"Base path configured as: '$normalized'")
    normalized
  }

  protected override def createParserSettings(): ParserSettings = {
    val defaultSettings: ParserSettings = super.createParserSettings()
    val maxHeaderCountKey = "play.http.server.pekko.max-header-count"
    if (context.config.configuration.has(maxHeaderCountKey)) {
      val maxHeaderCount = context.config.configuration.get[Int](maxHeaderCountKey)
      logger.info(s"Setting max header count to: $maxHeaderCount")
      defaultSettings.withMaxHeaderCount(maxHeaderCount)
    } else
      defaultSettings
  }

}

class CustomPekkoHttpServerProvider extends ServerProvider {
  def createServer(context: ServerProvider.Context) = {
    val serverContext = PekkoHttpServer.Context.fromServerProviderContext(context)
    new CustomPekkoHttpServer(serverContext)
  }
}
