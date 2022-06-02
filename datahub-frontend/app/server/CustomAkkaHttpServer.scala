package server

import play.api.Logger
import play.core.server.AkkaHttpServer
import play.core.server.AkkaHttpServerProvider
import play.core.server.ServerProvider
import akka.http.scaladsl.settings.ParserSettings

/** Custom Akka HTTP server that allows us to overrides some Akka server settings as the current Play / Akka
 *  versions we're using don't allow us to override these via conf files
 */
class CustomAkkaHttpServer(context: AkkaHttpServer.Context) extends AkkaHttpServer(context) {

  protected override def createParserSettings(): ParserSettings = {
    val defaultSettings: ParserSettings = super.createParserSettings()
    val maxHeaderCountKey = "play.http.server.akka.max-header-count"
    if (context.config.configuration.has(maxHeaderCountKey)) {
      val maxHeaderCount = context.config.configuration.get[Int](maxHeaderCountKey)
      val logger = Logger(classOf[CustomAkkaHttpServer])
      logger.info(s"Setting max header count to: $maxHeaderCount")
      defaultSettings.withMaxHeaderCount(maxHeaderCount)
    } else
      defaultSettings
  }
}

/** A factory that instantiates a CustomAkkaHttpServer. */
class CustomAkkaHttpServerProvider extends ServerProvider {
  def createServer(context: ServerProvider.Context) = {
    val serverContext = AkkaHttpServer.Context.fromServerProviderContext(context)
    new CustomAkkaHttpServer(serverContext)
  }
}

