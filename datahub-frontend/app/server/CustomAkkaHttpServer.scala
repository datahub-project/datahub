/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package server

import akka.http.scaladsl.settings.ParserSettings
import play.api.Logger
import play.core.server.AkkaHttpServer
import play.core.server.ServerProvider

import scala.concurrent.Future

/** Custom Akka HTTP server that allows us to overrides some Akka server settings as the current Play / Akka
 *  versions we're using don't allow us to override these via conf files. Also handles base path redirects
 *  when play.http.context is configured.
 */
class CustomAkkaHttpServer(context: AkkaHttpServer.Context) extends AkkaHttpServer(context) {

  private lazy val logger = Logger(classOf[CustomAkkaHttpServer])

  private lazy val basePath: String = {
    val contextPath = context.config.configuration.getOptional[String]("play.http.context").getOrElse("")
    val normalized = if (contextPath.trim.isEmpty) "" else contextPath.trim
    logger.info(s"Base path configured as: '$normalized'")
    normalized
  }

  protected override def createParserSettings(): ParserSettings = {
    val defaultSettings: ParserSettings = super.createParserSettings()
    val maxHeaderCountKey = "play.http.server.akka.max-header-count"
    if (context.config.configuration.has(maxHeaderCountKey)) {
      val maxHeaderCount = context.config.configuration.get[Int](maxHeaderCountKey)
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

