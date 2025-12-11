/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package auth.cookie

import com.google.inject.Inject
import play.api.http.{SecretConfiguration, SessionConfiguration}
import play.api.libs.crypto.CookieSigner
import play.api.mvc.DefaultSessionCookieBaker

import scala.collection.immutable.Map

/**
 * Overrides default fallback to URL Encoding behavior, prevents usage of old URL encoded session cookies
 * @param config
 * @param secretConfiguration
 * @param cookieSigner
 */
class CustomSessionCookieBaker @Inject() (
  override val config: SessionConfiguration,
  override val secretConfiguration: SecretConfiguration,
  cookieSigner: CookieSigner
) extends DefaultSessionCookieBaker(config, secretConfiguration, cookieSigner) {
  // Has to be a Scala class because it extends a trait with concrete implementations, Scala does compilation tricks

  // Forces use of jwt encoding and disallows fallback to legacy url encoding
  override def decode(encodedData: String): Map[String, String] = jwtCodec.decode(encodedData)
}
