/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.client.kafka.containers;

import java.io.IOException;
import java.net.ServerSocket;

final class Utils {
  public static final String CONFLUENT_PLATFORM_VERSION = "8.0.0";

  private Utils() {}

  /**
   * Retrieves a random port that is currently not in use on this machine.
   *
   * @return a free port
   * @throws IOException wraps the exceptions which may occur during this method call.
   */
  static int getRandomFreePort() throws IOException {
    @SuppressWarnings("resource")
    ServerSocket serverSocket = new ServerSocket(0);
    return serverSocket.getLocalPort();
  }
}
