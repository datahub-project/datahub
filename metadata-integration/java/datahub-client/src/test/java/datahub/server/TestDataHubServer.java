/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.server;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.*;

import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;

public class TestDataHubServer {

  private ClientAndServer mockServer;

  public Integer getPort() {
    return mockServer.getPort();
  }

  public ClientAndServer getMockServer() {
    return mockServer;
  }

  public TestDataHubServer() {
    mockServer = startClientAndServer();
    init();
  }

  public void init() {
    mockServer
        .when(
            request()
                .withMethod("GET")
                .withPath("/config")
                .withHeader("Content-type", "application/json"),
            Times.unlimited())
        .respond(org.mockserver.model.HttpResponse.response().withBody("{\"noCode\": true }"));
  }
}
