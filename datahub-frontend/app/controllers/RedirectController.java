/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package controllers;

import config.ConfigurationProvider;
import javax.inject.Inject;
import javax.inject.Singleton;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.Result;

@Singleton
public class RedirectController extends Controller {

  @Inject ConfigurationProvider config;

  public Result favicon(Http.Request request) {
    if (config.getVisualConfig().getAssets().getFaviconUrl().startsWith("http")) {
      return permanentRedirect(config.getVisualConfig().getAssets().getFaviconUrl());
    } else {
      final String prefix =
          config.getVisualConfig().getAssets().getFaviconUrl().startsWith("/")
              ? "/public"
              : "/public/";
      return ok(Application.class.getResourceAsStream(
              prefix + config.getVisualConfig().getAssets().getFaviconUrl()))
          .as("image/x-icon");
    }
  }
}
