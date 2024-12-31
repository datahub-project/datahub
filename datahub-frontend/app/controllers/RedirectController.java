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
