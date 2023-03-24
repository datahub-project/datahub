// @GENERATOR:play-routes-compiler
// @SOURCE:/home/jgarza/Development/datahub/datahub-frontend/conf/routes

import play.api.routing.JavaScriptReverseRoute


import _root_.controllers.Assets.Asset

// @LINE:7
package controllers.javascript {

  // @LINE:41
  class ReverseTrackingController(_prefix: => String) {

    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:41
    def track: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.TrackingController.track",
      """
        function() {
          return _wA({method:"POST", url:"""" + _prefix + { _defaultPrefix } + """" + "track"})
        }
      """
    )
  
  }

  // @LINE:38
  class ReverseAssets(_prefix: => String) {

    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:38
    def at: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.Assets.at",
      """
        function(file1) {
          return _wA({method:"GET", url:"""" + _prefix + { _defaultPrefix } + """" + "assets/" + (""" + implicitly[play.api.mvc.PathBindable[String]].javascriptUnbind + """)("file", file1)})
        }
      """
    )
  
  }

  // @LINE:16
  class ReverseAuthenticationController(_prefix: => String) {

    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:17
    def sso: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.AuthenticationController.sso",
      """
        function() {
          return _wA({method:"GET", url:"""" + _prefix + { _defaultPrefix } + """" + "sso"})
        }
      """
    )
  
    // @LINE:16
    def authenticate: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.AuthenticationController.authenticate",
      """
        function() {
          return _wA({method:"GET", url:"""" + _prefix + { _defaultPrefix } + """" + "authenticate"})
        }
      """
    )
  
    // @LINE:20
    def resetNativeUserCredentials: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.AuthenticationController.resetNativeUserCredentials",
      """
        function() {
          return _wA({method:"POST", url:"""" + _prefix + { _defaultPrefix } + """" + "resetNativeUserCredentials"})
        }
      """
    )
  
    // @LINE:19
    def signUp: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.AuthenticationController.signUp",
      """
        function() {
          return _wA({method:"POST", url:"""" + _prefix + { _defaultPrefix } + """" + "signUp"})
        }
      """
    )
  
    // @LINE:18
    def logIn: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.AuthenticationController.logIn",
      """
        function() {
          return _wA({method:"POST", url:"""" + _prefix + { _defaultPrefix } + """" + "logIn"})
        }
      """
    )
  
  }

  // @LINE:21
  class ReverseSsoCallbackController(_prefix: => String) {

    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:21
    def handleCallback: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.SsoCallbackController.handleCallback",
      """
        function(protocol0) {
        
          if (true) {
            return _wA({method:"GET", url:"""" + _prefix + { _defaultPrefix } + """" + "callback/" + encodeURIComponent((""" + implicitly[play.api.mvc.PathBindable[String]].javascriptUnbind + """)("protocol", protocol0))})
          }
        
        }
      """
    )
  
  }

  // @LINE:23
  class ReverseCentralLogoutController(_prefix: => String) {

    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:23
    def executeLogout: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.CentralLogoutController.executeLogout",
      """
        function() {
          return _wA({method:"GET", url:"""" + _prefix + { _defaultPrefix } + """" + "logOut"})
        }
      """
    )
  
  }

  // @LINE:7
  class ReverseApplication(_prefix: => String) {

    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:7
    def index: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.Application.index",
      """
        function(path0) {
        
          if (path0 == """ + implicitly[play.api.mvc.JavascriptLiteral[String]].to("index.html") + """) {
            return _wA({method:"GET", url:"""" + _prefix + """"})
          }
        
          if (true) {
            return _wA({method:"GET", url:"""" + _prefix + { _defaultPrefix } + """" + (""" + implicitly[play.api.mvc.PathBindable[String]].javascriptUnbind + """)("path", path0)})
          }
        
        }
      """
    )
  
    // @LINE:9
    def healthcheck: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.Application.healthcheck",
      """
        function() {
        
          if (true) {
            return _wA({method:"GET", url:"""" + _prefix + { _defaultPrefix } + """" + "admin"})
          }
        
        }
      """
    )
  
    // @LINE:11
    def appConfig: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.Application.appConfig",
      """
        function() {
          return _wA({method:"GET", url:"""" + _prefix + { _defaultPrefix } + """" + "config"})
        }
      """
    )
  
    // @LINE:26
    def proxy: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.Application.proxy",
      """
        function(path0) {
        
          if (true) {
            return _wA({method:"GET", url:"""" + _prefix + { _defaultPrefix } + """" + "api/" + (""" + implicitly[play.api.mvc.PathBindable[String]].javascriptUnbind + """)("path", path0)})
          }
        
        }
      """
    )
  
  }


}
