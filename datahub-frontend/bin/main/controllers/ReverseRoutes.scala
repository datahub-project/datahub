// @GENERATOR:play-routes-compiler
// @SOURCE:/home/jgarza/Development/datahub/datahub-frontend/conf/routes

import play.api.mvc.Call


import _root_.controllers.Assets.Asset

// @LINE:7
package controllers {

  // @LINE:41
  class ReverseTrackingController(_prefix: => String) {
    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:41
    def track(): Call = {
      
      Call("POST", _prefix + { _defaultPrefix } + "track")
    }
  
  }

  // @LINE:38
  class ReverseAssets(_prefix: => String) {
    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:38
    def at(file:String): Call = {
      implicit lazy val _rrc = new play.core.routing.ReverseRouteContext(Map(("path", "/public"))); _rrc
      Call("GET", _prefix + { _defaultPrefix } + "assets/" + implicitly[play.api.mvc.PathBindable[String]].unbind("file", file))
    }
  
  }

  // @LINE:16
  class ReverseAuthenticationController(_prefix: => String) {
    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:17
    def sso(): Call = {
      
      Call("GET", _prefix + { _defaultPrefix } + "sso")
    }
  
    // @LINE:16
    def authenticate(): Call = {
      
      Call("GET", _prefix + { _defaultPrefix } + "authenticate")
    }
  
    // @LINE:20
    def resetNativeUserCredentials(): Call = {
      
      Call("POST", _prefix + { _defaultPrefix } + "resetNativeUserCredentials")
    }
  
    // @LINE:19
    def signUp(): Call = {
      
      Call("POST", _prefix + { _defaultPrefix } + "signUp")
    }
  
    // @LINE:18
    def logIn(): Call = {
      
      Call("POST", _prefix + { _defaultPrefix } + "logIn")
    }
  
  }

  // @LINE:21
  class ReverseSsoCallbackController(_prefix: => String) {
    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:21
    def handleCallback(protocol:String): Call = {
    
      (protocol: @unchecked) match {
      
        // @LINE:21
        case (protocol)  =>
          
          Call("GET", _prefix + { _defaultPrefix } + "callback/" + play.core.routing.dynamicString(implicitly[play.api.mvc.PathBindable[String]].unbind("protocol", protocol)))
      
      }
    
    }
  
  }

  // @LINE:23
  class ReverseCentralLogoutController(_prefix: => String) {
    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:23
    def executeLogout(): Call = {
      
      Call("GET", _prefix + { _defaultPrefix } + "logOut")
    }
  
  }

  // @LINE:7
  class ReverseApplication(_prefix: => String) {
    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:7
    def index(path:String): Call = {
    
      (path: @unchecked) match {
      
        // @LINE:7
        case (path) if path == "index.html" =>
          implicit lazy val _rrc = new play.core.routing.ReverseRouteContext(Map(("path", "index.html"))); _rrc
          Call("GET", _prefix)
      
        // @LINE:44
        case (path)  =>
          
          Call("GET", _prefix + { _defaultPrefix } + implicitly[play.api.mvc.PathBindable[String]].unbind("path", path))
      
      }
    
    }
  
    // @LINE:9
    def healthcheck(): Call = {
    
      () match {
      
        // @LINE:9
        case ()  =>
          
          Call("GET", _prefix + { _defaultPrefix } + "admin")
      
      }
    
    }
  
    // @LINE:11
    def appConfig(): Call = {
      
      Call("GET", _prefix + { _defaultPrefix } + "config")
    }
  
    // @LINE:26
    def proxy(path:String): Call = {
    
      (path: @unchecked) match {
      
        // @LINE:26
        case (path)  =>
          
          Call("GET", _prefix + { _defaultPrefix } + "api/" + implicitly[play.api.mvc.PathBindable[String]].unbind("path", path))
      
      }
    
    }
  
  }


}
