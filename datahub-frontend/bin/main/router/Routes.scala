// @GENERATOR:play-routes-compiler
// @SOURCE:/home/jgarza/Development/datahub/datahub-frontend/conf/routes

package router

import play.core.routing._
import play.core.routing.HandlerInvokerFactory._

import play.api.mvc._

import _root_.controllers.Assets.Asset

class Routes(
  override val errorHandler: play.api.http.HttpErrorHandler, 
  // @LINE:7
  Application_1: controllers.Application,
  // @LINE:16
  AuthenticationController_0: controllers.AuthenticationController,
  // @LINE:21
  SsoCallbackController_5: controllers.SsoCallbackController,
  // @LINE:23
  CentralLogoutController_3: controllers.CentralLogoutController,
  // @LINE:38
  Assets_4: controllers.Assets,
  // @LINE:41
  TrackingController_2: controllers.TrackingController,
  val prefix: String
) extends GeneratedRouter {

   @javax.inject.Inject()
   def this(errorHandler: play.api.http.HttpErrorHandler,
    // @LINE:7
    Application_1: controllers.Application,
    // @LINE:16
    AuthenticationController_0: controllers.AuthenticationController,
    // @LINE:21
    SsoCallbackController_5: controllers.SsoCallbackController,
    // @LINE:23
    CentralLogoutController_3: controllers.CentralLogoutController,
    // @LINE:38
    Assets_4: controllers.Assets,
    // @LINE:41
    TrackingController_2: controllers.TrackingController
  ) = this(errorHandler, Application_1, AuthenticationController_0, SsoCallbackController_5, CentralLogoutController_3, Assets_4, TrackingController_2, "/")

  def withPrefix(addPrefix: String): Routes = {
    val prefix = play.api.routing.Router.concatPrefix(addPrefix, this.prefix)
    router.RoutesPrefix.setPrefix(prefix)
    new Routes(errorHandler, Application_1, AuthenticationController_0, SsoCallbackController_5, CentralLogoutController_3, Assets_4, TrackingController_2, prefix)
  }

  private[this] val defaultPrefix: String = {
    if (this.prefix.endsWith("/")) "" else "/"
  }

  def documentation = List(
    ("""GET""", this.prefix, """controllers.Application.index(path:String = "index.html")"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """admin""", """controllers.Application.healthcheck()"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """health""", """controllers.Application.healthcheck()"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """config""", """controllers.Application.appConfig()"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """authenticate""", """controllers.AuthenticationController.authenticate(request:Request)"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """sso""", """controllers.AuthenticationController.sso(request:Request)"""),
    ("""POST""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """logIn""", """controllers.AuthenticationController.logIn(request:Request)"""),
    ("""POST""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """signUp""", """controllers.AuthenticationController.signUp(request:Request)"""),
    ("""POST""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """resetNativeUserCredentials""", """controllers.AuthenticationController.resetNativeUserCredentials(request:Request)"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """callback/""" + "$" + """protocol<[^/]+>""", """controllers.SsoCallbackController.handleCallback(protocol:String, request:Request)"""),
    ("""POST""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """callback/""" + "$" + """protocol<[^/]+>""", """controllers.SsoCallbackController.handleCallback(protocol:String, request:Request)"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """logOut""", """controllers.CentralLogoutController.executeLogout(request:Request)"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """api/""" + "$" + """path<.+>""", """controllers.Application.proxy(path:String, request:Request)"""),
    ("""POST""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """api/""" + "$" + """path<.+>""", """controllers.Application.proxy(path:String, request:Request)"""),
    ("""DELETE""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """api/""" + "$" + """path<.+>""", """controllers.Application.proxy(path:String, request:Request)"""),
    ("""PUT""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """api/""" + "$" + """path<.+>""", """controllers.Application.proxy(path:String, request:Request)"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """openapi/""" + "$" + """path<.+>""", """controllers.Application.proxy(path:String, request:Request)"""),
    ("""POST""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """openapi/""" + "$" + """path<.+>""", """controllers.Application.proxy(path:String, request:Request)"""),
    ("""DELETE""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """openapi/""" + "$" + """path<.+>""", """controllers.Application.proxy(path:String, request:Request)"""),
    ("""PUT""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """openapi/""" + "$" + """path<.+>""", """controllers.Application.proxy(path:String, request:Request)"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """assets/""" + "$" + """file<.+>""", """controllers.Assets.at(path:String = "/public", file:String)"""),
    ("""POST""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """track""", """controllers.TrackingController.track(request:Request)"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """""" + "$" + """path<.+>""", """controllers.Application.index(path:String)"""),
    Nil
  ).foldLeft(List.empty[(String,String,String)]) { (s,e) => e.asInstanceOf[Any] match {
    case r @ (_,_,_) => s :+ r.asInstanceOf[(String,String,String)]
    case l => s ++ l.asInstanceOf[List[(String,String,String)]]
  }}


  // @LINE:7
  private[this] lazy val controllers_Application_index0_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix)))
  )
  private[this] lazy val controllers_Application_index0_invoker = createInvoker(
    Application_1.index(fakeValue[String]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "index",
      Seq(classOf[String]),
      "GET",
      this.prefix + """""",
      """ Home page
 serveAsset action requires a path string""",
      Seq()
    )
  )

  // @LINE:9
  private[this] lazy val controllers_Application_healthcheck1_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("admin")))
  )
  private[this] lazy val controllers_Application_healthcheck1_invoker = createInvoker(
    Application_1.healthcheck(),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "healthcheck",
      Nil,
      "GET",
      this.prefix + """admin""",
      """""",
      Seq()
    )
  )

  // @LINE:10
  private[this] lazy val controllers_Application_healthcheck2_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("health")))
  )
  private[this] lazy val controllers_Application_healthcheck2_invoker = createInvoker(
    Application_1.healthcheck(),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "healthcheck",
      Nil,
      "GET",
      this.prefix + """health""",
      """""",
      Seq()
    )
  )

  // @LINE:11
  private[this] lazy val controllers_Application_appConfig3_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("config")))
  )
  private[this] lazy val controllers_Application_appConfig3_invoker = createInvoker(
    Application_1.appConfig(),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "appConfig",
      Nil,
      "GET",
      this.prefix + """config""",
      """""",
      Seq()
    )
  )

  // @LINE:16
  private[this] lazy val controllers_AuthenticationController_authenticate4_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("authenticate")))
  )
  private[this] lazy val controllers_AuthenticationController_authenticate4_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      AuthenticationController_0.authenticate(fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.AuthenticationController",
      "authenticate",
      Seq(classOf[play.mvc.Http.Request]),
      "GET",
      this.prefix + """authenticate""",
      """ Authentication in React""",
      Seq()
    )
  )

  // @LINE:17
  private[this] lazy val controllers_AuthenticationController_sso5_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("sso")))
  )
  private[this] lazy val controllers_AuthenticationController_sso5_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      AuthenticationController_0.sso(fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.AuthenticationController",
      "sso",
      Seq(classOf[play.mvc.Http.Request]),
      "GET",
      this.prefix + """sso""",
      """""",
      Seq()
    )
  )

  // @LINE:18
  private[this] lazy val controllers_AuthenticationController_logIn6_route = Route("POST",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("logIn")))
  )
  private[this] lazy val controllers_AuthenticationController_logIn6_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      AuthenticationController_0.logIn(fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.AuthenticationController",
      "logIn",
      Seq(classOf[play.mvc.Http.Request]),
      "POST",
      this.prefix + """logIn""",
      """""",
      Seq()
    )
  )

  // @LINE:19
  private[this] lazy val controllers_AuthenticationController_signUp7_route = Route("POST",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("signUp")))
  )
  private[this] lazy val controllers_AuthenticationController_signUp7_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      AuthenticationController_0.signUp(fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.AuthenticationController",
      "signUp",
      Seq(classOf[play.mvc.Http.Request]),
      "POST",
      this.prefix + """signUp""",
      """""",
      Seq()
    )
  )

  // @LINE:20
  private[this] lazy val controllers_AuthenticationController_resetNativeUserCredentials8_route = Route("POST",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("resetNativeUserCredentials")))
  )
  private[this] lazy val controllers_AuthenticationController_resetNativeUserCredentials8_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      AuthenticationController_0.resetNativeUserCredentials(fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.AuthenticationController",
      "resetNativeUserCredentials",
      Seq(classOf[play.mvc.Http.Request]),
      "POST",
      this.prefix + """resetNativeUserCredentials""",
      """""",
      Seq()
    )
  )

  // @LINE:21
  private[this] lazy val controllers_SsoCallbackController_handleCallback9_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("callback/"), DynamicPart("protocol", """[^/]+""",true)))
  )
  private[this] lazy val controllers_SsoCallbackController_handleCallback9_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      SsoCallbackController_5.handleCallback(fakeValue[String], fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.SsoCallbackController",
      "handleCallback",
      Seq(classOf[String], classOf[play.mvc.Http.Request]),
      "GET",
      this.prefix + """callback/""" + "$" + """protocol<[^/]+>""",
      """""",
      Seq()
    )
  )

  // @LINE:22
  private[this] lazy val controllers_SsoCallbackController_handleCallback10_route = Route("POST",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("callback/"), DynamicPart("protocol", """[^/]+""",true)))
  )
  private[this] lazy val controllers_SsoCallbackController_handleCallback10_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      SsoCallbackController_5.handleCallback(fakeValue[String], fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.SsoCallbackController",
      "handleCallback",
      Seq(classOf[String], classOf[play.mvc.Http.Request]),
      "POST",
      this.prefix + """callback/""" + "$" + """protocol<[^/]+>""",
      """""",
      Seq()
    )
  )

  // @LINE:23
  private[this] lazy val controllers_CentralLogoutController_executeLogout11_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("logOut")))
  )
  private[this] lazy val controllers_CentralLogoutController_executeLogout11_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      CentralLogoutController_3.executeLogout(fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.CentralLogoutController",
      "executeLogout",
      Seq(classOf[play.mvc.Http.Request]),
      "GET",
      this.prefix + """logOut""",
      """""",
      Seq()
    )
  )

  // @LINE:26
  private[this] lazy val controllers_Application_proxy12_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("api/"), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_proxy12_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      Application_1.proxy(fakeValue[String], fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "proxy",
      Seq(classOf[String], classOf[play.mvc.Http.Request]),
      "GET",
      this.prefix + """api/""" + "$" + """path<.+>""",
      """ Proxies API requests to the metadata service api""",
      Seq()
    )
  )

  // @LINE:27
  private[this] lazy val controllers_Application_proxy13_route = Route("POST",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("api/"), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_proxy13_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      Application_1.proxy(fakeValue[String], fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "proxy",
      Seq(classOf[String], classOf[play.mvc.Http.Request]),
      "POST",
      this.prefix + """api/""" + "$" + """path<.+>""",
      """""",
      Seq()
    )
  )

  // @LINE:28
  private[this] lazy val controllers_Application_proxy14_route = Route("DELETE",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("api/"), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_proxy14_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      Application_1.proxy(fakeValue[String], fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "proxy",
      Seq(classOf[String], classOf[play.mvc.Http.Request]),
      "DELETE",
      this.prefix + """api/""" + "$" + """path<.+>""",
      """""",
      Seq()
    )
  )

  // @LINE:29
  private[this] lazy val controllers_Application_proxy15_route = Route("PUT",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("api/"), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_proxy15_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      Application_1.proxy(fakeValue[String], fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "proxy",
      Seq(classOf[String], classOf[play.mvc.Http.Request]),
      "PUT",
      this.prefix + """api/""" + "$" + """path<.+>""",
      """""",
      Seq()
    )
  )

  // @LINE:32
  private[this] lazy val controllers_Application_proxy16_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("openapi/"), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_proxy16_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      Application_1.proxy(fakeValue[String], fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "proxy",
      Seq(classOf[String], classOf[play.mvc.Http.Request]),
      "GET",
      this.prefix + """openapi/""" + "$" + """path<.+>""",
      """ Proxies API requests to the metadata service api""",
      Seq()
    )
  )

  // @LINE:33
  private[this] lazy val controllers_Application_proxy17_route = Route("POST",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("openapi/"), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_proxy17_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      Application_1.proxy(fakeValue[String], fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "proxy",
      Seq(classOf[String], classOf[play.mvc.Http.Request]),
      "POST",
      this.prefix + """openapi/""" + "$" + """path<.+>""",
      """""",
      Seq()
    )
  )

  // @LINE:34
  private[this] lazy val controllers_Application_proxy18_route = Route("DELETE",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("openapi/"), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_proxy18_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      Application_1.proxy(fakeValue[String], fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "proxy",
      Seq(classOf[String], classOf[play.mvc.Http.Request]),
      "DELETE",
      this.prefix + """openapi/""" + "$" + """path<.+>""",
      """""",
      Seq()
    )
  )

  // @LINE:35
  private[this] lazy val controllers_Application_proxy19_route = Route("PUT",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("openapi/"), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_proxy19_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      Application_1.proxy(fakeValue[String], fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "proxy",
      Seq(classOf[String], classOf[play.mvc.Http.Request]),
      "PUT",
      this.prefix + """openapi/""" + "$" + """path<.+>""",
      """""",
      Seq()
    )
  )

  // @LINE:38
  private[this] lazy val controllers_Assets_at20_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("assets/"), DynamicPart("file", """.+""",false)))
  )
  private[this] lazy val controllers_Assets_at20_invoker = createInvoker(
    Assets_4.at(fakeValue[String], fakeValue[String]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Assets",
      "at",
      Seq(classOf[String], classOf[String]),
      "GET",
      this.prefix + """assets/""" + "$" + """file<.+>""",
      """ Map static resources from the /public folder to the /assets URL path""",
      Seq()
    )
  )

  // @LINE:41
  private[this] lazy val controllers_TrackingController_track21_route = Route("POST",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("track")))
  )
  private[this] lazy val controllers_TrackingController_track21_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      TrackingController_2.track(fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.TrackingController",
      "track",
      Seq(classOf[play.mvc.Http.Request]),
      "POST",
      this.prefix + """track""",
      """ Analytics route""",
      Seq()
    )
  )

  // @LINE:44
  private[this] lazy val controllers_Application_index22_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_index22_invoker = createInvoker(
    Application_1.index(fakeValue[String]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "index",
      Seq(classOf[String]),
      "GET",
      this.prefix + """""" + "$" + """path<.+>""",
      """ Wildcard route accepts any routes and delegates to serveAsset which in turn serves the React Bundle""",
      Seq()
    )
  )


  def routes: PartialFunction[RequestHeader, Handler] = {
  
    // @LINE:7
    case controllers_Application_index0_route(params@_) =>
      call(Param[String]("path", Right("index.html"))) { (path) =>
        controllers_Application_index0_invoker.call(Application_1.index(path))
      }
  
    // @LINE:9
    case controllers_Application_healthcheck1_route(params@_) =>
      call { 
        controllers_Application_healthcheck1_invoker.call(Application_1.healthcheck())
      }
  
    // @LINE:10
    case controllers_Application_healthcheck2_route(params@_) =>
      call { 
        controllers_Application_healthcheck2_invoker.call(Application_1.healthcheck())
      }
  
    // @LINE:11
    case controllers_Application_appConfig3_route(params@_) =>
      call { 
        controllers_Application_appConfig3_invoker.call(Application_1.appConfig())
      }
  
    // @LINE:16
    case controllers_AuthenticationController_authenticate4_route(params@_) =>
      call { 
        controllers_AuthenticationController_authenticate4_invoker.call(
          req => AuthenticationController_0.authenticate(req))
      }
  
    // @LINE:17
    case controllers_AuthenticationController_sso5_route(params@_) =>
      call { 
        controllers_AuthenticationController_sso5_invoker.call(
          req => AuthenticationController_0.sso(req))
      }
  
    // @LINE:18
    case controllers_AuthenticationController_logIn6_route(params@_) =>
      call { 
        controllers_AuthenticationController_logIn6_invoker.call(
          req => AuthenticationController_0.logIn(req))
      }
  
    // @LINE:19
    case controllers_AuthenticationController_signUp7_route(params@_) =>
      call { 
        controllers_AuthenticationController_signUp7_invoker.call(
          req => AuthenticationController_0.signUp(req))
      }
  
    // @LINE:20
    case controllers_AuthenticationController_resetNativeUserCredentials8_route(params@_) =>
      call { 
        controllers_AuthenticationController_resetNativeUserCredentials8_invoker.call(
          req => AuthenticationController_0.resetNativeUserCredentials(req))
      }
  
    // @LINE:21
    case controllers_SsoCallbackController_handleCallback9_route(params@_) =>
      call(params.fromPath[String]("protocol", None)) { (protocol) =>
        controllers_SsoCallbackController_handleCallback9_invoker.call(
          req => SsoCallbackController_5.handleCallback(protocol, req))
      }
  
    // @LINE:22
    case controllers_SsoCallbackController_handleCallback10_route(params@_) =>
      call(params.fromPath[String]("protocol", None)) { (protocol) =>
        controllers_SsoCallbackController_handleCallback10_invoker.call(
          req => SsoCallbackController_5.handleCallback(protocol, req))
      }
  
    // @LINE:23
    case controllers_CentralLogoutController_executeLogout11_route(params@_) =>
      call { 
        controllers_CentralLogoutController_executeLogout11_invoker.call(
          req => CentralLogoutController_3.executeLogout(req))
      }
  
    // @LINE:26
    case controllers_Application_proxy12_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_proxy12_invoker.call(
          req => Application_1.proxy(path, req))
      }
  
    // @LINE:27
    case controllers_Application_proxy13_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_proxy13_invoker.call(
          req => Application_1.proxy(path, req))
      }
  
    // @LINE:28
    case controllers_Application_proxy14_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_proxy14_invoker.call(
          req => Application_1.proxy(path, req))
      }
  
    // @LINE:29
    case controllers_Application_proxy15_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_proxy15_invoker.call(
          req => Application_1.proxy(path, req))
      }
  
    // @LINE:32
    case controllers_Application_proxy16_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_proxy16_invoker.call(
          req => Application_1.proxy(path, req))
      }
  
    // @LINE:33
    case controllers_Application_proxy17_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_proxy17_invoker.call(
          req => Application_1.proxy(path, req))
      }
  
    // @LINE:34
    case controllers_Application_proxy18_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_proxy18_invoker.call(
          req => Application_1.proxy(path, req))
      }
  
    // @LINE:35
    case controllers_Application_proxy19_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_proxy19_invoker.call(
          req => Application_1.proxy(path, req))
      }
  
    // @LINE:38
    case controllers_Assets_at20_route(params@_) =>
      call(Param[String]("path", Right("/public")), params.fromPath[String]("file", None)) { (path, file) =>
        controllers_Assets_at20_invoker.call(Assets_4.at(path, file))
      }
  
    // @LINE:41
    case controllers_TrackingController_track21_route(params@_) =>
      call { 
        controllers_TrackingController_track21_invoker.call(
          req => TrackingController_2.track(req))
      }
  
    // @LINE:44
    case controllers_Application_index22_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_index22_invoker.call(Application_1.index(path))
      }
  }
}
