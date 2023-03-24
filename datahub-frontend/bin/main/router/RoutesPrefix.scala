// @GENERATOR:play-routes-compiler
// @SOURCE:/home/jgarza/Development/datahub/datahub-frontend/conf/routes


package router {
  object RoutesPrefix {
    private var _prefix: String = "/"
    def setPrefix(p: String): Unit = {
      _prefix = p
    }
    def prefix: String = _prefix
    val byNamePrefix: Function0[String] = { () => prefix }
  }
}
