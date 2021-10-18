import com.typesafe.config.Config;

import org.apache.shiro.crypto.CryptoException;
import play.*;
import play.api.OptionalSourceMapper;
import play.api.UsefulException;
import play.api.routing.Router;
import play.http.DefaultHttpErrorHandler;
import play.mvc.Http.*;
import play.mvc.*;
import play.twirl.api.Html;

import javax.inject.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Responsible for additional specialized behaviour for specific errors for which the default behaviour is not
 * useful enough for the end users.
 */
@Singleton
public class KlarnaCustomErrorHandler extends DefaultHttpErrorHandler {

    @Inject
    public KlarnaCustomErrorHandler(
            Config config,
            Environment environment,
            OptionalSourceMapper sourceMapper,
            Provider<Router> routes) {
        super(config, environment, sourceMapper, routes);
    }

    @Override
    protected CompletionStage<Result> onProdServerError(RequestHeader request, UsefulException exception) {
        if (isPotentialCookieAuthenticationError(exception)) {
            return CompletableFuture.completedFuture(
                    Results.internalServerError(
                            Html.apply(
                                    getCustomErrorPage("Internal server error.<br/>" +
                                            "Please ensure that you have been granted access to the service. <br/>" +
                                            "If you believe you should have access please try to clear your browser cookies and reload the page."))));
        }

        return super.onProdServerError(request, exception);
    }

    /**
     * Determine whether a certain exception could be caused by a stale play session cookie
     *
     * protected for testing visibility
     *
     * @param exception the exception which has been thrown
     * @return the decision based on the exception
     */
    protected static boolean isPotentialCookieAuthenticationError(UsefulException exception) {
        if( exception.cause instanceof CryptoException )
            return true;
        else if( exception.getMessage().contains("Performing a 401 HTTP action") )
            return true;

        return false;
    }

    /**
     * Stolen with pride from https://github.com/playframework/playframework/blob/2.6.x/core/play/src/main/scala/views/defaultpages/error.scala.html
     *
     * @param message the message that will be displayed to the end user.
     * @return a full HTML page
     */
    private String getCustomErrorPage(String message) {
        return "<!DOCTYPE html>\n" +
                "<html lang=\"en\">\n" +
                "    <head>\n" +
                "        <title>Error</title>\n" +
                "        <style>\n" +
                "            html, body, pre {\n" +
                "                margin: 0;\n" +
                "                padding: 0;\n" +
                "                font-family: Monaco, 'Lucida Console', monospace;\n" +
                "                background: #ECECEC;\n" +
                "            }\n" +
                "            h1 {\n" +
                "                margin: 0;\n" +
                "                background: #A31012;\n" +
                "                padding: 20px 45px;\n" +
                "                color: #fff;\n" +
                "                text-shadow: 1px 1px 1px rgba(0,0,0,.3);\n" +
                "                border-bottom: 1px solid #690000;\n" +
                "                font-size: 28px;\n" +
                "            }\n" +
                "            p#detail {\n" +
                "                margin: 0;\n" +
                "                padding: 15px 45px;\n" +
                "                background: #F5A0A0;\n" +
                "                border-top: 4px solid #D36D6D;\n" +
                "                color: #730000;\n" +
                "                text-shadow: 1px 1px 1px rgba(255,255,255,.3);\n" +
                "                font-size: 14px;\n" +
                "                border-bottom: 1px solid #BA7A7A;\n" +
                "            }\n" +
                "        </style>\n" +
                "    </head>\n" +
                "    <body>\n" +
                "        <h1>Oops, an error occurred</h1>\n" +
                "\n" +
                "        <p id=\"detail\">\n" +
                message + "\n" +
                "        </p>\n" +
                "\n" +
                "    </body>\n" +
                "</html>";
    }
}