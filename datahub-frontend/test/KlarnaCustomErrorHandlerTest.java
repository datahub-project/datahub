import org.apache.shiro.crypto.CryptoException;
import org.junit.Test;
import play.api.UnexpectedException;
import play.api.UsefulException;
import scala.Option;

import static org.junit.Assert.*;

public class KlarnaCustomErrorHandlerTest {

    @Test
    public void test_isPotentialCookieAuthenticationError() {
        UsefulException cryptoException = new UnexpectedException(
                Option.apply("whatever message"),
                Option.apply(new CryptoException("irrelevant")));
        UsefulException generic401Exception = new UnexpectedException(
                Option.apply("Irrelevant prefix... Performing a 401 HTTP action. Irrelevant suffix."),
                Option.apply(new RuntimeException("irrelevant")));
        UsefulException totallyGenericException = new UnexpectedException(
                Option.apply("Something bad happened."),
                Option.apply(new RuntimeException("irrelevant")));

        assertTrue(KlarnaCustomErrorHandler.isPotentialCookieAuthenticationError(cryptoException));
        assertTrue(KlarnaCustomErrorHandler.isPotentialCookieAuthenticationError(generic401Exception));
        assertFalse(KlarnaCustomErrorHandler.isPotentialCookieAuthenticationError(totallyGenericException));
    }

}
