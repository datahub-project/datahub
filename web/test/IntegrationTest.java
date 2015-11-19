import org.junit.*;

import play.libs.WS;

import static play.test.Helpers.*;
import static org.fest.assertions.Assertions.*;

public class IntegrationTest {

    @Test
    public void test() {

        running(testServer(3333), new Runnable() {
            public void run() {
                assertThat(WS.url("http://localhost:3333").get().get(1000*60).getStatus()).isEqualTo(OK);
            }
        });
    }

}
