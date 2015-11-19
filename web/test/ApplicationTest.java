import org.junit.*;

import play.mvc.*;

import static play.test.Helpers.*;
import static org.fest.assertions.Assertions.*;


public class ApplicationTest {

    private static String TEST_USER = "test";
    private static String FAKE_CSRF_TOKEN = "token";
    @Test
    public void renderTemplate() {
        Content html = views.html.index.render(TEST_USER, FAKE_CSRF_TOKEN);
        assertThat(contentType(html)).isEqualTo("text/html");
    }


}
