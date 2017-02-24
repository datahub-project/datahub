import org.junit.*;

import play.twirl.api.Content;


import static play.test.Helpers.*;
import static org.fest.assertions.api.Assertions.*;


public class ApplicationTest {

    private static String TEST_USER = "test";
    private static String FAKE_CSRF_TOKEN = "token";
    @Test
    public void renderTemplate() {
        Content html = views.html.index.render(TEST_USER, FAKE_CSRF_TOKEN, true, 1234);
        assertThat(contentType(html)).isEqualTo("text/html");
    }


}
