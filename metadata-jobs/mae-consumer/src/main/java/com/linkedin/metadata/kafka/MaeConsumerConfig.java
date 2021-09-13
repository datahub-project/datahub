package com.linkedin.metadata.kafka;

import java.util.HashMap;
import java.util.Map;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class MaeConsumerConfig {
    Map<String, String> config = new HashMap<String, String>() {{
        put("noCode", "true");
    }};

    @GetMapping("/config")
    @ResponseBody
    public Map<String, String> sayHello() {
      return config;
    }
}
