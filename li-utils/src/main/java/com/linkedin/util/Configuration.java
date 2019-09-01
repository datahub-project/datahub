package com.linkedin.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration {
    public static Properties loadProperties(String configFile) {
        Properties configuration = new Properties();
        try (InputStream inputStream = Configuration.class.getClassLoader().getResourceAsStream(configFile)) {
            configuration.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException("Can't read file: " + configFile);
        }
        return configuration;
    }
}
