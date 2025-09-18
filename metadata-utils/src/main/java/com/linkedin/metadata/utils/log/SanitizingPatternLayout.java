package com.linkedin.metadata.utils.log;

import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;

public class SanitizingPatternLayout extends PatternLayout {

    @Override
    public String doLayout(ILoggingEvent event) {
        String output = super.doLayout(event);
        return sanitize(output);
    }

    private String sanitize(String input) {
        if (input == null) return null;
        String cleaned = input.replace("\r", "_")
                .replaceAll("\n{2,}", "\n") // collapse multiple newlines
                .replace("\t", "    ");
        cleaned = cleaned.replaceAll("\u001B\\[[;\\d]*[ -/]*[@-~]", ""); // remove ANSI
        return cleaned;
    }
}
