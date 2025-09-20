package com.linkedin.metadata.utils.log;

import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;

import java.util.regex.Pattern;

public class SanitizingPatternLayout extends PatternLayout {

    // Precompiled regex patterns
    private static final Pattern MULTIPLE_NEWLINES = Pattern.compile("\n{2,}");
    private static final Pattern ANSI_PATTERN = Pattern.compile("\u001B\\[[;\\d]*[ -/]*[@-~]");
    private static final Pattern CARRIAGE_RETURN = Pattern.compile("\r");
    private static final Pattern TAB = Pattern.compile("\t");

    // Config: sanitization enabled flag
    private final boolean sanitizeEnabled;

    public SanitizingPatternLayout() {
        // Read from environment variable or system property
        // Default = false
        String configValue = System.getenv().getOrDefault("DATAHUB_SANITIZE_LOGS",
                System.getProperty("datahub.sanitize.logs", "false"));
        this.sanitizeEnabled = Boolean.parseBoolean(configValue);
    }

    @Override
    public String doLayout(ILoggingEvent event) {
        String output = super.doLayout(event);
        if (sanitizeEnabled) {
            return sanitize(output);
        }
        return output; // Return untouched logs if disabled
    }

    private String sanitize(String input) {
        if (input == null) return null;

        String cleaned = input;
        cleaned = CARRIAGE_RETURN.matcher(cleaned).replaceAll("_");   // Replace carriage returns
        cleaned = TAB.matcher(cleaned).replaceAll("    ");            // Replace tabs with spaces
        cleaned = MULTIPLE_NEWLINES.matcher(cleaned).replaceAll("\n");// Collapse multiple newlines
        cleaned = ANSI_PATTERN.matcher(cleaned).replaceAll("");       // Remove ANSI escape codes

        return cleaned;
    }
}
