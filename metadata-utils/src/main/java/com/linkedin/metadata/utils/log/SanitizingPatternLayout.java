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

    @Override
    public String doLayout(ILoggingEvent event) {
        String output = super.doLayout(event);
        return sanitize(output);
    }

    private String sanitize(String input) {
        if (input == null) return null;

        String cleaned = input;

        // Replace carriage returns with underscores
        cleaned = CARRIAGE_RETURN.matcher(cleaned).replaceAll("_");

        // Replace tabs with 4 spaces
        cleaned = TAB.matcher(cleaned).replaceAll("    ");

        // Collapse multiple newlines into a single newline
        cleaned = MULTIPLE_NEWLINES.matcher(cleaned).replaceAll("\n");

        // Remove ANSI escape codes
        cleaned = ANSI_PATTERN.matcher(cleaned).replaceAll("");

        return cleaned;
    }
}
