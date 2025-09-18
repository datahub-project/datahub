package com.linkedin.metadata.utils.log;

import ch.qos.logback.classic.encoder.PatternLayoutEncoder;


public class SanitizingPatternLayoutEncoder extends PatternLayoutEncoder {
    @Override
    public void start() {
        SanitizingPatternLayout layout = new SanitizingPatternLayout();
        layout.setContext(getContext());
        layout.setPattern(getPattern());
        layout.start();
        this.layout = layout; // replace internal layout
        super.start();
    }
}
