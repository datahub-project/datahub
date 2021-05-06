package com.linkedin.experimental;

import java.util.List;

public class SearchableFieldSpec {
    private final String _analyzerType; // Should be an enum or something otherwise validated.
    private final List<String> _fieldPath;

    public SearchableFieldSpec(final String analyzerType, final List<String> fieldPath) {
        _analyzerType = analyzerType;
        _fieldPath = fieldPath;
    }

    public String getAnalyzerType() {
        return _analyzerType;
    }

    public List<String> getFieldPath() {
        return _fieldPath;
    }
}