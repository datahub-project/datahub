package com.linkedin.experimental;

import java.util.List;

public class RelationshipFieldSpec {
    private final String _name; // Relationship Name
    private final List<String> _validKeyTypes;

    public RelationshipFieldSpec(final String name, final List<String> validKeyTypes) {
        _name = name;
        _validKeyTypes = validKeyTypes;
    }

    public String getName() {
        return _name;
    }

    public List<String> getValidKeyTypes() {
        return _validKeyTypes;
    }
}
