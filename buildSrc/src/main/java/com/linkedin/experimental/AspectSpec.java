package com.linkedin.experimental;


public class AspectSpec {

    private final String _name;
    private final Boolean _isKey;
    private final AspectSearchSpec _searchSpec;
    private final AspectRelationshipSpec _relationshipSpec;

    public AspectSpec(final String name,
                      final Boolean isKey,
                      final AspectSearchSpec searchSpec,
                      final AspectRelationshipSpec relationshipSpec) {
        _name = name;
        _isKey = isKey;
        _searchSpec = searchSpec;
        _relationshipSpec = relationshipSpec;
    }

    public String getName() {
        return _name;
    }

    public Boolean isKey() {
        return _isKey;
    }

    public AspectSearchSpec getSearchSpec() {
        return _searchSpec;
    }

    public AspectRelationshipSpec getRelationshipSpec() {
        return _relationshipSpec;
    }

    public static class AspectSearchSpec {
        // Map<List<String>, SearchableFieldSpec>

    }

    public static class AspectRelationshipSpec {
        // Map<List<String>, RelationshipFieldSpec>

    }

}
