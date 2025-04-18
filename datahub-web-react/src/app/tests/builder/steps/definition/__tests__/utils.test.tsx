import { deserializeTestDefinition, serializeTestDefinition, validateJsonDefinition } from '../utils';
import { DEFAULT_TEST_DEFINITION } from '../../../types';

const MISSING_ON = `
{
    "rules": []
}
`;

const MISSING_RULES = `
{
    "on": {
        "types": ["dataset"]
    }
}
`;

const MISSING_CONDITIONS = `
{
    "on": {
        "types": ["dataset"]
    },
    "rules": [ { "property": "test", "operator": "exists" } ]
}
`;

const MISSING_TYPES_CONDITIONS = `
{
    "on": {
        "conditions": []
    },
    "rules": [ { "property": "test", "operator": "exists" }]
}
`;

const EMPTY_RULES_CONDITIONS = `
{
    "on": {
        "types": ["dataset"],
        "conditions": []
    },
    "rules": []
}
`;

const EMPTY_TYPES = `
{
    "on": {
        "types": [],
        "conditions": [ { "property": "test", "operator": "exists" } ]
    },
    "rules": [ { "property": "test", "operator": "exists" } ]
}
`;

const RULES_CONDITIONS_OBJECTS_1 = `
{
    "on": {
        "types": ["dataset"],
        "conditions": { "property": "test", "operator": "exists" }
    },
    "rules": { "and": [ { "property": "test", "operator": "exists" } ] }
}
`;

const RULES_CONDITIONS_OBJECTS_2 = `
{
    "on": {
        "types": ["dataset"],
        "conditions": { "or": { "property": "test", "operator": "exists" } }
    },
    "rules": { "not": { "property": "test", "operator": "exists" } }
}
`;

const LEGACY_FIELDS = `
{
    "on": {
        "types": [],
        "match": [ { "query": "test", "operation": "equals", "params": { "values": [ "value" ] } } ]
    },
    "rules": [ { "query": "test", "operation": "exists" } ]
}
`;

const LEGACY_FIELDS_TRANSFORMED = `
{
    "on": {
        "types": [],
        "conditions": [{ "property": "test", "operator": "equals", "values": [ "value" ] } ]
    },
    "rules": [ { "property": "test", "operator": "exists" } ]
}
`;

const COMPLEX_PREDICATES = `
{
    "on": {
        "types": ["dataset", "dashboard"],
        "conditions": [
            { 
                "or": [
                    {
                        "property": "test",
                        "operator": "exists"
                    },
                    {
                        "and": [
                            {
                                "query": "test2",
                                "operation": "equals",
                                "params": {
                                    "value": "value"
                                }
                            }
                        ]
                    }
                ] 
            } 
        ]
    },
    "rules": { 
        "not": [
            {
                "property": "test",
                "operation": "exists"
            },
            {
                "and": [
                    {
                        "property": "test3",
                        "operator": "greater_than",
                        "values": ["1"]
                    },
                    {
                        "query": "test4",
                        "operator": "greater_than",
                        "values": ["1"]
                    }
                ]
            }
        ]
     }
}
`;

const COMPLEX_PREDICATES_TRANSFORMED = `
{
    "on": {
        "types": ["dataset", "dashboard"],
        "conditions": [
            { 
                "or": [
                    {
                        "property": "test",
                        "operator": "exists"
                    },
                    {
                        "and": [
                            {
                                "property": "test2",
                                "operator": "equals",
                                "values": [ "value" ]
                            }
                        ]
                    }
                ] 
            } 
        ]
    },
    "rules": { 
        "not": [
            {
                "property": "test",
                "operator": "exists"
            },
            {
                "and": [
                    {
                        "property": "test3",
                        "operator": "greater_than",
                        "values": ["1"]
                    },
                    {
                        "property": "test4",
                        "operator": "greater_than",
                        "values": ["1"]
                    }
                ]
            }
        ]
     }
}
`;

const UNKNOWN_FIELDS = `
{
    "on": {
        "types": ["dataset"]
    },
    "rules": [ { "property": "test", "operator": "exists", "someField": "value" } ],
    "someOtherField": "VALUE"
}
`;

const UNKNOWN_FIELDS_TRANSFORMED = `
{
    "on": {
        "types": ["dataset"]
    },
    "rules": [ { "property": "test", "operator": "exists" } ],
    "someOtherField": "VALUE"
}
`;

const MALFORMED_JSON = `
{
    "on": asd{
        "types": [],
        "maatch": [{ "query": "test", "operation": "equals", "params": { "values": [ "value" ]} }]
    },
    "rules":a [ { "query"a: "test", "operation": "exists" }]
}
`;

describe('deserializeTestDefinition', () => {
    it('default test for missing on', () => {
        expect(deserializeTestDefinition(MISSING_ON)).toEqual(DEFAULT_TEST_DEFINITION);
    });
    it('default test for missing rules', () => {
        expect(deserializeTestDefinition(MISSING_RULES)).toEqual(DEFAULT_TEST_DEFINITION);
    });
    it('default test for missing types', () => {
        expect(deserializeTestDefinition(MISSING_TYPES_CONDITIONS)).toEqual(DEFAULT_TEST_DEFINITION);
    });
    it('supports missing conditions', () => {
        expect(deserializeTestDefinition(MISSING_CONDITIONS)).toEqual(JSON.parse(MISSING_CONDITIONS));
    });
    it('supports empty rules and conditions', () => {
        expect(deserializeTestDefinition(EMPTY_RULES_CONDITIONS)).toEqual(JSON.parse(EMPTY_RULES_CONDITIONS));
    });
    it('supports rules and conditions objects', () => {
        expect(deserializeTestDefinition(RULES_CONDITIONS_OBJECTS_1)).toEqual(JSON.parse(RULES_CONDITIONS_OBJECTS_1));
        expect(deserializeTestDefinition(RULES_CONDITIONS_OBJECTS_2)).toEqual(JSON.parse(RULES_CONDITIONS_OBJECTS_2));
    });
    it('supports legacy fields', () => {
        expect(deserializeTestDefinition(LEGACY_FIELDS)).toEqual(JSON.parse(LEGACY_FIELDS_TRANSFORMED));
    });
    it('supports empty types', () => {
        expect(deserializeTestDefinition(EMPTY_TYPES)).toEqual(JSON.parse(EMPTY_TYPES));
    });
    it('supports complex predicates', () => {
        expect(deserializeTestDefinition(COMPLEX_PREDICATES)).toEqual(JSON.parse(COMPLEX_PREDICATES_TRANSFORMED));
    });
    it('ignores unknown fields', () => {
        expect(deserializeTestDefinition(UNKNOWN_FIELDS)).toEqual(JSON.parse(UNKNOWN_FIELDS_TRANSFORMED));
    });
    it('default test for malformed json', () => {
        expect(deserializeTestDefinition(MALFORMED_JSON)).toEqual(DEFAULT_TEST_DEFINITION);
    });
});

const MISSING_ON_WITH_DEFAULT = `
{
    "on": { "types": [] },
    "rules": []
}
`;

const MISSING_TYPES_WITH_DEFAULT = `
{
    "on": { "conditions": [], "types": [] },
    "rules": [ { "property": "test", "operator": "exists" }]
}
`;

const MISSING_RULES_WITH_DEFAULT = `
{
    "on": {
        "types": ["dataset"]
    },
    "rules": []
}
`;

describe('serializeTestDefinition', () => {
    it('provides default for missing on', () => {
        expect(serializeTestDefinition(JSON.parse(MISSING_ON))).toEqual(
            JSON.stringify(JSON.parse(MISSING_ON_WITH_DEFAULT)),
        );
    });
    it('provides default for missing types', () => {
        expect(serializeTestDefinition(JSON.parse(MISSING_TYPES_CONDITIONS))).toEqual(
            JSON.stringify(JSON.parse(MISSING_TYPES_WITH_DEFAULT)),
        );
    });
    it('provides default for missing rules', () => {
        expect(serializeTestDefinition(JSON.parse(MISSING_RULES))).toEqual(
            JSON.stringify(JSON.parse(MISSING_RULES_WITH_DEFAULT)),
        );
    });
});

describe('validateTestDefinition', () => {
    it('requires an on block', () => {
        expect(validateJsonDefinition(MISSING_ON).isValid).toEqual(false);
    });
    it('requires a types block', () => {
        expect(validateJsonDefinition(MISSING_TYPES_CONDITIONS).isValid).toEqual(false);
    });
    it('requires a rules block', () => {
        expect(validateJsonDefinition(MISSING_RULES).isValid).toEqual(false);
    });
    it('is valid', () => {
        expect(validateJsonDefinition(COMPLEX_PREDICATES_TRANSFORMED).isValid).toEqual(true);
    });
    it('ignores unknown fields', () => {
        expect(validateJsonDefinition(UNKNOWN_FIELDS).isValid).toEqual(true);
    });
});
