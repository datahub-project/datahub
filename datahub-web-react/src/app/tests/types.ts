import { Test } from '../../types.generated';

/**
 * A single property predicate as it appears in a deserialized
 * Test Definition.
 */
export interface Predicate {
    property: string;
    operator?: string;
    values?: string[];
}

/**
 * A conjunctive predicate as it appears in a deserialized
 * Test Definition.
 */
export type AndPredicate = {
    and: TestPredicate;
};

/**
 * A disjunctive predicate as it appears in a deserialized
 * Test Definition
 */
export type OrPredicate = {
    or: TestPredicate;
};

/**
 * An inverse predicate as it appears in a deserialized
 * Test Definition
 */
export type NotPredicate = {
    not: TestPredicate;
};

/**
 * A compound Test Predicate, which can be nested with various
 * sub predicates.
 */
export type TestPredicate = AndPredicate | OrPredicate | NotPredicate | Predicate | TestPredicate[];

/**
 * An object representation of the 'on' block in a deserialized
 * Test Definition.
 */
export type SelectPredicate = {
    types: string[];
    conditions?: TestPredicate;
};

/**
 * A single Action present in the actions clause of a deserialized
 * Test Definition.
 */
export type TestAction = {
    type: string;
    values: string[];
};

/**
 * A set of on success / on failure actions present in the actions clause
 * of a deserialized Test Definition.
 */
export type TestActions = {
    failing: TestAction[];
    passing: TestAction[];
};

/**
 * A deserialized Test Definition.
 */
export interface TestDefinition {
    /**
     * The select conditions for the test (or)
     */
    on: SelectPredicate;

    /**
     * The rules conditions for the test
     */
    rules: TestPredicate;

    /**
     * The actions for the test
     */
    actions?: TestActions;
}

export type TestCategoryType = { name: string; description?: string };
export type TestCategoryGroupType = { name: string; description?: string; tests: Test[] };
