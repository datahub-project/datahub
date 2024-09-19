import { ComponentType } from 'react';
import { Test } from '@src/types.generated';
import { AutomationTypes } from './constants';

/**
 * Component Base Type (props)
 */
export type ComponentBaseProps = {
    state: any;
    passStateToParent: (newState: any) => void;
    props: any;
};

/**
 * Type used to standardize the list of automations regardless of if their a Test or Action.
 */
export interface ListAutomationItem {
    key: string;
    urn: string;
    name: string;
    description: string;
    category: string;
    definition: string;
    type: AutomationTypes;
    updated: Date;
    created: Date;
}

export type Connection = {
    urn: string;
    data: {
        // urn: string;  // urn will eventually be moved here
        account_id: string;
        name: string;
        password: string;
        role: string;
        warehouse: string;
        username: string;
        database: string;
        schema: string;
    };
};

export enum RequirementRule {
    NOT_EMPTY_STRING = 'notEmptyString',
    NOT_EMPTY_ARRAY = 'notEmptyArray',
    NOT_EMPTY_OBJECT = 'notEmptyObject',
    NOT_EMPTY_NUMBER = 'notEmptyNumber',
    NOT_NULL = 'notNull',
}

export interface Field<T = any> {
    title: string;
    description?: string;
    tooltop?: string;
    fields: Array<{
        // Instead of 'type' being a string, it's now a React component
        component?: ComponentType<T>;

        // Props specific to the component, can be of any type
        props?: any;

        // State mapping to connect form data to the component's state
        state?: Record<string, any>;

        isRequired?: boolean;
        requiredRules?: RequirementRule[];
    }>;
}

export type AutomationTemplate = {
    key: string;
    platform: string;
    type: string;
    name: string;
    description: string;
    logo: string;
    fields: Field[];
    baseRecipe: any;
    isDisabled: boolean;
};

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

export type AutomationCategoryType = { name: string; description?: string };
export type AutomationCategoryGroupType = { name: string; description?: string; tests: Test[] };
